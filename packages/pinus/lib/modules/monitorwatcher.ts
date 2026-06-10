import { getLogger } from 'pinus-logger';
import * as http from 'http';
import * as utils from '../util/utils';
import { default as events } from '../util/events';
import * as Constants from '../util/constants';
import * as util from 'util';
import { Application } from '../application';
import { IModule, ConsoleService, MonitorAgent, MonitorCallback } from 'pinus-admin';
import { ServerInfo } from '../util/constants';
import { MasterInfo } from '../index';
import { ActiveHealthMonitor } from '../util/activeHealthMonitor';
import * as path from 'path';
let logger = getLogger('pinus', path.basename(__filename));

const MAX_DISCONNECT_BEFORE_FAILOVER = 3;

export class MonitorWatcherModule implements IModule {
    app: Application;
    service: any;
    id: string;

    private haCandidates: MasterInfo[] = [];
    private currentMasterIndex: number = -1;
    private switchLock: boolean = false;
    private disconnectCount: number = 0;
    private activeHealthMonitor: ActiveHealthMonitor | null = null;

    static moduleId = Constants.KEYWORDS.MONITOR_WATCHER;

    constructor(opts: {app: Application}, consoleService: ConsoleService) {
        this.app = opts.app;
        this.service = consoleService;
        this.id = this.app.getServerId();

        this.app.event.on(events.START_SERVER, finishStart.bind(null, this));

        this.haCandidates = this.app.get('masterHACandidates') || [];
        if (this.haCandidates.length > 0) {
            this.service.on('disconnect', () => this.onMasterDisconnect());
            this.service.on('reconnect', () => {
                this.disconnectCount = 0;
                // switchLock is managed by failoverToNext, not reset here.
                // The old MQTT client fires reconnect events to the dead master
                // which would otherwise incorrectly reset the lock mid-failover.
            });
            this.startActiveMonitor(this.app.getMaster());
        }
    }

    private startActiveMonitor(masterInfo: MasterInfo) {
        if (this.activeHealthMonitor) {
            this.activeHealthMonitor.stop();
            this.activeHealthMonitor = null;
        }
        const monitor = new ActiveHealthMonitor(masterInfo);
        this.activeHealthMonitor = monitor;
        monitor.on('masterDead', (dead: MasterInfo) => {
            if (this.switchLock) return;
            // Stale guard: ignore events from a monitor that was already replaced.
            if (this.activeHealthMonitor !== monitor) return;
            this.switchLock = true;
            logger.warn('[HA] ActiveHealthMonitor declared master dead: %j', dead);
            this.failoverToNext();
        });
        monitor.start();
    }

    private onMasterDisconnect() {
        if (this.switchLock) return;
        this.disconnectCount++;
        logger.warn('[HA] Master disconnect detected (%d/%d)', this.disconnectCount, MAX_DISCONNECT_BEFORE_FAILOVER);
        if (this.disconnectCount >= MAX_DISCONNECT_BEFORE_FAILOVER) {
            this.switchLock = true;
            this.failoverToNext();
        }
    }

    private async connectToLeader(): Promise<MasterInfo | null> {
        for (const candidate of this.haCandidates) {
            try {
                const leaderInfo = await this.fetchRaftLeader(candidate);
                if (leaderInfo) return leaderInfo;
            } catch {
                // candidate unreachable, try next
            }
        }
        return null;
    }

    private fetchRaftLeader(candidate: MasterInfo): Promise<MasterInfo | null> {
        return new Promise((resolve) => {
            const healthPort = candidate.port + 1;
            const req = http.get(
                { host: candidate.host, port: healthPort, path: '/raft/leader', timeout: 2000 },
                (res) => {
                    if (res.statusCode !== 200) { resolve(null); return; }
                    let body = '';
                    res.on('data', (chunk: string) => { body += chunk; });
                    res.on('end', () => {
                        try {
                            const data = JSON.parse(body);
                            if (data.leaderHost && data.leaderPort) {
                                resolve({ id: data.leaderId || '', host: data.leaderHost, port: data.leaderPort });
                            } else {
                                resolve(null);
                            }
                        } catch { resolve(null); }
                    });
                }
            );
            req.on('error', () => resolve(null));
            req.on('timeout', () => { req.destroy(); resolve(null); });
        });
    }

    private async failoverToNext() {
        // Stop the current monitor immediately before any async work.
        // This prevents in-flight health-check requests from re-triggering
        // masterDead and causing a concurrent second failover attempt.
        if (this.activeHealthMonitor) {
            this.activeHealthMonitor.stop();
            this.activeHealthMonitor = null;
        }

        if (this.haCandidates.length > 0) {
            const leader = await this.connectToLeader();
            if (leader) {
                const currentMaster = this.app.getMaster();
                this.disconnectCount = 0;
                logger.warn('[HA] Raft failover: %j -> leader %j', currentMaster, leader);
                // Update app.master before reconnect so the new MonitorWatcherModule
                // instance created by loadModules() picks up the correct master.
                this.app.master = leader as any;
                const monitorComponent = this.app.components.__monitor__;
                if (monitorComponent) {
                    monitorComponent.reconnect(leader);
                }
                this.switchLock = false;
                return;
            }
        }
        const currentMaster = this.app.getMaster();
        let nextIndex = -1;
        for (let i = 0; i < this.haCandidates.length; i++) {
            const c = this.haCandidates[i];
            if (c.host === currentMaster.host && c.port === currentMaster.port) continue;
            if (i > this.currentMasterIndex) {
                nextIndex = i;
                break;
            }
        }
        if (nextIndex === -1) {
            // wrap around: pick first candidate that differs from current master
            for (let i = 0; i < this.haCandidates.length; i++) {
                const c = this.haCandidates[i];
                if (!(c.host === currentMaster.host && c.port === currentMaster.port)) {
                    nextIndex = i;
                    break;
                }
            }
        }
        if (nextIndex === -1) {
            logger.error('[HA] All master candidates exhausted, cannot failover.');
            this.switchLock = false;
            return;
        }
        const nextMaster = this.haCandidates[nextIndex];
        this.currentMasterIndex = nextIndex;
        this.disconnectCount = 0;
        logger.warn('[HA] Master failover: %j -> %j', currentMaster, nextMaster);
        // Update app.master before reconnect so the new MonitorWatcherModule
        // instance created by loadModules() picks up the correct master.
        this.app.master = nextMaster as any;
        const monitorComponent = this.app.components.__monitor__;
        if (monitorComponent) {
            monitorComponent.reconnect(nextMaster);
        }
        this.switchLock = false;
    }

    start(cb: () => void) {
        subscribeRequest(this, this.service.agent, this.id, cb);
    }

    monitorHandler(agent: MonitorAgent, msg: any, cb: MonitorCallback) {
        if (!msg || !msg.action) {
            return;
        }
        let func = (monitorMethods as any)[msg.action];
        if (!func) {
            logger.info('monitorwatcher unknown action: %j', msg.action);
            return;
        }
        func(this, agent, msg, cb);
    }
}

// ----------------- monitor start method -------------------------

let subscribeRequest = function (self: MonitorWatcherModule, agent: MonitorAgent, id: string, cb: MonitorCallback) {
    let msg = { action: 'subscribe', id: id };
    agent.request(Constants.KEYWORDS.MASTER_WATCHER, msg, function (err: Error, servers) {
        if (err) {
            logger.error('subscribeRequest request to master with error: %j', err.stack);
            utils.invokeCallback(cb, err);
        }
        let res = [];
        for (let id in servers) {
            res.push(servers[id]);
        }
        addServers(self, res);
        utils.invokeCallback(cb);
    });
};

// ----------------- monitor request methods -------------------------

let addServer = function (self: MonitorWatcherModule, agent: MonitorAgent, msg: any, cb: MonitorCallback) {
    logger.debug('[%s] receive addServer signal: %j', self.app.serverId, msg);
    if (!msg || !msg.server) {
        logger.warn('monitorwatcher addServer receive empty message: %j', msg);
        utils.invokeCallback(cb, Constants.SIGNAL.FAIL);
        return;
    }
    addServers(self, [msg.server]);
    utils.invokeCallback(cb, Constants.SIGNAL.OK);
};

let removeServer = function  (self: MonitorWatcherModule, agent: MonitorAgent, msg: any, cb: MonitorCallback) {
    logger.debug('%s receive removeServer signal: %j', self.app.serverId, msg);
    if (!msg || !msg.id) {
        logger.warn('monitorwatcher removeServer receive empty message: %j', msg);
        utils.invokeCallback(cb, Constants.SIGNAL.FAIL);
        return;
    }
    removeServers(self, [msg.id]);
    utils.invokeCallback(cb, Constants.SIGNAL.OK);
};

let replaceServer = function (self: MonitorWatcherModule, agent: MonitorAgent, msg: any, cb: MonitorCallback) {
    logger.debug('%s receive replaceServer signal: %j', self.app.serverId, msg);
    if (!msg || !msg.servers) {
        logger.warn('monitorwatcher replaceServer receive empty message: %j', msg);
        utils.invokeCallback(cb, Constants.SIGNAL.FAIL);
        return;
    }
    replaceServers(self, msg.servers);
    utils.invokeCallback(cb, Constants.SIGNAL.OK);
};

let startOver = function (self: MonitorWatcherModule, agent: MonitorAgent, msg: any, cb: MonitorCallback) {

    for(let component of self.app.loaded) {
        let fun = component[Constants.RESERVED.AFTER_STARTALL];
        if (!!fun) {
            fun.call(component);
        }
    }

    for(let lifecycle of self.app.usedPlugins) {
        let fun = lifecycle[Constants.LIFECYCLE.AFTER_STARTALL];
        if (!!fun) {
            fun.call(lifecycle, self.app);
        }
    }

    self.app.event.emit(events.START_ALL);
    utils.invokeCallback(cb, Constants.SIGNAL.OK);
};

// ----------------- common methods -------------------------

let addServers = function (self: MonitorWatcherModule, servers: ServerInfo[]) {
    if (!servers || !servers.length) {
        return;
    }
    self.app.addServers(servers);
};

let removeServers = function (self: MonitorWatcherModule, ids: string[]) {
    if (!ids || !ids.length) {
        return;
    }
    self.app.removeServers(ids);
};

let replaceServers = function (self: MonitorWatcherModule, servers:  {[serverId: string]: ServerInfo}) {
    self.app.replaceServers(servers);
};

// ----------------- bind methods -------------------------

let finishStart = function (self: MonitorWatcherModule, id: string) {
    let msg = { action: 'record', id: id };
    self.service.agent.notify(Constants.KEYWORDS.MASTER_WATCHER, msg);
};

let monitorMethods = {
    'addServer': addServer,
    'removeServer': removeServer,
    'replaceServer': replaceServer,
    'startOver': startOver
};
