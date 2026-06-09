import { getLogger } from 'pinus-logger';
import * as utils from '../util/utils';
import * as Constants from '../util/constants';
import * as countDownLatch from '../util/countDownLatch';
import { EventEmitter } from 'events';
import * as util from 'util';
import { Application } from '../application';
import { ServerInfo } from '../util/constants';
import { ConsoleService, MasterAgent } from 'pinus-admin';
import * as path from 'path';
import * as fs from 'fs';
import * as http from 'http';
import { RaftNode, RaftRole, RaftPeer } from '../util/raftNode';
let logger = getLogger('pinus', path.basename(__filename));


export class Watchdog extends EventEmitter {

    isStarted = false;
    servers: {[serverId: string]: ServerInfo} = {};
    _listeners: {[serverId: string]: number} = {};
    count: number;
    private snapshotPath: string;
    private snapshotTimer: NodeJS.Timeout | null = null;
    private static readonly SNAPSHOT_INTERVAL_MS = 10000;
    private static readonly SNAPSHOT_MAX_AGE_MS = 60000;

    constructor(private app: Application, private service: ConsoleService) {
        super();

        this.count = Object.keys(app.getServersFromConfig()).length;
        this.snapshotPath = path.join(app.getBase(), '.pinus', 'master-snapshot.json');
        this.loadSnapshot();
        this.startSnapshotTimer();
    }

    private startSnapshotTimer() {
        this.snapshotTimer = setInterval(() => this.writeSnapshot(), Watchdog.SNAPSHOT_INTERVAL_MS);
        this.snapshotTimer.unref();
    }

    private writeSnapshot() {
        try {
            const dir = path.dirname(this.snapshotPath);
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
            }
            const snapshot = { servers: this.servers, timestamp: Date.now() };
            fs.writeFileSync(this.snapshotPath, JSON.stringify(snapshot, null, 2), 'utf8');
        } catch (err) {
            logger.warn('[HA] Failed to write master snapshot: %s', (err as Error).message);
        }
    }

    private loadSnapshot() {
        if (!fs.existsSync(this.snapshotPath)) return;
        try {
            const data = JSON.parse(fs.readFileSync(this.snapshotPath, 'utf8'));
            const age = Date.now() - data.timestamp;
            if (age > Watchdog.SNAPSHOT_MAX_AGE_MS) {
                logger.warn('[HA] Master snapshot is stale (%ds old), ignoring.', Math.floor(age / 1000));
                return;
            }
            Object.assign(this.servers, data.servers);
            logger.info('[HA] Restored %d servers from snapshot.', Object.keys(this.servers).length);
        } catch (err) {
            logger.warn('[HA] Failed to load master snapshot: %s', (err as Error).message);
        }
    }


    addServer(server: ServerInfo) {
        if (!server) {
            return;
        }
        this.servers[server.id] = server;
        this.notify({ action: 'addServer', server: server });
        this.writeSnapshot();
    }

    removeServer(id: string) {
        if (!id) {
            return;
        }
        this.unsubscribe(id);
        delete this.servers[id];
        this.notify({ action: 'removeServer', id: id });
        this.writeSnapshot();
    }

    reconnectServer(server: ServerInfo) {
        let self = this;
        if (!server) {
            return;
        }
        if (!this.servers[server.id]) {
            this.servers[server.id] = server;
        }
        // replace server in reconnect server
        this.notifyById(server.id, { action: 'replaceServer', servers: self.servers });
        // notify other server to add server
        this.notify({ action: 'addServer', server: server });
        // add server in listener
        this.subscribe(server.id);
    }

    subscribe(id: string) {
        this._listeners[id] = 1;
    }

    unsubscribe(id: string) {
        delete this._listeners[id];
    }

    query() {
        return this.servers;
    }

    record(id: string) {
        if (!this.isStarted && --this.count < 0) {
            let usedTime = Date.now() - this.app.startTime;
            this.notify({ action: 'startOver' });
            this.isStarted = true;
            logger.warn('all servers startup in %s ms', usedTime);
        }
    }

    notifyById(id: string, msg: any) {
        (this.service.agent as MasterAgent).request(id, Constants.KEYWORDS.MONITOR_WATCHER, msg, function (signal: any) {
            if (signal !== Constants.SIGNAL.OK) {
                logger.error('master watchdog fail to notify to monitor, id: %s, msg: %j', id, msg);
            } else {
                logger.debug('master watchdog notify to monitor success, id: %s, msg: %j', id, msg);
            }
        });
    }

    notify(msg: any) {
        let _listeners = this._listeners;
        let success = true;
        let fails: string[] = [];
        let timeouts: string[] = [];
        let requests: {[key: string]: number} = {};
        let count = Object.keys(_listeners).length;
        if (count === 0) {
            logger.warn('master watchdog _listeners is none, msg: %j', msg);
            return;
        }
        let latch = countDownLatch.createCountDownLatch(count, { timeout: Constants.TIME.TIME_WAIT_COUNTDOWN }, function (isTimeout) {
            if (!!isTimeout) {
                for (let key in requests) {
                    if (!requests[key]) {
                        timeouts.push(key);
                    }
                }
                logger.error('master watchdog request timeout message: %j, timeouts: %j, fails: %j', msg, timeouts, fails);
            }
            if (!success) {
                logger.error('master watchdog request fail message: %j, fails: %j', msg, fails);
            }
        });

        let moduleRequest = function (self: Watchdog, id: string) {
            return (function () {
                (self.service.agent as MasterAgent).request(id, Constants.KEYWORDS.MONITOR_WATCHER, msg, function (signal: any) {
                    if (signal !== Constants.SIGNAL.OK) {
                        fails.push(id);
                        success = false;
                    }
                    requests[id] = 1;
                    latch.done();
                });
            })();
        };

        for (let id in _listeners) {
            requests[id] = 0;
            moduleRequest(this, id);
        }
    }
}

export class RaftWatchdog extends Watchdog {
    constructor(app: Application, service: ConsoleService, private raftNode: RaftNode) {
        super(app, service);
    }

    addServer(server: ServerInfo) {
        if (this.raftNode.role !== RaftRole.LEADER) {
            this.forwardToLeader({ action: 'addServer', server });
            return;
        }
        super.addServer(server);
    }

    removeServer(id: string) {
        if (this.raftNode.role !== RaftRole.LEADER) {
            this.forwardToLeader({ action: 'removeServer', id });
            return;
        }
        super.removeServer(id);
    }

    private forwardToLeader(payload: object) {
        const leaderId = this.raftNode.leaderId;
        if (!leaderId) {
            logger.warn('[Raft] No leader elected, cannot forward command: %j', payload);
            return;
        }
        const peers: RaftPeer[] = (this.raftNode as any).peers || [];
        const leaderPeer = peers.find((p: RaftPeer) => p.id === leaderId);
        if (!leaderPeer) {
            logger.warn('[Raft] Leader peer %s not found in peer list', leaderId);
            return;
        }
        const adminPort = leaderPeer.port + 1;
        const body = JSON.stringify(payload);
        const req = http.request({
            hostname: leaderPeer.host,
            port: adminPort,
            path: '/internal/raft-forward',
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
        }, (res) => {
            if (res.statusCode !== 200) {
                logger.warn('[Raft] Leader forward returned status %d', res.statusCode);
            }
        });
        req.on('error', (err: Error) => {
            logger.warn('[Raft] Failed to forward to leader: %s', err.message);
        });
        req.write(body);
        req.end();
    }
}