import * as starter from './starter';
import { getLogger } from 'pinus-logger';
import * as path from 'path';
import * as http from 'http';

let logger = getLogger('pinus', path.basename(__filename));
let crashLogger = getLogger('crash-log', path.basename(__filename));
let adminLogger = getLogger('admin-log', path.basename(__filename));
import * as admin from 'pinus-admin';
import * as util from 'util';
import * as utils from '../util/utils';
import * as moduleUtil from '../util/moduleUtil';
import * as Constants from '../util/constants';
import { Application } from '../application';
import { ConsoleService, ConsoleServiceOpts } from 'pinus-admin';
import { IModule } from '../index';
import { MasterWatcherModule } from '../modules/masterwatcher';
import { Watchdog } from './watchdog';
import { RaftNode, RaftPeer } from '../util/raftNode';


interface RaftLeaderInfo {
    leaderId: string;
    leaderHost: string;
    leaderPort: number;
    term: number;
}

export class MasterHealthServer {
    private server: http.Server | null = null;

    constructor(
        private readonly getWatchdog: () => Watchdog | null,
        private readonly getLeaderInfo: () => RaftLeaderInfo | null = () => null
    ) {}

    start(port: number) {
        this.server = http.createServer((req, res) => {
            if (req.method === 'GET' && req.url === '/health') {
                const watchdog = this.getWatchdog();
                const servers = watchdog ? watchdog.query() : {};
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    status: 'ok',
                    uptime: process.uptime(),
                    serverCount: Object.keys(servers).length,
                    timestamp: Date.now()
                }));
            } else if (req.method === 'GET' && req.url === '/raft/leader') {
                const leaderInfo = this.getLeaderInfo();
                if (!leaderInfo) {
                    res.writeHead(503, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'no leader elected' }));
                } else {
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(leaderInfo));
                }
            } else {
                res.writeHead(404);
                res.end();
            }
        });
        this.server.listen(port, () => {
            logger.info('[HA] Master health endpoint listening on port %d', port);
        });
        this.server.on('error', (err) => {
            logger.warn('[HA] Master health server error: %s', err.message);
        });
    }

    stop() {
        if (this.server) {
            this.server.close();
            this.server = null;
        }
    }
}

export type MasterServerOptions =
    {
        port?: number;
        env?: string;
        closeWatcher?: boolean;
    } & Partial<ConsoleServiceOpts>;

export class MasterServer {
    app: Application;
    masterInfo: any;
    registered = {};
    modules: IModule[] = [];
    closeWatcher: boolean;
    masterConsole: ConsoleService;
    healthServer: MasterHealthServer | null = null;
    private raftNode: RaftNode | null = null;
    private raftLeaderInfo: { leaderId: string; leaderHost: string; leaderPort: number; term: number } | null = null;

    constructor(app: Application, opts?: MasterServerOptions) {
        this.app = app;
        this.masterInfo = app.getMaster();
        opts = opts || {};

        opts.port = this.masterInfo.port;
        opts.env = this.app.get(Constants.RESERVED.ENV);
        this.closeWatcher = opts.closeWatcher || false;
        this.masterConsole = admin.createMasterConsole(opts);
    }


    start(cb: (err?: Error) => void) {
        moduleUtil.registerDefaultModules(true, this.app, this.closeWatcher);
        moduleUtil.loadModules(this, this.masterConsole);

        let self = this;
        // start master console
        this.masterConsole.start(function (err) {
            if (err) {
                process.exit(0);
            }
            moduleUtil.startModules(self.modules, function (err: Error) {
                if (err) {
                    utils.invokeCallback(cb, err);
                    return;
                }

                const healthPort = self.masterInfo.port + 1;
                self.healthServer = new MasterHealthServer(
                    () => {
                        const watcherMod = self.modules.find(m => m instanceof MasterWatcherModule) as MasterWatcherModule | undefined;
                        return watcherMod ? watcherMod.watchdog : null;
                    },
                    () => self.raftLeaderInfo
                );
                self.healthServer.start(healthPort);

                const haCandidates: { id?: string; host: string; port: number }[] = self.app.get('masterHACandidates') || [];
                if (haCandidates.length > 0) {
                    const nodeId = self.masterInfo.id || `master-${self.masterInfo.host}-${self.masterInfo.port}`;
                    const peers: RaftPeer[] = haCandidates
                        .filter(c => !(c.host === self.masterInfo.host && c.port === self.masterInfo.port))
                        .map(c => ({
                            id: c.id || `master-${c.host}-${c.port}`,
                            host: c.host,
                            port: c.port,
                            raftPort: c.port + 2
                        }));
                    const allNodes = [...haCandidates.map(c => ({
                        id: c.id || `master-${c.host}-${c.port}`,
                        host: c.host,
                        port: c.port
                    })), { id: nodeId, host: self.masterInfo.host, port: self.masterInfo.port }];
                    self.raftNode = new RaftNode(nodeId, peers, self.masterInfo.port + 2);
                    self.raftNode.on('leaderElected', (leaderId: string) => {
                        const found = allNodes.find(n => n.id === leaderId);
                        self.raftLeaderInfo = {
                            leaderId,
                            leaderHost: found ? found.host : self.masterInfo.host,
                            leaderPort: found ? found.port : self.masterInfo.port,
                            term: self.raftNode!.currentTerm
                        };
                        logger.info('[Raft] Leader elected: %s (term %d)', leaderId, self.raftLeaderInfo.term);
                    });
                    self.raftNode.on('follower', () => {
                        logger.info('[Raft] Node %s became follower (term %d)', nodeId, self.raftNode!.currentTerm);
                    });
                    self.raftNode.start();
                }

                if (self.app.get(Constants.RESERVED.MODE) !== Constants.RESERVED.STAND_ALONE) {
                    starter.runServers(self.app);
                }
                utils.invokeCallback(cb);
            });
        });

        this.masterConsole.on('error', function (err) {
            if (!!err) {
                logger.error('masterConsole encounters with error: ' + err.stack);
                return;
            }
        });

        this.masterConsole.on('reconnect', function (info) {
            self.app.addServers([info]);
        });

        // monitor servers disconnect event
        this.masterConsole.on('disconnect', function (id, type, info, reason) {
            crashLogger.info(util.format('[%s],[%s],[%s],[%s]', type, id, Date.now(), reason || 'disconnect'));
            let count = 0;
            let time = 0;
            let pingTimer: NodeJS.Timeout = null;
            let server = self.app.getServerById(id);
            let stopFlags = self.app.get(Constants.RESERVED.STOP_SERVERS) || [];
            let autoRestart: any = server && server[Constants.RESERVED.AUTO_RESTART] || '';
            let restartForce: any = server && server[Constants.RESERVED.RESTART_FORCE] || '';
            if ((autoRestart.toString() === 'true' || restartForce.toString() === 'true') && stopFlags.indexOf(id) < 0) {
                let handle = function () {
                    clearTimeout(pingTimer);
                    utils.checkPort(self.app, server, function (status) {
                        if (status === 'error') {
                            utils.invokeCallback(cb, new Error('Check port command executed with error.'));
                            return;
                        } else if (status === 'busy') {
                            if (!!server[Constants.RESERVED.RESTART_FORCE]) {
                                starter.kill(self.app, [info.pid], [server]);
                            } else {
                                utils.invokeCallback(cb, new Error('Port occupied already, check your server to add.'));
                                return;
                            }
                        }
                        setTimeout(function () {
                            starter.run(self.app, server, null);
                        }, Constants.TIME.TIME_WAIT_STOP);
                    });
                };
                let setTimer = function (time: number) {
                    pingTimer = setTimeout(function () {
                        utils.ping(server.host, function (flag) {
                            if (flag) {
                                handle();
                            } else {
                                count++;
                                if (count > 3) {
                                    time = Constants.TIME.TIME_WAIT_MAX_PING;
                                } else {
                                    time = Constants.TIME.TIME_WAIT_PING * count;
                                }
                                setTimer(time);
                            }
                        });
                    }, time);
                };
                setTimer(time);
            }
        });

        // monitor servers register event
        this.masterConsole.on('register', function (record) {
            starter.bindCpu(self.app, record.id, record.pid, record.host);
        });

        this.masterConsole.on('admin-log', function (log, error) {
            if (error) {
                adminLogger.error(JSON.stringify(log));
            } else {
                adminLogger.info(JSON.stringify(log));
            }
        });
    }

    stop(cb: () => void) {
        if (this.raftNode) {
            this.raftNode.stop();
            this.raftNode = null;
        }
        if (this.healthServer) {
            this.healthServer.stop();
            this.healthServer = null;
        }
        this.masterConsole.stop();
        process.nextTick(cb);
    }
}
