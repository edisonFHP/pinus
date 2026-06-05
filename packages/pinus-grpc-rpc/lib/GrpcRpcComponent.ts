import * as path from 'path';
import { getLogger } from 'pinus-logger';
import { IAcceptorFactory, IMailBoxFactory } from 'pinus-rpc';
import { createGrpcAcceptor, GrpcAcceptorOpts } from './GrpcAcceptor';
import { createGrpcMailBox } from './GrpcChannelMailBox';

const logger = getLogger('pinus-grpc-rpc', path.basename(__filename));

export interface GrpcRpcComponentOpts {
    /**
     * gRPC server port for THIS process.
     * Defaults to getCurServer().grpcPort or getCurServer().port + 1000.
     */
    grpcPort?: number;
    /**
     * Override the acceptor factory (server side).
     * Defaults to createGrpcAcceptor.
     */
    acceptorFactory?: IAcceptorFactory;
    /**
     * Override the mailbox factory (client side).
     * Defaults to createGrpcMailBox.
     */
    mailboxFactory?: IMailBoxFactory;
}

/**
 * GrpcRpcComponent — Phase 1 integration shim.
 *
 * Load AFTER __remote__ and __proxy__:
 *
 *   app.load(pinus.components.remote);
 *   app.load(pinus.components.proxy);
 *   app.load(GrpcRpcComponent, { grpcPort: 4000 });
 *
 * start()     — starts an additional gRPC server that reuses the existing
 *               Dispatcher from __remote__.
 * afterStart() — patches MailStation.mailboxFactory in __proxy__ so all
 *               new outbound connections use gRPC.
 */
export class GrpcRpcComponent {
    name = '__grpcRpc__';

    private _grpcAcceptor: any = null;

    constructor(private app: any, private opts: GrpcRpcComponentOpts = {}) {}

    start(cb: () => void): void {
        const serverInfo = this.app.getCurServer() as any;

        // Resolve and store grpcPort on the local server info so that
        // GrpcAcceptor and remote peers (via servers.json) can use it.
        const grpcPort: number =
            this.opts.grpcPort
            ?? serverInfo.grpcPort
            ?? (Number(serverInfo.port) + 1000);

        serverInfo.grpcPort = grpcPort;
        process.nextTick(cb);
    }

    afterStart(cb: () => void): void {
        // Patch mailboxFactory as early as possible — mailboxes are created lazily
        // on first dispatch (triggered by ADD_SERVERS events after afterStartAll),
        // so this is safe even though ProxyComponent.afterStart() may not have run
        // yet. The _station instance exists from the ProxyComponent constructor.
        const proxyComp = this.app.components['__proxy__'];
        if (proxyComp) {
            const station = proxyComp.client && proxyComp.client._station;
            if (station) {
                station.mailboxFactory = this.opts.mailboxFactory ?? createGrpcMailBox;
                logger.info('[GrpcRpcComponent] gRPC mailboxFactory injected into MailStation');
            } else {
                logger.warn('[GrpcRpcComponent] __proxy__._station not found — gRPC client not configured');
            }
        } else {
            logger.warn('[GrpcRpcComponent] __proxy__ not found — gRPC client not configured');
        }
        process.nextTick(cb);
    }

    // afterStartAll fires after every component's start() and afterStart() have
    // completed — __remote__.remote.dispatcher is guaranteed to exist at this point.
    afterStartAll(): void {
        const serverInfo = this.app.getCurServer() as any;
        const grpcPort: number = serverInfo.grpcPort ?? (Number(serverInfo.port) + 1000);

        const remoteComp = this.app.components['__remote__'];
        if (!remoteComp) {
            // Frontend-only servers (e.g. connector without user remotes) have no
            // __remote__ component — skip gRPC server, client-side is enough.
            logger.info('[GrpcRpcComponent] no __remote__ on this server — gRPC server not started');
            return;
        }

        const gateway = remoteComp.remote;
        if (!gateway || !gateway.dispatcher) {
            logger.warn('[GrpcRpcComponent] __remote__ has no gateway/dispatcher — skipping gRPC server');
            return;
        }

        const acceptorFactory = this.opts.acceptorFactory ?? createGrpcAcceptor;
        const acceptorOpts: GrpcAcceptorOpts = {
            ...(remoteComp.opts ?? {}),
            grpcPort,
            context: this.app,
        };

        this._grpcAcceptor = acceptorFactory(
            acceptorOpts,
            (tracer: any, msg: any, dispatchCb: any) => {
                gateway.dispatcher.route(tracer, msg, dispatchCb);
            }
        );

        this._grpcAcceptor.on('error', (err: Error) => {
            logger.error('[GrpcRpcComponent] gRPC server error: %s', err.message);
        });

        // bindAsync is non-blocking; gRPC will be ready before the first RPC call
        // because those are triggered by ADD_SERVERS events from master which arrive
        // after afterStartAll completes.
        this._grpcAcceptor.listen(grpcPort);
        logger.info('[GrpcRpcComponent] gRPC server starting on port %d', grpcPort);
    }

    stop(force: boolean, cb: () => void): void {
        if (this._grpcAcceptor) {
            this._grpcAcceptor.close(cb);
            this._grpcAcceptor = null;
        } else {
            process.nextTick(cb);
        }
    }
}
