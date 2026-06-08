import * as grpc from '@grpc/grpc-js';
import * as path from 'path';
import { EventEmitter } from 'events';
import { getLogger } from 'pinus-logger';
import { IAcceptor, AcceptorCallback, AcceptorOpts } from 'pinus-rpc';
import { Tracer } from 'pinus-rpc/lib/util/tracer';
import { getPkgDef } from './proto-loader';

const logger = getLogger('pinus-grpc-rpc', path.basename(__filename));

const SHUTDOWN_GRACE_MS = 5000;

export interface GrpcAcceptorOpts extends AcceptorOpts {
    grpcPort?: number;
    context?: any; // pinus Application
}

export class GrpcAcceptor extends EventEmitter implements IAcceptor {
    private server: grpc.Server;
    private _started = false;

    constructor(private opts: GrpcAcceptorOpts, private cb: AcceptorCallback) {
        super();
        this.server = new grpc.Server({
            'grpc.max_receive_message_length': 8 * 1024 * 1024,
            'grpc.max_send_message_length': 8 * 1024 * 1024,
        });
    }

    listen(port: number | string): void {
        if (this._started) { return; }

        // Resolve gRPC port: explicit opt > servers.json grpcPort > fallback port+1000
        const grpcPort: number =
            this.opts.grpcPort
            ?? (this.opts.context && this.opts.context.getCurServer
                ? (this.opts.context.getCurServer().grpcPort as number | undefined)
                : undefined)
            ?? (Number(port) + 1000);

        const proto = grpc.loadPackageDefinition(getPkgDef()) as any;

        this.server.addService(proto.pinus.PinusRpc.service, {
            Invoke: (
                call: grpc.ServerUnaryCall<any, any>,
                grpcCb: grpc.sendUnaryData<any>
            ) => this._handleCall(call, grpcCb),

            Notify: (
                call: grpc.ServerUnaryCall<any, any>,
                grpcCb: grpc.sendUnaryData<any>
            ) => {
                this._handleCall(call, null);
                grpcCb(null, {});
            },
        });

        this.server.bindAsync(
            `0.0.0.0:${grpcPort}`,
            grpc.ServerCredentials.createInsecure(),
            (err: Error | null) => {
                if (err) {
                    logger.error('[GrpcAcceptor] bind failed on port %d: %s', grpcPort, err.message);
                    this.emit('error', err, this);
                    return;
                }
                this._started = true;
                // bindAsync starts the server since @grpc/grpc-js 1.10.x
                logger.info('[GrpcAcceptor] gRPC server listening on port %d', grpcPort);
            }
        );
    }

    close(cb?: () => void): void {
        const done = () => {
            this.emit('closed');
            cb?.();
        };

        // Fallback: force-shutdown if graceful drain takes too long.
        const forceTimer = setTimeout(() => {
            logger.warn('[GrpcAcceptor] tryShutdown timed out — forcing shutdown');
            this.server.forceShutdown();
            done();
        }, SHUTDOWN_GRACE_MS);

        this.server.tryShutdown(() => {
            clearTimeout(forceTimer);
            done();
        });
    }

    private _handleCall(
        call: grpc.ServerUnaryCall<any, any>,
        grpcCb: grpc.sendUnaryData<any> | null
    ): void {
        const req = call.request;

        let args: any[];
        try {
            args = JSON.parse((req.args_json as Buffer).toString());
        } catch (e) {
            logger.error('[GrpcAcceptor] failed to parse args_json: %s', (e as Error).message);
            if (grpcCb) {
                grpcCb({ code: grpc.status.INVALID_ARGUMENT, message: 'invalid args_json' });
            }
            return;
        }

        const msgPkg = {
            namespace: (req.namespace as string) || 'user',
            service:   req.service   as string,
            method:    req.method    as string,
            args,
        };

        this.cb(null as any, msgPkg, (err: Error | null, result?: any) => {
            if (!grpcCb) { return; }
            if (err) {
                logger.error('[GrpcAcceptor] dispatch error: %s', err.message);
                grpcCb({ code: grpc.status.INTERNAL, message: err.message });
                return;
            }
            grpcCb(null, {
                result_json: Buffer.from(JSON.stringify(result ?? null)),
                error: '',
            });
        });
    }
}

export function createGrpcAcceptor(opts: GrpcAcceptorOpts, cb: AcceptorCallback): IAcceptor {
    return new GrpcAcceptor(opts, cb);
}
