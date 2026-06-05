import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { EventEmitter } from 'events';
import { getLogger } from 'pinus-logger';
import { IAcceptor, AcceptorCallback, AcceptorOpts } from 'pinus-rpc';
import { Tracer } from 'pinus-rpc/lib/util/tracer';

const logger = getLogger('pinus-grpc-rpc', path.basename(__filename));

const PROTO_PATH = path.resolve(__dirname, './proto/pinus_rpc.proto');

const PKG_DEF_OPTS: protoLoader.Options = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};

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
        this._started = true;

        // Resolve gRPC port: explicit opt > servers.json grpcPort > fallback port+1000
        const grpcPort: number =
            this.opts.grpcPort
            ?? (this.opts.context && this.opts.context.getCurServer
                ? (this.opts.context.getCurServer().grpcPort as number | undefined)
                : undefined)
            ?? (Number(port) + 1000);

        const pkgDef = protoLoader.loadSync(PROTO_PATH, PKG_DEF_OPTS);
        const proto = grpc.loadPackageDefinition(pkgDef) as any;

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
                this.server.start();
                logger.info('[GrpcAcceptor] gRPC server listening on port %d', grpcPort);
            }
        );
    }

    close(): void {
        this.server.tryShutdown(() => {
            this.emit('closed');
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
