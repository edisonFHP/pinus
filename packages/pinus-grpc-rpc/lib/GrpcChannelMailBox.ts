import * as grpc from '@grpc/grpc-js';
import * as path from 'path';
import { EventEmitter } from 'events';
import { getLogger } from 'pinus-logger';
import { IMailBox, MailBoxMessage, MailBoxOpts, MailBoxTimeoutCallback } from 'pinus-rpc';
import { Tracer } from 'pinus-rpc/lib/util/tracer';
import { getPkgDef } from './proto-loader';

const logger = getLogger('pinus-grpc-rpc', path.basename(__filename));

export interface GrpcServerInfo {
    id: string;
    host: string;
    port: number;
    grpcPort?: number; // optional override; defaults to port+1000
}

export class GrpcChannelMailBox extends EventEmitter implements IMailBox {
    private stub: any;
    private _closed = false;

    constructor(
        private readonly serverInfo: GrpcServerInfo,
        private readonly opts: MailBoxOpts
    ) {
        super();
    }

    connect(tracer: Tracer, cb: (err?: Error) => void): void {
        const grpcPort = this.serverInfo.grpcPort ?? (this.serverInfo.port + 1000);
        const addr = `${this.serverInfo.host}:${grpcPort}`;

        const proto = grpc.loadPackageDefinition(getPkgDef()) as any;
        this.stub = new proto.pinus.PinusRpc(
            addr,
            grpc.credentials.createInsecure(),
            {
                'grpc.max_receive_message_length': 8 * 1024 * 1024,
                'grpc.max_send_message_length': 8 * 1024 * 1024,
            }
        );

        const deadlineMs = Date.now() + 5000;
        this.stub.waitForReady(deadlineMs, (err?: Error) => {
            if (err) {
                // Close the channel so it stops retrying in the background.
                this.stub.close();
                this.stub = null;
                logger.error('[GrpcMailBox] connect failed to %s: %s', addr, err.message);
                cb(err);
                return;
            }
            logger.info('[GrpcMailBox] connected to %s (id=%s)', addr, this.serverInfo.id);
            this._watchConnectivity();
            cb();
        });
    }

    send(tracer: Tracer, msg: MailBoxMessage, opts: any, cb: MailBoxTimeoutCallback): void {
        if (this._closed || !this.stub) {
            if (cb) { cb(tracer, new Error('[GrpcMailBox] mailbox is closed')); }
            return;
        }

        const extMsg = msg as any;
        const req = {
            namespace:   extMsg.namespace  ?? 'user',
            server_type: extMsg.serverType ?? '',
            service:     msg.service,
            method:      msg.method,
            args_json:   Buffer.from(JSON.stringify(msg.args ?? [])),
        };

        const metadata = new grpc.Metadata();

        if (!cb) {
            // Notify mode — fire and forget
            this.stub.Notify(req, metadata, () => { /* ignore */ });
            return;
        }

        const timeoutMs = (this.opts as any)?.timeout ?? 30000;
        const deadline = new Date(Date.now() + timeoutMs);

        this.stub.Invoke(
            req,
            metadata,
            { deadline },
            (err: grpc.ServiceError, resp: any) => {
                if (err) {
                    cb(tracer, err);
                    return;
                }
                if (resp.error) {
                    cb(tracer, new Error(resp.error));
                    return;
                }
                let result: any = null;
                try {
                    const buf: Buffer = resp.result_json;
                    if (buf && buf.length > 0) {
                        result = JSON.parse(buf.toString());
                    }
                } catch (parseErr) {
                    cb(tracer, new Error('[GrpcMailBox] failed to parse result_json'));
                    return;
                }
                cb(tracer, null as any, result);
            }
        );
    }

    close(): void {
        this._closed = true;
        if (this.stub) {
            this.stub.close();
            this.stub = null;
        }
    }

    // Overload to satisfy IMailBox — only 'close' event is defined by the interface.
    on(event: 'close', listener: (serverId: string) => void): this;
    on(event: string, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    private _watchConnectivity(): void {
        if (!this.stub) { return; }
        const channel: grpc.Channel = this.stub.getChannel();
        const serverId = this.serverInfo.id;

        const watch = (lastState: grpc.connectivityState): void => {
            if (this._closed) { return; }
            channel.watchConnectivityState(lastState, Infinity, () => {
                if (this._closed) { return; }
                const newState = channel.getConnectivityState(false);
                if (
                    newState === grpc.connectivityState.TRANSIENT_FAILURE ||
                    newState === grpc.connectivityState.SHUTDOWN
                ) {
                    logger.warn('[GrpcMailBox] channel to %s lost connectivity', serverId);
                    this.emit('close', serverId);
                } else {
                    watch(newState);
                }
            });
        };

        watch(channel.getConnectivityState(false));
    }
}

// IMailBoxFactory signature — drop-in replacement for createMqttMailBox / createTcpMailBox.
export function createGrpcMailBox(
    serverInfo: { id: string; host: string; port: number },
    opts: MailBoxOpts
): IMailBox {
    return new GrpcChannelMailBox(serverInfo as GrpcServerInfo, opts);
}
