import { getLogger } from 'pinus-logger';
import { EventEmitter } from 'events';
import { connect, NatsConnection, JSONCodec, ErrorCode } from 'nats';
import { Tracer } from '../../util/tracer';
import { IMailBox, MailBoxOpts, MailBoxMessage, MailBoxTimeoutCallback } from '../mailbox';

const logger = getLogger('pinus-rpc', 'nats-mailbox');

const DEFAULT_TIMEOUT = 10 * 1000;
const SUBJECT_PREFIX = 'pinus.rpc';

export interface NatsMailBoxOpts extends MailBoxOpts {
    natsUrl?: string;
}

export class NatsMailBox extends EventEmitter implements IMailBox {
    private nc: NatsConnection = null;
    private readonly jc = JSONCodec();
    private readonly serverId: string;
    private readonly subject: string;
    private readonly natsUrl: string;
    private readonly timeoutValue: number;
    connected = false;
    closed = false;

    constructor(serverInfo: { id: string; host: string; port: number }, opts: NatsMailBoxOpts) {
        super();
        this.serverId = serverInfo.id;
        this.subject = `${SUBJECT_PREFIX}.${serverInfo.id}`;
        this.natsUrl = opts.natsUrl || 'nats://localhost:4222';
        this.timeoutValue = opts.timeout || DEFAULT_TIMEOUT;
    }

    connect(tracer: Tracer, cb: (err?: Error) => void) {
        tracer && tracer.info('client', __filename, 'connect', 'nats-mailbox try to connect');
        if (this.connected) {
            cb(new Error('nats-mailbox already connected'));
            return;
        }

        connect({ servers: this.natsUrl }).then(nc => {
            this.nc = nc;
            this.connected = true;
            this.closed = false;
            // 当 NATS 连接断开时触发 close 事件，让 mailstation 感知
            nc.closed().then(() => {
                if (!this.closed) {
                    logger.warn('nats connection closed unexpectedly, serverId: %s', this.serverId);
                    this.emit('close', this.serverId);
                }
            });
            cb();
        }).catch(err => {
            logger.error('nats-mailbox connect to %s failed: %s', this.natsUrl, err.message);
            cb(err);
        });
    }

    /**
     * 发送 RPC 请求，利用 NATS 内置 Request-Reply 模式，
     * 无需手动管理关联 ID 和超时 timer。
     */
    send(tracer: Tracer, msg: MailBoxMessage, opts: any, cb: MailBoxTimeoutCallback) {
        tracer && tracer.info('client', __filename, 'send', 'nats-mailbox try to send');
        if (!this.connected || this.closed) {
            cb(tracer, new Error('nats-mailbox is not connected'));
            return;
        }

        const pkg = tracer && tracer.isEnabled
            ? { traceId: tracer.id, seqId: tracer.seq, source: tracer.source, remote: tracer.remote, msg }
            : { msg };

        this.nc.request(this.subject, this.jc.encode(pkg), { timeout: this.timeoutValue })
            .then(reply => {
                const resp = this.jc.decode(reply.data) as { resp: any };
                cb(tracer, null, resp.resp);
            })
            .catch(err => {
                if (err.code === ErrorCode.Timeout) {
                    logger.error('nats rpc timeout to server %s, subject: %s', this.serverId, this.subject);
                } else {
                    logger.error('nats rpc error to server %s: %s', this.serverId, err.message);
                }
                cb(tracer, err);
            });
    }

    close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.connected = false;
        if (this.nc) {
            const nc = this.nc;
            this.nc = null;
            nc.drain().catch(() => nc.close());
        }
    }
}

/**
 * Factory method to create NATS mailbox.
 *
 * @param serverInfo  remote server info {id, host, port}
 * @param opts        opts.natsUrl   NATS server URL, default 'nats://localhost:4222'
 *                    opts.timeout   RPC timeout ms, default 10000
 */
export function create(serverInfo: { id: string; host: string; port: number }, opts: NatsMailBoxOpts): IMailBox {
    return new NatsMailBox(serverInfo, opts || {} as NatsMailBoxOpts);
}
