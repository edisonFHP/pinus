import { getLogger } from 'pinus-logger';
import { EventEmitter } from 'events';
import { connect, NatsConnection, JSONCodec, Subscription } from 'nats';
import { Tracer } from '../../util/tracer';
import { AcceptorOpts, IAcceptor, AcceptorCallback } from '../acceptor';
import { MsgPkg } from '../dispatcher';

const logger = getLogger('pinus-rpc', 'nats-acceptor');

const SUBJECT_PREFIX = 'pinus.rpc';

export interface NatsAcceptorOpts extends AcceptorOpts {
    natsUrl?: string;
    /** 显式指定本服务器 ID；未设置时从 opts.context.getCurrentServer().id 取 */
    serverId?: string;
    /** pinus Application，由 RemoteComponent 自动注入 opts.context */
    context?: { getCurrentServer(): { id: string } };
}

export class NatsAcceptor extends EventEmitter implements IAcceptor {
    private nc: NatsConnection = null;
    private sub: Subscription = null;
    private readonly jc = JSONCodec();
    private readonly natsUrl: string;
    private readonly serverId: string;
    private readonly cb: AcceptorCallback;
    private readonly rpcLogger: any;
    private readonly rpcDebugLog: boolean;
    closed = false;
    inited = false;

    constructor(opts: NatsAcceptorOpts, cb: AcceptorCallback) {
        super();
        this.natsUrl = opts.natsUrl || 'nats://localhost:4222';
        this.rpcLogger = opts.rpcLogger;
        this.rpcDebugLog = opts.rpcDebugLog;
        this.cb = cb;

        if (opts.serverId) {
            this.serverId = opts.serverId;
        } else if (opts.context && typeof opts.context.getCurrentServer === 'function') {
            this.serverId = opts.context.getCurrentServer().id;
        }
    }

    /**
     * 连接 NATS 并订阅本服务器的 RPC subject。
     * port 参数对 NATS 传输无意义，接口兼容保留。
     */
    listen(port: number) {
        if (this.inited) {
            throw new Error('nats-acceptor already inited');
        }
        if (!this.serverId) {
            this.emit('error', new Error('nats-acceptor: serverId is required, set opts.serverId or opts.context'));
            return;
        }
        this.inited = true;

        const subject = `${SUBJECT_PREFIX}.${this.serverId}`;
        logger.info('nats-acceptor subscribing to subject: %s', subject);

        connect({ servers: this.natsUrl }).then(nc => {
            this.nc = nc;
            this.sub = nc.subscribe(subject);
            // 异步消息处理循环，subscription 关闭时自动退出
            this.processMessages(this.sub).catch(err => {
                if (!this.closed) {
                    logger.error('nats-acceptor subscription error: %s', err.message);
                    this.emit('error', err, this);
                }
            });
            // 监听 NATS 连接关闭
            nc.closed().then(() => {
                if (!this.closed) {
                    this.emit('closed');
                }
            });
        }).catch(err => {
            logger.error('nats-acceptor connect to %s failed: %s', this.natsUrl, err.message);
            this.emit('error', err, this);
        });
    }

    private async processMessages(sub: Subscription) {
        for await (const msg of sub) {
            try {
                const pkg = this.jc.decode(msg.data) as { traceId?: string; seqId?: number; source?: string; remote?: string; msg: MsgPkg };

                let tracer: Tracer = null;
                if (this.rpcDebugLog) {
                    tracer = new Tracer(this.rpcLogger, this.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
                    tracer.info('server', __filename, 'processMessages', 'nats-acceptor received rpc message');
                }

                // 捕获 msg 引用用于在回调中 respond
                const natsMsg = msg;
                this.cb(tracer, pkg.msg, (...args: any[]) => {
                    const errorArg = args[0];
                    if (errorArg && errorArg instanceof Error) {
                        args[0] = { msg: errorArg.message, stack: errorArg.stack };
                    }
                    try {
                        natsMsg.respond(this.jc.encode({ resp: args }));
                    } catch (e) {
                        logger.error('nats-acceptor respond error: %s', e.message);
                    }
                });
            } catch (err) {
                logger.error('nats-acceptor decode message error: %s', err.message);
            }
        }
    }

    close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (this.sub) {
            this.sub.drain();
        }
        if (this.nc) {
            const nc = this.nc;
            this.nc = null;
            nc.drain()
                .catch(() => nc.close())
                .finally(() => this.emit('closed'));
        } else {
            this.emit('closed');
        }
    }
}

/**
 * Factory method to create NATS acceptor.
 *
 * @param opts  opts.natsUrl   NATS server URL, default 'nats://localhost:4222'
 *              opts.serverId  explicit server ID (optional, auto-detected from opts.context)
 *              opts.context   pinus Application (injected by RemoteComponent)
 * @param cb    dispatcher callback
 */
export function create(opts: NatsAcceptorOpts, cb: AcceptorCallback): IAcceptor {
    return new NatsAcceptor(opts || {} as NatsAcceptorOpts, cb);
}
