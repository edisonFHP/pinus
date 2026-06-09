import * as http from 'http';
import { EventEmitter } from 'events';
import { getLogger } from 'pinus-logger';
import { MasterInfo } from '../index';
import * as path from 'path';

let logger = getLogger('pinus', path.basename(__filename));

const CHECK_INTERVAL_MS = 5000;
const CHECK_TIMEOUT_MS = 2000;
const MAX_MISS_COUNT = 3;

export class ActiveHealthMonitor extends EventEmitter {
    private timer: NodeJS.Timeout | null = null;
    private missCount: number = 0;

    constructor(private masterInfo: MasterInfo) {
        super();
    }

    start() {
        this.missCount = 0;
        this.timer = setInterval(() => this.check(), CHECK_INTERVAL_MS);
    }

    stop() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    }

    private check() {
        const healthPort = this.masterInfo.port + 1;
        let settled = false;

        const settle = (success: boolean) => {
            if (settled) return;
            settled = true;
            clearTimeout(timeoutId);
            if (success) {
                this.missCount = 0;
            } else {
                this.onMiss();
            }
        };

        const timeoutId = setTimeout(() => settle(false), CHECK_TIMEOUT_MS);

        const req = http.request({
            host: this.masterInfo.host,
            port: healthPort,
            path: '/health',
            method: 'GET',
        }, (res) => {
            res.resume();
            settle(res.statusCode === 200);
        });

        req.on('error', () => settle(false));
        req.end();
    }

    private onMiss() {
        this.missCount++;
        logger.warn('[HA] Master health check failed (%d/%d): %j',
            this.missCount, MAX_MISS_COUNT, this.masterInfo);
        if (this.missCount >= MAX_MISS_COUNT) {
            this.stop();
            logger.error('[HA] Master %j declared dead, initiating failover.', this.masterInfo);
            this.emit('masterDead', this.masterInfo);
        }
    }
}
