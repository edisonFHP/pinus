import * as http from 'http';
import { getLogger } from 'pinus-logger';
import { Watchdog } from '../master/watchdog';
import * as path from 'path';

let logger = getLogger('pinus', path.basename(__filename));

export interface HAMasterNode {
    id: string;
    host: string;
    port: number;
    adminPort?: number;
}

const SYNC_INTERVAL_MS = 5000;

export class WatchdogSyncer {
    private timer: NodeJS.Timeout | null = null;

    start(watchdog: Watchdog, backupMasters: HAMasterNode[]) {
        if (!backupMasters || backupMasters.length === 0) return;

        this.timer = setInterval(() => {
            const snapshot = {
                action: 'syncState',
                servers: watchdog.query(),
                timestamp: Date.now()
            };
            for (const backup of backupMasters) {
                this.pushToBackup(backup, snapshot);
            }
        }, SYNC_INTERVAL_MS);
    }

    stop() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    }

    private pushToBackup(backup: HAMasterNode, snapshot: object) {
        const adminPort = backup.adminPort != null ? backup.adminPort : backup.port + 1;
        const body = JSON.stringify(snapshot);
        const options: http.RequestOptions = {
            host: backup.host,
            port: adminPort,
            path: '/internal/sync',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(body)
            }
        };
        const req = http.request(options, (res) => {
            if (res.statusCode !== 200) {
                logger.warn('[HA] Sync to backup master %s:%d returned status %d',
                    backup.host, adminPort, res.statusCode);
            }
        });
        req.on('error', (err) => {
            logger.warn('[HA] Failed to sync state to backup master %s:%d: %s',
                backup.host, adminPort, err.message);
        });
        req.write(body);
        req.end();
    }
}
