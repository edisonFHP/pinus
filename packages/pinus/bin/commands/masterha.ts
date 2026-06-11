


import * as fs from 'fs';
import * as path from 'path';
import * as constants from '../../lib/util/constants';
import { abort, runServer } from '../utils/utils';
import { DEFAULT_GAME_SERVER_DIR, MASTER_HA_NOT_FOUND } from '../utils/constants';
import { Command } from 'commander';

export default function (program: Command) {
    program.command('masterha')
    .description('start all the slaves of the master')
    .option('-d, --directory <directory>', 'the code directory', DEFAULT_GAME_SERVER_DIR)
    .action(function (opts) {
        startMasterha(opts);
    });
}

/**
 * Start master slaves.
 *
 * @param {String} option for `startMasterha` operation
 */
function startMasterha(opts: any) {
    let configFile = path.join(opts.directory, constants.FILEPATH.MASTER_HA);
    if (!fs.existsSync(configFile)) {
        abort(MASTER_HA_NOT_FOUND);
    }
    let masterha = require(configFile).masterha;
    if (!masterha || !masterha.length) {
        abort('masterha.json must contain a non-empty "masterha" array');
    }

    // Read master.json to identify the already-running primary master so we
    // can skip its entry and avoid an EADDRINUSE port conflict.
    let currentMaster: { host?: string; port?: number } = {};
    const masterFile = path.join(opts.directory, constants.FILEPATH.MASTER);
    if (fs.existsSync(masterFile)) {
        try {
            const masterCfg = JSON.parse(fs.readFileSync(masterFile, 'utf8'));
            const env = process.env.NODE_ENV || 'development';
            currentMaster = masterCfg[env] || masterCfg['development'] || {};
        } catch (e) {
            // Non-fatal: if master.json is unreadable we just won't skip any entry.
        }
    }

    for (let i = 0; i < masterha.length; i++) {
        let server = masterha[i];
        if (server.host === currentMaster.host && server.port === currentMaster.port) {
            continue;
        }
        server.mode = constants.RESERVED.STAND_ALONE;
        server.masterha = 'true';
        server.home = opts.directory;
        runServer(server);
    }
}