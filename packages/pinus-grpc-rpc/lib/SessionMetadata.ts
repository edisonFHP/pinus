import * as grpc from '@grpc/grpc-js';

export interface PinusSessionData {
    id?: number;
    uid?: string | number;
    frontendId?: string;
    settings?: { [key: string]: any };
}

const SETTINGS_SIZE_LIMIT = 4096;

export function injectSession(metadata: grpc.Metadata, session: PinusSessionData): void {
    if (!session) { return; }
    if (session.id != null) {
        metadata.set('pinus-session-id', String(session.id));
    }
    if (session.uid != null) {
        metadata.set('pinus-session-uid', String(session.uid));
    }
    if (session.frontendId) {
        metadata.set('pinus-session-frontend-id', session.frontendId);
    }
    const settings = session.settings;
    if (settings && Object.keys(settings).length > 0) {
        const json = JSON.stringify(settings);
        if (json.length < SETTINGS_SIZE_LIMIT) {
            metadata.set('pinus-session-settings', json);
        }
        // If settings exceed limit, only the identifier fields above are transmitted.
        // Server-side code can re-fetch full settings using frontendId + id if needed.
    }
}

export function extractSession(metadata: grpc.Metadata): PinusSessionData {
    const get = (key: string): string => (metadata.get(key)[0] as string) ?? '';
    const idStr = get('pinus-session-id');
    const settingsStr = get('pinus-session-settings');

    const result: PinusSessionData = {};
    if (idStr) { result.id = Number(idStr); }
    const uid = get('pinus-session-uid');
    if (uid) { result.uid = uid; }
    const frontendId = get('pinus-session-frontend-id');
    if (frontendId) { result.frontendId = frontendId; }

    try {
        result.settings = settingsStr ? JSON.parse(settingsStr) : {};
    } catch {
        result.settings = {};
    }

    return result;
}
