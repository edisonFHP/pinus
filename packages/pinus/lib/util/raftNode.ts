import { EventEmitter } from 'events';

export enum RaftRole {
    FOLLOWER = 'follower',
    CANDIDATE = 'candidate',
    LEADER = 'leader'
}

export interface RaftPeer {
    id: string;
    host: string;
    port: number;
    raftPort: number;
}

export class RaftNode extends EventEmitter {
    role: RaftRole = RaftRole.FOLLOWER;
    currentTerm: number = 0;
    leaderId: string | null = null;
    nodeId: string;
    peers: RaftPeer[];

    constructor(nodeId: string, peers: RaftPeer[], raftPort: number) {
        super();
        this.nodeId = nodeId;
        this.peers = peers;
    }

    start(): void {}
    stop(): void {}
}
