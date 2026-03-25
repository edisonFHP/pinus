# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands should be run from the repo root unless noted otherwise.

```bash
# Install dependencies
yarn install

# Build all packages
yarn run build

# Run all tests
yarn run test

# Run tests with coverage
yarn run cov

# Lint all packages
yarn run lint

# Auto-fix lint issues
yarn run fix-lint
```

To run tests for a single package:
```bash
cd packages/pinus
yarn run mochatest   # runs mocha against compiled dist/
yarn run cov         # runs coverage
```

To build a single package:
```bash
cd packages/pinus
yarn run build       # runs tsc
```

## Architecture Overview

Pinus is a distributed, multi-process game server framework for Node.js (TypeScript), inspired by Pomelo. It uses a **master/frontend/backend** server topology with inter-process RPC communication.

### Monorepo Layout

| Path | Purpose |
|------|---------|
| `packages/pinus` | Core framework |
| `packages/pinus-rpc` | Inter-process RPC system |
| `packages/pinus-admin` | Admin monitoring interface |
| `packages/pinus-logger` | Logging abstraction (wraps log4js) |
| `packages/pinus-loader` | Module loader with reflection |
| `packages/pinus-monitor` | OS/process monitoring |
| `packages/pinus-protobuf` | Protobuf message serialization |
| `packages/pinus-protocol` | Wire protocol definitions |
| `packages/pinus-scheduler` | Task scheduling utilities |
| `tools/pinus-cli` | CLI management tool |
| `plugins/` | Optional plugins (gate, base) |
| `examples/` | Working example apps |

### Core Concepts

**Application** (`packages/pinus/lib/application.ts`) — The central orchestrator. `pinus.createApp()` returns an `Application` instance. All components, filters, and configuration hang off it.

**IComponent** — Lifecycle interface implemented by all pluggable subsystems. Each component has: `beforeStart → start → afterStart → afterStartAll → stop`. Key built-in components: `Connector`, `Session`, `Channel`, `Remote`, `Proxy`, `PushScheduler`, `Dictionary`, `Protobuf`, `Monitor`, `Master`.

**Server Roles:**
- **master** — Manages server cluster startup/shutdown
- **frontend** — Accepts client TCP/WebSocket connections; owns client sessions
- **backend** — Runs game logic; receives requests forwarded from frontend via RPC

**RPC / Remote** — `packages/pinus-rpc` provides transparent TypeScript RPC between servers. User-defined RPC services are typed through a global `SysRpc` interface augmentation pattern. Built-in remotes (`msgRemote`, `channelRemote`, `sessionRemote`) handle core framework messaging.

**Session Model** — `FrontendSession` tracks a live client connection on the frontend server. `BackendSession` is a serialized mirror passed to backend handlers. `SessionService` manages all sessions per server.

**Channel** — Named groups of sessions used for broadcasting messages to multiple clients simultaneously.

**Connector** — Pluggable transport layer. Built-in connectors: HybridConnector (custom binary), SioConnector (Socket.IO), UDPConnector, MQTTConnector.

**Push Scheduler** — Controls when messages are flushed to clients. `DirectPushScheduler` sends immediately; `BufferPushScheduler` batches by interval.

**Filters** — Middleware for handler pipeline (`before`/`after`) and RPC calls. Built-ins: `serial`, `timeout`, `toobusy`, `time`.

### Request Flow

```
Client → Connector → Session → Proxy → Handler (frontend)
                                    ↓ RPC (if backend route)
                              Remote → Handler (backend)
                                    ↓
                         Channel.pushMessage → Connector → Client
```

### TypeScript Build

Each package compiles independently via `tsc`. Source is in `lib/`, compiled output goes to `dist/`. Tests are in `test/` and compiled to `dist/test/`. The `mochatest` script runs mocha against `dist/test/**/*.js`.

### Testing Standards

Per `CONTRIBUTING.md`: statement coverage ≥ 90%, branch coverage ≥ 80%. Tests use Mocha. Run `yarn run cov` in any package to check coverage.
