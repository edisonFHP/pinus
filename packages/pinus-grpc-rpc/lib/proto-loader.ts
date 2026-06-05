import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';

const PROTO_PATH = path.resolve(__dirname, './proto/pinus_rpc.proto');

const PKG_DEF_OPTS: protoLoader.Options = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};

let _pkgDef: protoLoader.PackageDefinition | null = null;

export function getPkgDef(): protoLoader.PackageDefinition {
    if (!_pkgDef) {
        _pkgDef = protoLoader.loadSync(PROTO_PATH, PKG_DEF_OPTS);
    }
    return _pkgDef;
}
