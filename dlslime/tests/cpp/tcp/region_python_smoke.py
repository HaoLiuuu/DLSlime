# Python HOST ownership and acknowledged transfers.
import gc
import json

import _slime_c as m

a, b = m.TcpRegionEndpoint("127.0.0.1"), m.TcpRegionEndpoint("127.0.0.1")
try:
    source, destination = m.HostBuffer(8194), m.HostBuffer(8194)
    sv, dv = memoryview(source), memoryview(destination)
    sv[:] = bytes(i % 251 for i in range(len(sv)))
    dv[:] = bytes([255]) * len(dv)
    ad = a.regions().register_buffer(source)
    bd = b.regions().register_buffer(destination)
    peer = m.TcpRegionPeer.from_dict(
        json.loads(json.dumps(b.endpoint_info().to_dict()))
    )
    remote = m.RegionDescriptor.from_dict(json.loads(json.dumps(bd.to_dict())))
    spans = [
        m.TcpRegionSpan(ad.key, remote, 1 + i * 64, 1 + i * 64, 64) for i in range(128)
    ]
    assert a.write(peer, spans).wait() == 0
    assert dv[1:-1] == sv[1:-1] and dv[0] == dv[-1] == 255
    sv[1:-1] = bytes(8192)
    assert a.read(peer, spans).wait() == 0 and sv[1:-1] == dv[1:-1]
    old = remote
    b.regions().retire(bd.key).wait()
    fresh = b.regions().register_buffer(destination)
    assert fresh.key.generation != old.key.generation
    assert a.read(peer, [m.TcpRegionSpan(ad.key, old, 0, 0, 1)]).wait() == -4
    del source, sv
    gc.collect()
    # Registry owns the source allocation even after its Python wrapper dies.
    assert a.write(peer, [m.TcpRegionSpan(ad.key, fresh, 1, 1, 8192)]).wait() == 0
    a.regions().retire(ad.key).wait()
    print(
        "PASS Python HOST ownership, 128-span ACK/READ, metadata roundtrip, stale generation"
    )
finally:
    a.shutdown()
    b.shutdown()
