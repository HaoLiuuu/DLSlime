#!/usr/bin/env python3
"""Two-process DRAM TCP matrix. All payload I/O uses production TcpEndpoint."""
import argparse
import concurrent.futures
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import re
import secrets
import select
import socket
import subprocess
import threading
import time

SIZES = [1024, 16384, 262144, 1048576, 4194304, 16777216]
BATCHES = [1, 4, 16, 64]
MODES = [('READ', 'A'), ('READ', 'B'), ('READ', 'both'),
         ('WRITE', 'A'), ('WRITE', 'B'), ('WRITE', 'both')]


def utc():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def save(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    tmp.replace(path)


def cpu():
    values = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    return sum(values), values[3] + values[4]


def gpu_util(kind):
    commands = {'metax': ['mx-smi'], 'ascend': ['npu-smi', 'info'],
                'h200': ['nvidia-smi', '--query-gpu=utilization.gpu', '--format=csv,noheader,nounits']}
    result = subprocess.run(commands[kind], text=True, capture_output=True, timeout=12, check=True)
    if kind == 'h200':
        values = [int(x.strip()) for x in result.stdout.splitlines() if x.strip()]
    elif kind == 'metax':
        values = [int(x) for x in re.findall(r'\|\s*(\d+)%', result.stdout)]
    else:
        values = [int(x) for x in re.findall(r'^\|\s*\d+\s*\|\s*[\da-fA-F:.]+\s*\|\s*(\d+)\s+', result.stdout, re.M)]
    if len(values) != 8:
        raise RuntimeError('GPU utilization inventory incomplete')
    return values


class LoadMonitor:
    """Only reads telemetry. A busy sample invalidates an overlapping trial."""
    def __init__(self, kind, interface, stable_seconds):
        self.kind, self.interface, self.stable_seconds = kind, interface, stable_seconds
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.data = {'ready': False, 'reason': 'collecting idle baseline', 'busy_epoch': 0}
        self.updated = time.monotonic()
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.thread.start()

    def watch(self):
        previous = cpu()
        idle_since = None
        while not self.stop.wait(5):
            try:
                current = cpu()
                busy = 100 * (1 - (current[1] - previous[1]) / max(1, current[0] - previous[0]))
                previous = current
                util = gpu_util(self.kind)
                reasons = []
                if busy >= 5:
                    reasons.append('host CPU >= 5%')
                if max(util) >= 5:
                    reasons.append('accelerator utilization >= 5%')
                if reasons:
                    idle_since = None
                elif idle_since is None:
                    idle_since = time.monotonic()
                ready = idle_since is not None and time.monotonic() - idle_since >= self.stable_seconds
                with self.lock:
                    self.updated = time.monotonic()
                    self.data = {'utc': utc(), 'ready': ready, 'cpu_busy_percent': busy,
                                 'gpu_util_percent': util, 'reason': ', '.join(reasons) if reasons else ('idle' if ready else 'confirming idle'),
                                 'busy_epoch': self.data['busy_epoch'] + bool(reasons)}
            except Exception as error:
                idle_since = None
                with self.lock:
                    self.updated = time.monotonic()
                    self.data = {'utc': utc(), 'ready': False, 'reason': str(error),
                                 'busy_epoch': self.data['busy_epoch'] + 1}

    def status(self, network=False):
        with self.lock:
            data = dict(self.data)
            if time.monotonic() - self.updated > 20:
                data.update(ready=False, reason='stale load telemetry')
        if network:
            def counters():
                base = Path('/sys/class/net') / self.interface / 'statistics'
                return [int((base / name).read_text()) for name in ['rx_bytes', 'tx_bytes']]
            try:
                a = counters()
                start = time.monotonic()
                time.sleep(1)
                b = counters()
                rates = [(y - x) * 8 / (time.monotonic() - start) / 1e6 for x, y in zip(a, b)]
                data['background_net_mbps'] = rates
                if max(rates) >= 100:
                    data.update(ready=False, reason='background network >= 100 Mb/s')
            except Exception as error:
                data.update(ready=False, reason='network telemetry: ' + str(error))
        return data


class Agent:
    def __init__(self, binary, cpus, log):
        self.stderr = open(log, 'a')
        self.proc = subprocess.Popen([binary, '--cpus', cpus], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.stderr, text=True, bufsize=1)

    def call(self, command):
        self.proc.stdin.write(json.dumps(command) + '\n')
        self.proc.stdin.flush()
        if not select.select([self.proc.stdout], [], [], 120)[0]:
            raise TimeoutError('benchmark command exceeded 120 seconds')
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError('benchmark process exited: ' + str(self.proc.poll()))
        value = json.loads(line)
        if not value.get('ok'):
            raise RuntimeError(value.get('error', value))
        return value

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.stderr.close()


class Remote:
    def __init__(self, config):
        self.sock = socket.create_connection((config['host'], config['port']), timeout=120)
        self.file = self.sock.makefile('rw', buffering=1)
        self.call({'command': 'authenticate', 'token': config['token']})

    def call(self, command):
        self.file.write(json.dumps(command) + '\n')
        self.file.flush()
        line = self.file.readline()
        if not line:
            raise RuntimeError('peer control connection closed')
        value = json.loads(line)
        if not value.get('ok'):
            raise RuntimeError(value.get('error', value))
        return value

    def close(self):
        self.file.close()
        self.sock.close()


def metadata(args):
    tcp = Path(__file__).resolve().parents[3] / 'dlslime/csrc/engine/tcp'
    cache = Path(args.binary).parent / 'CMakeCache.txt'
    cache_text = cache.read_text()
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in cache_text or 'TCP_SANITIZE:BOOL=OFF' not in cache_text:
        raise RuntimeError('performance suite requires Release and TCP_SANITIZE=OFF')
    files = ['suite.py', 'tcp_dram_bench.cpp', 'CMakeLists.txt']
    return {'utc': utc(), 'platform': platform.platform(), 'host_kind': args.kind,
            'cpus_a': args.cpus_a, 'cpus_b': args.cpus_b, 'build': 'Release, no sanitizers',
            'transport_sha256': {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(tcp.glob('*')) if f.is_file()},
            'source_sha256': {f: hashlib.sha256(Path(__file__).with_name(f).read_bytes()).hexdigest() for f in files},
            'binary_sha256': hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
            'kernel': platform.release(), 'numa_policy': 'first touch; per-process CPU affinity',
            'outstanding_batches_per_initiator': 1, 'rounds': args.rounds,
            'warmup_seconds': args.warmup, 'measure_seconds': args.seconds,
            'span_sizes': args.sizes, 'batch_sizes': args.batches,
            'loopback': args.remote is None, 'guard_stable_seconds': args.stable_seconds,
            'write_completion': 'local retirement plus same-stream READ fence at trial end',
            'verification': 'whole final batch and 64-byte guards after every warmup and measured trial'}


def serve(args, monitor, out):
    agent = None
    token = secrets.token_hex(32)
    with socket.socket() as listener:
        listener.bind((args.bind_ip, 0))
        listener.listen(1)
        manifest = {'host': args.bind_ip, 'port': listener.getsockname()[1], 'token': token}
        save(out / 'peer-private.json', manifest)
        os.chmod(out / 'peer-private.json', 0o600)
        save(out / 'status.json', {'state': 'listening', 'utc': utc(), 'port': manifest['port']})
        connection, _ = listener.accept()
        try:
            with connection, connection.makefile('rw', buffering=1) as channel:
                connection.settimeout(600)
                hello = json.loads(channel.readline(65536))
                if hello.get('command') != 'authenticate' or not secrets.compare_digest(hello.get('token', ''), token):
                    raise RuntimeError('authentication failed')
                channel.write(json.dumps({'ok': True}) + '\n'); channel.flush()
                agent = Agent(args.binary, args.cpus_b, out / 'peer.stderr')
                for line in channel:
                    command = json.loads(line)
                    action = command.get('command')
                    if action == 'load':
                        response = {'ok': True, 'load': monitor.status(command.get('network', False))}
                    elif action == 'metadata':
                        response = {'ok': True, 'metadata': metadata(args)}
                    elif action in ['setup', 'connect', 'prepare', 'run', 'verify', 'close', 'exit']:
                        if action == 'setup':
                            command['bind_ip'] = args.bind_ip
                        response = agent.call(command)
                    else:
                        raise RuntimeError('unsupported control command')
                    channel.write(json.dumps(response) + '\n'); channel.flush()
                    if action == 'exit':
                        save(out / 'status.json', {'state': 'complete', 'utc': utc()})
                        break
        finally:
            if agent is not None:
                agent.close()


def matrix(args, monitor, out):
    remote_config = json.loads(Path(args.remote).read_text()) if args.remote else None
    a = Agent(args.binary, args.cpus_a, out / 'a.stderr')
    b = Remote(remote_config) if remote_config else Agent(args.binary, args.cpus_b, out / 'b.stderr')
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    def both(command_a, command_b=None):
        fa = pool.submit(a.call, command_a)
        fb = pool.submit(b.call, command_a if command_b is None else command_b)
        return fa.result(), fb.result()
    def loads(network=False):
        fa = pool.submit(monitor.status, network)
        fb = pool.submit(b.call, {'command': 'load', 'network': network}) if remote_config else None
        own = fa.result()
        return [own, fb.result()['load'] if fb else own]
    done = set()
    raw = out / 'trials.jsonl'
    if raw.exists():
        for line in raw.read_text().splitlines():
            value = json.loads(line)
            if value.get('valid'):
                done.add((value['case_id'], value['round']))
    cases = [(s, batch, op, mode) for s in args.sizes for batch in args.batches for op, mode in MODES]
    random.Random(20261014).shuffle(cases)
    total = len(cases) * args.rounds
    meta = metadata(args)
    if remote_config:
        meta['peer'] = b.call({'command': 'metadata'})['metadata']
    meta['case_order'] = cases
    meta['expected_valid_trials'] = total
    meta_path = out / 'metadata.json'
    if meta_path.exists():
        previous = json.loads(meta_path.read_text())
        for key in ['source_sha256', 'binary_sha256', 'cpus_a', 'cpus_b', 'rounds', 'warmup_seconds', 'measure_seconds', 'case_order', 'loopback', 'transport_sha256']:
            if previous[key] != meta[key]:
                raise RuntimeError('resume configuration changed: ' + key)
        if previous.get('peer') and previous['peer']['binary_sha256'] != meta['peer']['binary_sha256']:
            raise RuntimeError('peer binary changed during resume')
    else:
        save(meta_path, meta)
    try:
        with raw.open('a', buffering=1) as result:
            for size, batch, op, mode in cases:
                case_id = f'{size}-{batch}-{op}-{mode}'
                if all((case_id, n) in done for n in range(args.rounds)):
                    continue
                status = {'state': 'waiting_idle', 'utc': utc(), 'case_id': case_id, 'valid_trials': len(done), 'total_trials': total}
                while True:
                    current = loads(network=True)
                    save(out / 'status.json', {**status, 'load': current, 'utc': utc()})
                    if all(x['ready'] for x in current):
                        break
                    time.sleep(5)
                setup = {'command': 'setup', 'span_bytes': size, 'batch_size': batch, 'bind_ip': args.bind_ip}
                ia, ib = both(setup)
                both({'command': 'connect', 'endpoint': ib['endpoint']}, {'command': 'connect', 'endpoint': ia['endpoint']})
                for n in range(args.rounds):
                    if (case_id, n) in done:
                        continue
                    while True:
                        before = loads()
                        status.update(round=n, valid_trials=len(done), utc=utc(), load=before)
                        if not all(x['ready'] for x in before):
                            status['state'] = 'waiting_idle'; save(out / 'status.json', status)
                            time.sleep(5)
                            continue
                        status['state'] = 'running'; save(out / 'status.json', status)
                        active_a, active_b = mode in ['A', 'both'], mode in ['B', 'both']
                        measurements = None
                        for stage, duration in [('warmup', args.warmup), ('measure', args.seconds)]:
                            seed_a, seed_b = (n + 1) * 10 + (stage == 'measure'), (n + 1) * 10 + 4 + (stage == 'measure')
                            ca = {'command': 'prepare', 'operation': op, 'active': active_a, 'seed': seed_a, 'peer_seed': seed_b}
                            cb = {'command': 'prepare', 'operation': op, 'active': active_b, 'seed': seed_b, 'peer_seed': seed_a}
                            both(ca, cb)
                            begin = time.monotonic()
                            ra, rb = both({'command': 'run', 'seconds': duration})
                            controller_seconds = time.monotonic() - begin
                            both({'command': 'verify'})
                            if stage == 'measure':
                                measurements = [ra['result'], rb['result']]
                                interval = controller_seconds
                        after = loads()
                        valid = all(x['ready'] and x['busy_epoch'] == y['busy_epoch'] for x, y in zip(after, before))
                        value = {'utc': utc(), 'case_id': case_id, 'round': n, 'operation': op, 'initiators': mode,
                                 'span_bytes': size, 'batch_size': batch, 'valid': valid, 'verified': True,
                                 'load_before': before, 'load_after': after, 'peers': measurements,
                                 'controller_seconds': interval,
                                 'aggregate_confirmed_gbps': sum(x['payload_bytes'] for x in measurements) * 8 / interval / 1e9}
                        result.write(json.dumps(value) + '\n'); result.flush(); os.fsync(result.fileno())
                        if valid:
                            done.add((case_id, n))
                            break
                both({'command': 'close'})
        both({'command': 'exit'})
        save(out / 'status.json', {'state': 'complete', 'utc': utc(), 'valid_trials': len(done), 'total_trials': total})
    finally:
        a.close(); b.close(); pool.shutdown(wait=False, cancel_futures=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', required=True, choices=['metax', 'ascend', 'h200'])
    parser.add_argument('--binary', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--interface', required=True)
    parser.add_argument('--bind-ip', default='127.0.0.1')
    parser.add_argument('--cpus-a', default='2,3')
    parser.add_argument('--cpus-b', default='4,5')
    parser.add_argument('--rounds', type=int, default=50)
    parser.add_argument('--warmup', type=float, default=1)
    parser.add_argument('--seconds', type=float, default=3)
    parser.add_argument('--sizes', type=int, nargs='+', default=SIZES)
    parser.add_argument('--batches', type=int, nargs='+', default=BATCHES)
    parser.add_argument('--stable-seconds', type=float, default=60)
    parser.add_argument('--remote', help='private peer manifest path; never publish its token')
    parser.add_argument('--serve', action='store_true')
    parser.add_argument('--lock', default='/opt/dlslime-dev/tcp-benchmark.lock')
    args = parser.parse_args()
    if not 1 <= args.rounds <= 10000 or not 0 < args.warmup <= 60 or not 0 < args.seconds <= 60:
        parser.error('invalid rounds or duration')
    if any(s < 8 or s % 8 or s * b > 2**30 or b < 1 or b > 4096 for s in args.sizes for b in args.batches):
        parser.error('size/batch exceeds 1 GiB per batch or is not a multiple of 8')
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    with open(args.lock, 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        monitor = LoadMonitor(args.kind, args.interface, args.stable_seconds)
        try:
            if args.serve:
                serve(args, monitor, out)
            else:
                matrix(args, monitor, out)
        except Exception as error:
            save(out / 'status.json', {'state': 'failed', 'utc': utc(), 'error': str(error)})
            raise
        finally:
            monitor.stop.set()


if __name__ == '__main__':
    main()
