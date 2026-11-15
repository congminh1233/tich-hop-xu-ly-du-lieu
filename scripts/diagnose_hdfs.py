#!/usr/bin/env python3
"""Bounded Spark diagnostics using the existing IT5427 Compose image (no rebuild)."""
import argparse
import faulthandler
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
import zipfile
from datetime import datetime, timezone

MODES = ('local', 'hdfs', 'demo')


def mark(message):
    print(datetime.now(timezone.utc).isoformat(), message, flush=True)


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding='utf-8')


def java_dump(group):
    """Signal only this probe's Java SparkSubmit, never the supervisor or Python."""
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            pid = int(proc.name)
            args = (proc / 'cmdline').read_bytes().split(b'\0')
            if (os.getpgid(pid) == group and (proc / 'exe').resolve().name == 'java'
                    and b'org.apache.spark.deploy.SparkSubmit' in args and b'-Xrs' not in args):
                mark(f'JAVA_THREAD_DUMP pid={pid}')
                os.kill(pid, signal.SIGQUIT)
        except (OSError, ValueError):
            pass


def supervise(command, seconds=150, dump_after=(60, 120), dump=java_dump):
    """Child process group has a hard deadline, independently of stdout activity."""
    started = time.monotonic()
    process = subprocess.Popen(command, start_new_session=True)
    sent = set()
    timed_out = False
    try:
        while process.poll() is None:
            elapsed = time.monotonic() - started
            for threshold in dump_after:
                if elapsed >= threshold and threshold not in sent:
                    sent.add(threshold)
                    dump(process.pid)
            if elapsed >= seconds:
                timed_out = True
                mark(f'TIMEOUT after {seconds}s; stopping this probe only')
                break
            time.sleep(0.2)
    finally:
        # The parent can exit before a child; clean up the entire private group.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
    return {'exit_code': 124 if timed_out else process.returncode,
            'timed_out': timed_out, 'elapsed_seconds': round(time.monotonic() - started, 2)}


def inside(a):
    report = Path(a.report)
    events = report / (a.mode + '-events')
    events.mkdir()
    cmd = ['spark-submit', '--master', 'local[2]', '--driver-memory', '2g',
           '--conf', 'spark.sql.adaptive.enabled=false',
           '--conf', 'spark.sql.shuffle.partitions=8',
           '--conf', 'spark.ui.enabled=false',
           '--conf', 'spark.eventLog.enabled=true',
           '--conf', 'spark.eventLog.compress=false',
           '--conf', 'spark.eventLog.dir=' + events.as_uri(),
           str(Path(__file__).resolve()), '--driver', '--mode', a.mode,
           '--snapshot', a.snapshot, '--report', a.report, '--run-id', a.run_id]
    mark('SPARK_COMMAND ' + ' '.join(cmd))
    result = supervise(cmd)
    save(report / (a.mode + '-process.json'), result)
    return result['exit_code'] if result['exit_code'] >= 0 else 1


def step(name, operation):
    mark('BEGIN ' + name)
    start = time.monotonic()
    value = operation()
    mark('END ' + name + f' seconds={time.monotonic()-start:.2f}')
    return value


def driver(a):
    faulthandler.enable()
    faulthandler.dump_traceback_later(45, repeat=True)
    report = Path(a.report)
    mark('DRIVER ' + a.mode)
    if a.mode == 'demo':
        # Run the original algorithm from the image, with INFO logging and line traces.
        original = Path('/opt/it5427/spark_hdfs_demo.py')
        source = original.read_text()
        save(report / 'demo-source.json', {'original_sha256': hashlib.sha256(source.encode()).hexdigest(),
             'changes': ['WARN logging changed to INFO', 'Python line tracing enabled'],
             'aqe_enabled': False})
        (report / 'demo-original.py').write_text(source)
        instrumented = source.replace("setLogLevel('WARN')", "setLogLevel('INFO')")
        lines = instrumented.splitlines()
        sys.path.insert(0, '/opt/it5427')
        namespace = {'__name__': 'diagnostic_original', '__file__': str(original)}
        exec(compile(instrumented, str(original), 'exec'), namespace)
        def trace(frame, event, arg):
            if frame.f_code.co_filename == str(original):
                if event == 'line':
                    n = frame.f_lineno
                    mark(f'ORIGINAL_LINE {n}: {lines[n-1].strip()}')
                return trace
            return None
        sys.settrace(trace)
        try:
            namespace['run'](a.snapshot, a.run_id, str(report / 'demo-result.json'))
        finally:
            sys.settrace(None)
            faulthandler.cancel_dump_traceback_later()
        return 0

    from pyspark.sql import SparkSession, functions as F
    spark = step('create_spark', lambda: SparkSession.builder.appName('IT5427 diagnostic '+a.mode).getOrCreate())
    spark.sparkContext.setLogLevel('INFO')
    config = {'spark': spark.version,
              'aqe': spark.conf.get('spark.sql.adaptive.enabled'),
              'default_fs': spark.sparkContext._jsc.hadoopConfiguration().get('fs.defaultFS'),
              'java': spark._jvm.java.lang.System.getProperty('java.version')}
    mark('CONFIG ' + json.dumps(config))
    save(report / (a.mode + '-config.json'), config)
    try:
        if a.mode == 'local':
            rows = step('local_range_collect', lambda: spark.range(10).collect())
            assert [r.id for r in rows] == list(range(10))
            grouped = spark.range(1000).select((F.col('id') % 4).alias('bucket')).groupBy('bucket').count()
            rows = step('local_shuffle_collect', grouped.collect)
            assert sorted((r.bucket, r['count']) for r in rows) == [(i, 250) for i in range(4)]
            with tempfile.TemporaryDirectory(prefix='it5427-probe-') as temp:
                uri = Path(temp).as_uri() + '/parquet'
                step('local_parquet_write', lambda: spark.range(100).write.mode('error').parquet(uri))
                count = step('local_parquet_count', lambda: spark.read.parquet(uri).count())
                assert count == 100
            result = {'status': 'passed', 'scope': 'Synthetic data + local container filesystem; no HDFS data reads'}
        else:
            base = 'hdfs://namenode:9000/it5427/snapshots/' + a.snapshot
            raw = step('hdfs_manifest_cli', lambda: subprocess.run(
                ['hdfs', 'dfs', '-cat', base+'/import_manifest.json'],
                capture_output=True, text=True, check=True, timeout=25).stdout)
            manifest = json.loads(raw)
            assert manifest['snapshot_id'] == a.snapshot
            counts = {}
            for table, expected in manifest['table_rows'].items():
                if table not in ('host_window_features', 'sensor_window_features', 'network_overview'):
                    raise ValueError('Unexpected table: '+table)
                df = step(table+'.read_schema', lambda: spark.read.parquet(base+'/gold/'+table))
                step(table+'.read_first_row', lambda: df.limit(1).collect())
                counts[table] = step(table+'.count', df.count)
                assert counts[table] == expected, (table, counts[table], expected)
            result = {'status': 'passed', 'counts': counts}
        save(report / (a.mode + '-result.json'), result)
        mark('PROBE_PASSED '+a.mode)
    finally:
        step('spark_stop', spark.stop)
        faulthandler.cancel_dump_traceback_later()
    return 0


def stream_command(command, log, timeout=205, cleanup=None):
    with Path(log).open('w', encoding='utf-8') as output:
        mark('RUN ' + ' '.join(command))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, encoding='utf-8', errors='replace')
        def drain():
            for line in process.stdout:
                output.write(line)
                output.flush()
                print(line, end='', flush=True)
        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        try:
            code = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            if cleanup:
                try:
                    subprocess.run(cleanup, stdout=output, stderr=output, timeout=20)
                except subprocess.TimeoutExpired:
                    pass
            process.kill()
            process.wait(timeout=5)
            code = 124
        reader.join(timeout=5)
    return code


def host(a):
    project = Path(__file__).resolve().parent.parent
    compose = project/'infra/hdfs/compose.yaml'
    if not compose.is_file():
        raise SystemExit('Copy this file into IT5427_TON_IoT/scripts/diagnose_hdfs.py first.')
    if not hasattr(os, 'getuid') or os.getuid() == 0:
        raise SystemExit('Run in WSL as your normal user, not sudo.')
    run_id = 'diagnose-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    folder = project/'reports/hdfs'/run_id
    folder.mkdir(parents=True)
    dc = ['docker', 'compose', '-f', str(compose)]
    result = {'run_id': run_id, 'snapshot': a.snapshot, 'aqe_enabled': False, 'probes': {},
              'scope': 'Diagnostic probes; not a replacement for manage.py verify summary'}
    try:
        for service in ('namenode', 'datanode'):
            code = stream_command(dc+['exec', '-T', service, 'python', '/opt/it5427/health.py', service],
                                  folder/(service+'-health.log'), timeout=20)
            if code:
                raise RuntimeError(service+' health check failed')
        for mode in MODES:
            name = 'it5427-'+run_id+'-'+mode
            command = dc+['run', '--rm', '--no-deps', '--name', name, 'client',
                          'python', '-u', '/project/scripts/diagnose_hdfs.py', '--inside',
                          '--mode', mode, '--snapshot', a.snapshot,
                          '--report', '/reports/'+run_id, '--run-id', run_id]
            code = stream_command(command, folder/(mode+'.log'),
                                  cleanup=['docker', 'stop', '--timeout', '10', name])
            result['probes'][mode] = {'exit_code': code}
            report = folder/(mode+'-result.json')
            if code != 0 or not report.exists() or json.loads(report.read_text()).get('status') != 'passed':
                result['status'] = 'failed'
                result['first_failed_probe'] = mode
                break
        else:
            result['status'] = 'all_probes_passed'
    except Exception as exc:
        result['status'] = 'diagnostic_error'
        result['error'] = str(exc)
        traceback.print_exc()
    finally:
        save(folder/'diagnosis.json', result)
        archive = folder.with_suffix('.zip')
        with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED) as z:
            for path in sorted(folder.rglob('*')):
                if path.is_file() and not path.is_symlink():
                    z.write(path, path.relative_to(folder.parent).as_posix())
        mark('SEND_THIS_FILE '+str(archive))
    return 0 if result.get('status') == 'all_probes_passed' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    roles = parser.add_mutually_exclusive_group()
    roles.add_argument('--inside', action='store_true')
    roles.add_argument('--driver', action='store_true')
    parser.add_argument('--mode', choices=MODES)
    parser.add_argument('--report')
    parser.add_argument('--run-id')
    parser.add_argument('--snapshot', default='2f14f1cbb8e6c322fa283c26')
    a = parser.parse_args()
    if not re.fullmatch('[0-9a-f]{24}', a.snapshot):
        parser.error('Invalid snapshot ID')
    if a.inside or a.driver:
        if not a.mode or not a.report or not a.run_id:
            parser.error('Internal mode requires --mode, --report, --run-id')
        if not re.fullmatch('diagnose-[A-Za-z0-9_-]+', a.run_id):
            parser.error('Invalid run ID')
        return inside(a) if a.inside else driver(a)
    return host(a)


if __name__ == '__main__':
    sys.exit(main())
