#!/usr/bin/env python3
"""Point existing verify launcher at the v2 host-mounted job; no Docker build."""
from pathlib import Path
from datetime import datetime
import shutil


def patch(text):
    old = '/opt/it5427/spark_hdfs_demo.py'
    new = '/project/scripts/spark_hdfs_demo_v2.py'
    if new in text:
        if text.count(new) != 1 or old in text:
            raise ValueError('Ambiguous launcher paths; no change made')
        return text
    if text.count(old) != 1:
        raise ValueError('Unexpected manage.py; send this file for review')
    # Require the already-tested timeout/AQE patch; do not silently remove guards.
    if "'spark.sql.adaptive.enabled=false'" not in text or "'600s'" not in text:
        raise ValueError('Expected AQE=false and 600s timeout patch is missing; no change made')
    updated = text.replace(old, new, 1)
    compile(updated, 'manage.py', 'exec')
    return updated


def main():
    project = Path(__file__).resolve().parent.parent
    job = project/'scripts/spark_hdfs_demo_v2.py'
    target = project/'infra/hdfs/manage.py'
    if not job.is_file() or not target.is_file():
        raise SystemExit('Copy both scripts into IT5427_TON_IoT/scripts first')
    compile(job.read_text(encoding='utf-8'), str(job), 'exec')
    text = target.read_text(encoding='utf-8')
    updated = patch(text)
    if updated == text:
        print('Already installed. Run: python infra/hdfs/manage.py verify')
        return
    backup = target.with_name('manage.py.before-roundtrip-v2-'+datetime.now().strftime('%Y%m%d_%H%M%S_%f'))
    shutil.copy2(target, backup)
    target.write_text(updated, encoding='utf-8')
    print('Backup:', backup)
    print('Updated:', target)
    print('No rebuild required. Run: python infra/hdfs/manage.py verify')


if __name__ == '__main__':
    main()
