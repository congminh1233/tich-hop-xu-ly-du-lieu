"""Synthetic contract tests. These are not TON_IoT detection results."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'infra/serving/app'))
import core
import loader


def row(version):
    r=dict(window_start=1554076800,split='test',score=2.5,threshold=2.0,prediction=1,
           snapshot_id='test-snapshot',label_observed=0)
    if version=='iot-baseline-1.0':r.update(sensor_stream_id='iot_weather',model='combined')
    else:
        r.update(entity_id='windows10_host',label_network=0,label_os=0)
        if version=='host-comparison-1.0':r.update(family='logistic',group='combined')
        else:r.update(model='combined',top_feature='host_cpu_max',top_feature_value=99.0)
    return r


def experiment(tmp,version,rows=None):
    root=tmp/version;root.mkdir(parents=True)
    rows=rows if rows is not None else [row(version)]
    meta=dict(status='completed',script_version=version,snapshot_id='test-snapshot',scored_rows=len(rows),
              metrics=[],warnings=['SYNTHETIC TEST ONLY'])
    for name,data in [('summary.json',meta),('models.json',{}),('input_checksums.json',{})]:
        (root/name).write_text(json.dumps(data))
    pq.write_table(pa.Table.from_pylist(rows),root/'scores.parquet')
    return core.manifest(tmp,root),meta


@pytest.mark.parametrize('version',list(core.VERSIONS))
def test_real_parquet_adapter_roundtrip(tmp_path,version):
    item,meta=experiment(tmp_path,version)
    raw=pq.read_table(tmp_path/item['path']/'scores.parquet').to_pylist()[0]
    result=core.normalize(raw,meta,item['experiment_id'])
    assert result[1]==core.VERSIONS[version] and result[9]==1
    assert json.loads(result[-1])==raw
    assert loader.check_item(tmp_path,item)==tmp_path/item['path']

@pytest.mark.parametrize('changes',[{'snapshot_id':'other'},{'score':float('nan')},{'score':float('inf')},
 {'prediction':0},{'window_start':1554076801},{'split':'unknown'},{'label_observed':2},{'model':None}])
def test_reject_bad_rows(changes):
    r=row('host-baseline-1.0');r.update(changes)
    with pytest.raises(ValueError):core.normalize(r,{'script_version':'host-baseline-1.0','snapshot_id':'test-snapshot'},'eid')


def test_source_change_detected(tmp_path):
    item,_=experiment(tmp_path,'host-baseline-1.0')
    (tmp_path/item['path']/'models.json').write_text('{"changed":true}')
    with pytest.raises(ValueError,match='changed'):loader.check_item(tmp_path,item)


def test_missing_required_file(tmp_path):
    item,_=experiment(tmp_path,'iot-baseline-1.0')
    (tmp_path/item['path']/'models.json').unlink()
    with pytest.raises(ValueError,match='Missing'):core.manifest(tmp_path,item['path'])


def test_outside_project(tmp_path):
    with pytest.raises(ValueError):core.inside(tmp_path,'../outside')


def test_payload_digest_preserves_numeric_text():
    v='host-baseline-1.0';meta={'script_version':v,'snapshot_id':'test-snapshot'}
    raw=row(v);raw['top_feature_value']=1e20
    r=core.normalize(raw,meta,'eid')
    assert isinstance(r[-1],str)
    assert core.add_digest(0,r)==core.add_digest(0,tuple(r))
    altered=list(r);altered[9]=0
    assert core.add_digest(0,r)!=core.add_digest(0,altered)


def test_discovery_only_scored_experiments(tmp_path):
    spec=importlib.util.spec_from_file_location('serving_manage',ROOT/'infra/serving/manage.py')
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    (tmp_path/'toniot').mkdir();(tmp_path/'infra/serving').mkdir(parents=True)
    experiment(tmp_path/'experiments','host-baseline-1.0')
    prepared=tmp_path/'experiments/host_eval_v2';prepared.mkdir()
    (prepared/'summary.json').write_text('{"script_version":"host-eval-2.0"}')
    with patch.object(m,'PROJECT',tmp_path),patch.object(m,'HERE',tmp_path/'infra/serving'),patch.object(m,'dc'):
        m.configure([])
    cfg=core.read(tmp_path/'infra/serving/config.json')
    assert len(cfg['experiments'])==1
    assert (tmp_path/'infra/serving/.env').stat().st_mode & 0o777==0o600


def test_postgres_transactions_and_replay(tmp_path):
    """Opt-in: TEST_PG_DSN must refer to a disposable test database, never production."""
    dsn=os.environ.get('TEST_PG_DSN')
    if not dsn:pytest.skip('No PostgreSQL service available here; user verify command performs real replay check')
    import psycopg
    with psycopg.connect(dsn) as conn:
        loader.initialize(conn);conn.commit()
        ids=[]
        try:
            for version in core.VERSIONS:
                item,_=experiment(tmp_path,version);ids.append(item['experiment_id'])
                assert loader.load_one(conn,tmp_path,item)['action']=='inserted'
                assert loader.load_one(conn,tmp_path,item)['action']=='reused'
                assert loader.verify_one(conn,tmp_path,item)['rows']==1
                conn.commit()
            dup=[row('host-baseline-1.0')]*2
            item,_=experiment(tmp_path/'duplicate','host-baseline-1.0',dup);ids.append(item['experiment_id'])
            with pytest.raises(psycopg.errors.UniqueViolation):loader.load_one(conn,tmp_path/'duplicate',item)
            assert conn.execute('SELECT count(*) FROM serving.experiments WHERE experiment_id=%s',(item['experiment_id'],)).fetchone()[0]==0
            conn.commit()
        finally:
            conn.rollback()
            conn.execute('DELETE FROM serving.scores WHERE experiment_id=ANY(%s)',(ids,))
            conn.execute('DELETE FROM serving.experiments WHERE experiment_id=ANY(%s)',(ids,))
            conn.commit()
