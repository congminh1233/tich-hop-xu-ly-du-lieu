"""Real Streamlit AppTest with synthetic database responses (no live DB)."""
from datetime import datetime,timezone
import json
from pathlib import Path
import sys
from unittest.mock import patch
import pytest
from streamlit.testing.v1 import AppTest
import streamlit as st
ROOT=Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('version',['host-baseline-1.0','host-comparison-1.0','iot-baseline-1.0'])
def test_dashboard_renders(version,monkeypatch):
    for name in ('PGHOST','PGDATABASE','PGUSER','PGPASSWORD'):monkeypatch.setenv(name,'synthetic')
    subject='iot_weather' if version.startswith('iot') else 'windows10_host'
    family='logistic' if 'comparison' in version else version
    metric={'sensor_stream_id' if version.startswith('iot') else 'entity_id':subject,'split':'test',
            'tp':0,'fp':1,'tn':0,'fn':0,'n':1,'f1':None,'precision':0.0,'recall':None}
    metric.update({'family':family,'group':'combined'} if 'comparison' in version else {'model':'combined'})
    exp={'experiment_id':'synthetic-only','source_path':'experiments/synthetic','script_version':version,
         'snapshot_id':'synthetic','manifest':{},'summary':{'metrics':[metric],'warnings':['Synthetic data']}}
    raw={'top_feature':'example','label_observed':0}
    def answer(sql,args):
        if 'FROM serving.experiments' in sql:return [exp]
        if 'SELECT DISTINCT subject_id' in sql:return [{'subject_id':subject,'family':family,'feature_group':'combined','split':'test'}]
        if 'min(window_start)' in sql:return [{'lo':1554076800,'hi':1554076800}]
        if 'count(label_observed)' in sql:return [{'n':1,'alerts':1,'known':1}]
        if 'GROUP BY 1' in sql:return [{'epoch':1554076800,'windows':1,'alerts':1}]
        if 'SELECT window_start,score,threshold FROM' in sql:return [{'window_start':1554076800,'score':2.5,'threshold':2.0}]
        if 'AS payload' in sql:return [{'window_start':1554076800,'score':2.5,'threshold':2.0,'prediction':1,'label_observed':0,'payload':raw}]
        if 'serving.documents' in sql:return []
        raise AssertionError(sql)
    class Response:
        def __init__(self,rows):self.rows=rows
        def fetchall(self):return self.rows
    class Conn:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,params):return Response(answer(sql,params))
    st.cache_data.clear()
    with patch('psycopg.connect',return_value=Conn()):
        app=AppTest.from_file(str(ROOT/'infra/serving/app/dashboard.py')).run(timeout=30)
        assert not app.exception, [x.message for x in app.exception]
        assert len(app.tabs)==5
        assert app.metric[0].value=='1'
        app.checkbox[0].uncheck().run(timeout=30)
        assert not app.exception
