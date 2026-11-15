import json
import sys
import urllib.request
role=sys.argv[1]
port,name=(9870,'NameNode') if role=='namenode' else (9864,'DataNode')
try:
 with urllib.request.urlopen(f'http://127.0.0.1:{port}/jmx?qry=Hadoop:service={name},name={name}Info',timeout=3) as r:
  bean=json.load(r)['beans'][0]
 if role=='namenode':
  assert not bean['Safemode']
  assert len(json.loads(bean['LiveNodes']))>=1
except Exception as e:
 print(type(e).__name__+': '+str(e));sys.exit(1)
