import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

class FormatGuardTests(unittest.TestCase):
 def test_fresh_restart_and_partial_directory(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);name=root/'name';name.mkdir();binpath=root/'bin';binpath.mkdir()
   calls=root/'calls.jsonl'
   mock=binpath/'hdfs'
   mock.write_text('''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
with open(os.environ['TEST_CALLS'],'a') as f:f.write(json.dumps(sys.argv[1:])+'\\n')
if '-format' in sys.argv:
 p=Path(os.environ['TEST_NAME'])/'current';p.mkdir();(p/'VERSION').write_text('synthetic')
''')
   mock.chmod(0o755)
   original=Path(__file__).resolve().parents[1]/'entrypoint.sh'
   script=root/'entrypoint.sh';script.write_text(original.read_text().replace('/data/name',str(name)))
   env={**os.environ,'PATH':str(binpath)+os.pathsep+os.environ['PATH'],
        'HADOOP_LOG_DIR':str(root/'logs'),'HADOOP_PID_DIR':str(root/'pids'),
        'TEST_CALLS':str(calls),'TEST_NAME':str(name)}
   for _ in range(2):subprocess.run(['bash',str(script),'namenode'],env=env,check=True,capture_output=True)
   commands=[json.loads(x) for x in calls.read_text().splitlines()]
   self.assertEqual(sum('-format' in a for a in commands),1)
   # Simulate incomplete storage: current directory remains, VERSION is absent.
   (name/'current/VERSION').unlink()
   failed=subprocess.run(['bash',str(script),'namenode'],env=env,capture_output=True,text=True)
   self.assertNotEqual(failed.returncode,0)
   self.assertIn('Refusing to format',failed.stderr)
   self.assertEqual(len(calls.read_text().splitlines()),len(commands))

if __name__=='__main__':unittest.main()
