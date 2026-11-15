import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'jobs'))
from transfer_gold import transfer,validate_id,discover

SID='2f14f1cbb8e6c322fa283c26'
DEST='/it5427/snapshots/'+SID


class FakeHdfs:
 def __init__(self):self.dirs={'/'};self.files={};self.uploads=0;self.corrupt=False
 def exists(self,p):return p in self.dirs or p in self.files
 def read_json(self,p):return json.loads(self.files[p])
 def hash(self,p):return hashlib.sha256(self.files[p]).hexdigest()
 def run(self,*args):
  cmd,*vals=map(str,args)
  if cmd=='-ls':return
  if cmd=='-mkdir':
   parents=vals[0]=='-p'
   for v in vals[1:] if parents else vals:
    if self.exists(v) and not parents:raise RuntimeError('Directory exists')
    if not parents and str(Path(v).parent) not in self.dirs:raise RuntimeError('Missing parent')
    self.dirs.add(v)
    if parents:self.dirs.update(str(x) for x in Path(v).parents)
  elif cmd=='-put':
   src,dest=vals
   if self.exists(dest):raise RuntimeError('Refuse overwrite')
   if str(Path(dest).parent) not in self.dirs:raise RuntimeError('Missing target parent')
   self.files[dest]=Path(src).read_bytes()+(b'CORRUPT' if self.corrupt else b'');self.uploads+=1
  elif cmd=='-mv':
   src,dest=vals
   if self.exists(dest):raise RuntimeError('Destination exists')
   moving={x for x in self.dirs if x==src or x.startswith(src+'/')}
   self.dirs-=moving;self.dirs.update(dest+x[len(src):] for x in moving)
   for old in list(self.files):
    if old.startswith(src+'/'):self.files[dest+old[len(src):]]=self.files.pop(old)
  elif cmd=='-rmdir':
   path=vals[0]
   if any(x.startswith(path+'/') for x in self.files):raise RuntimeError('Directory not empty')
   self.dirs.remove(path)
  else:raise AssertionError('Unknown test command '+cmd)


class TransferTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
  (self.root/'gold/table').mkdir(parents=True);f=self.root/'gold/table/part.parquet';f.write_bytes(b'synthetic bytes')
  self.manifest={'snapshot_id':SID,'files':[{'path':'gold/table/part.parquet','sha256':hashlib.sha256(f.read_bytes()).hexdigest()}]}
  self.fs=FakeHdfs()
 def tearDown(self):self.temp.cleanup()
 def test_verified_copy_and_idempotent_retry(self):
  first=transfer(self.fs,self.root,self.manifest);n=self.fs.uploads
  self.assertEqual(first['status'],'imported_verified')
  second=transfer(self.fs,self.root,self.manifest)
  self.assertEqual(second['status'],'already_present_verified');self.assertEqual(n,self.fs.uploads)
 def test_conflicting_manifest_never_overwrites(self):
  transfer(self.fs,self.root,self.manifest);before=dict(self.fs.files)
  changed={**self.manifest,'new_policy':True}
  with self.assertRaises(ValueError):transfer(self.fs,self.root,changed)
  self.assertEqual(before,self.fs.files)
 def test_corrupt_upload_never_commits(self):
  self.fs.corrupt=True
  with self.assertRaises(ValueError):transfer(self.fs,self.root,self.manifest)
  self.assertFalse(self.fs.exists(DEST));self.assertFalse(self.fs.exists('/it5427/_locks/'+SID))
  self.assertTrue(any(x.startswith('/it5427/_staging/') for x in self.fs.files))
 def test_corrupt_existing_copy_detected(self):
  transfer(self.fs,self.root,self.manifest)
  self.fs.files[DEST+'/gold/table/part.parquet']=b'changed remotely'
  with self.assertRaises(ValueError):transfer(self.fs,self.root,self.manifest)
 def test_changed_local_file_rejected(self):
  (self.root/'gold/table/part.parquet').write_bytes(b'changed')
  with self.assertRaises(ValueError):transfer(self.fs,self.root,self.manifest)
  self.assertFalse(self.fs.exists(DEST))
 def test_existing_lock_is_not_removed(self):
  self.fs.run('-mkdir','-p','/it5427/_locks/'+SID)
  with self.assertRaises(RuntimeError):transfer(self.fs,self.root,self.manifest)
  self.assertTrue(self.fs.exists('/it5427/_locks/'+SID))
 def test_invalid_ids_rejected(self):
  for s in ['../../etc/passwd','abc','A'*24,'a'*25]:
   with self.assertRaises(ValueError):validate_id(s)
 def test_uncommitted_snapshot_rejected(self):
  snap=self.root/'lake/snapshots'/SID;snap.mkdir(parents=True)
  (snap/'snapshot.json').write_text(json.dumps({'snapshot_id':SID,'status':'staging'}))
  with self.assertRaises(ValueError):discover(self.root,SID)

if __name__=='__main__':unittest.main()
