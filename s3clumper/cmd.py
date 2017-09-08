#!/usr/bin/env python

from __future__ import absolute_import
import boto3, botocore, clip, contextmanagerlib, logging, logtool
import shutil, retryp, tarfile, tempfile, urlparse
from collections import namedtuple
from path import Path
from progress.bar import Bar
from . import cmdio

LOG = logging.getLogger (__name__)

_UrlSpec = namedtuple ("_UrlSpec", ["protocol", "bucket", "key"])

@contextlib.contextmanager
def _tempdir (prefix):
  d = tempfile.mkdtemp (prefix = prefix)
  yield d
  shutil.rmtree (d)

class S3ClumperException (Exception):
  pass

class Action (cmdio.CmdIO):

  @logtool.log_call
  def __init__ (self, args):
    self.args = args
    self.p_from = self._parse_url ("Source", args.url_from)
    self.p_to = self._parse_url ("Destination", args.url_to)
    self.compress = args.compress
    self.check = args.check
    self.s3 = boto3.resource ("s3")
    self.keys = None
    super (self.__class__, self).__init__ ()

  @logtool.log_call
  def _parse_url (self, typ, url):
    _urlspec = namedtuple ("_UrlSpec", ["protocol", "bucket", "key"])
    p = urlparse.urlparse (url)
    rc = _urlspec (protocol = p.scheme, bucket = p.netloc,
                   key = p.path[1:] if p.path.startswith ("/") else p.path)
    if rc.protocol != "s3":
      raise S3ClumperException ("%s protocol is not s3: %s" % (typ, url))
    return rc

#  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _cleanup (self):
    for key in (Bar ("Deleting").iter (self.keys)
                if not self.args.quiet else self.keys):
      key.delete ()

#  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _send (self, out_f):
    self.s3.Object (self.p_to.bucket, self.p_to.key).put (
      Body = out_f.read ())

#  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _entar (self, out_f):
    mode = "w:gz" if self.compress else "w"
    with tarfile.open (out_f.name, mode = mode) as tar:
      for key in (Bar ("Fetching").iter (self.keys)
                  if not self.args.quiet else self.keys):
        with tempfile.NamedTemporaryFile ("s3clumper__") as f:
          s3.download_fileobj (self.p_from.bucket, key.key, f.name)
          tar.add (f.name, key.key)
    out_f.flush ()
    out_f.seek (0)

#  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _list_from (self):
    bucket = self.s3.Bucket (self.p_from.bucket)
    self.keys = [k for k in bucket.objects.filter (
      Prefix = self.p_from.key)]

#  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _check (self):
    if self.check:
      return False
    try:
      rc = boto3.client ("s3").head_object (
        Bucket = self.p_to.bucket,
        Key = self.p_to.key)
      self.error ("Target exists: %s" % self.args.url_to)
      return True
    except botocore.exceptions.ClientError as e:
      if int (e.response['Error']['Code']) == 404:
        return False
      raise

  @logtool.log_call
  def run (self):
    if self._check ():
      clip.exit (err = True)
    self._list_from ()
      with tempfile.NamedTemporaryFile (prefix = "s3clumper__") as out_f:
        self._entar (out_f)
        self._send (out_f)
        self._cleanup ()
