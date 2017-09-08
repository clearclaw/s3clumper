#!/usr/bin/env python

from __future__ import absolute_import
import boto3, botocore, clip, logging, logtool, os
import retryp, tarfile, tempfile, threading, urlparse
from collections import namedtuple
from progress.bar import Bar
from . import cmdio

LOG = logging.getLogger (__name__)

class _ProgressPercentage (object):

  # pylint: disable=too-few-public-methods

  @logtool.log_call
  def __init__ (self, filename, quiet):
    self._filename = filename
    self._size = float (os.path.getsize (filename))
    self._lock = threading.Lock ()
    self.quiet = quiet
    self.progress = Bar ("Sending", max = 100) if not quiet else None

  @logtool.log_call
  def __enter__ (self):
    return self

  @logtool.log_call
  def __exit__ (self, *args):
    if not self.quiet:
      self.progress.finish ()

  @logtool.log_call
  def __call__ (self, bytes_amount):
    if not self.quiet:
      with self._lock:
        self.progress.next (n = int (100 * bytes_amount / self._size))

class Action (cmdio.CmdIO):

  @logtool.log_call
  def __init__ (self, args):
    cmdio.CmdIO.__init__ (self, conf = args)
    self.args = args
    self.p_from = self._parse_url ("Source", args.url_from)
    self.p_to = self._parse_url ("Destination", args.url_to)
    self.compress = args.compress
    self.check = args.check
    self.s3 = boto3.resource ("s3")
    self.keys = None

  @logtool.log_call
  def _parse_url (self, typ, url):
    _urlspec = namedtuple ("_UrlSpec", ["protocol", "bucket", "key"])
    p = urlparse.urlparse (url)
    rc = _urlspec (protocol = p.scheme, bucket = p.netloc,
                   key = p.path[1:] if p.path.startswith ("/") else p.path)
    if rc.protocol != "s3":
      self.error ("%s protocol is not s3: %s" % (typ, url))
      clip.exit (err = True)
    return rc

  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _cleanup (self):
    if not self.args.delete:
      for key in (Bar ("Deleting").iter (self.keys)
                  if not self.args.quiet else self.keys):
        key.delete ()

  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _send (self, out_f):
    client = boto3.client('s3')
    with _ProgressPercentage (out_f.name, self.args.quiet) as cb:
      client.upload_file (
        out_f.name, self.p_to.bucket, self.p_to.key, Callback = cb)

  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _get_file (self, client, bucket, key, fhandle):
    client.download_fileobj (bucket, key, fhandle)
    fhandle.flush ()

  @logtool.log_call
  def _entar (self, out_f):
    mode = "w" if self.compress else "w:gz"
    client = boto3.client('s3')
    with tarfile.open (out_f.name, mode = mode) as tar:
      for key in (Bar ("Fetching").iter (self.keys)
                  if not self.args.quiet else self.keys):
        with tempfile.NamedTemporaryFile (prefix = "s3clumper_tf__") as f:
          self._get_file (client, self.p_from.bucket, key.key, f)
          tar.add (f.name, arcname = key.key)
    out_f.flush ()
    out_f.seek (0)

  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _list_from (self):
    bucket = self.s3.Bucket (self.p_from.bucket)
    self.keys = [k for k in bucket.objects.filter (
      Prefix = self.p_from.key)]

  @retryp.retryp (expose_last_exc = True, log_faults = True)
  @logtool.log_call
  def _check (self):
    if self.check:
      return False
    try:
      boto3.client ("s3").head_object (
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
    with tempfile.NamedTemporaryFile (prefix = "s3clumper_tar__") as out_f:
      self._entar (out_f)
      self._send (out_f)
      self._cleanup ()
      if not self.args.quiet:
        print ""
