#!/usr/bin/env python

import logtool, os, stat, yaml
from addict import Dict
from path import Path

DEFAULT_USERCONFIG = Path ("~/.s3clumperrc").expanduser ()

class UserCfg (object):

  keys = [
    "client_cfg.ssh_keypath",
    "jira.username",
    "jira.password",
#    "code.datavisor_path",
    "code.infra_code_path",
#    "code.ps_path",
#    "cloud.ali.sin.access",
#    "cloud.ali.sin.secret",
#    "cloud.ali.sin.ssh_keypath",
#    "cloud.ali.pek.access",
#    "cloud.ali.pek.secret",
#    "cloud.ali.pek.ssh_keypath",
    "cloud.aws.or.access",
    "cloud.aws.or.secret",
    "cloud.aws.or.ssh_keypath",
#    "cloud.qc.pek.access",
#    "cloud.qc.pek.secret",
#    "cloud.qc.pek.ssh_keypath",
  ]

  @logtool.log_call
  def __init__ (self, write = False):
    self.write = write
    self._state = None
    self.load ()

  @logtool.log_call
  def get_keystr (self, key):
    return reduce (lambda c, k: c[k], key.split ("."), self._state)

  @logtool.log_call
  def set_keystr (self, ks, v):
    l = ks.split (".")
    l_key = l[-1]
    r_key = l[:-1]
    lhs = reduce (lambda c, k: c[k], r_key, self._state)
    lhs[l_key] = v

  @logtool.log_call
  def load (self):
    # pylint bug!
    # pylint: disable = no-value-for-parameter
    if not DEFAULT_USERCONFIG.isfile ():
      DEFAULT_USERCONFIG.touch ()
    DEFAULT_USERCONFIG.chmod (stat.S_IWRITE | stat.S_IREAD)
    with file (DEFAULT_USERCONFIG) as f:
      d = yaml.safe_load (f)
    self._state = Dict (d)
    for k in self.keys:
      var = ("DVT." + k.upper ()).replace (".", "_")
      if os.environ.get (var):
        self.set_keystr (k, os.environ[var])

  @logtool.log_call
  def dump (self):
    return yaml.dump (self._state.to_dict (), width = 70, indent = 2,
                      default_flow_style = False)

  @logtool.log_call
  def save (self):
    with open (DEFAULT_USERCONFIG, "w") as f:
      f.write (self.dump ())

  @property
  @logtool.log_call
  def data (self):
    return self._state

  @logtool.log_call
  def __enter__ (self):
    return self

  @logtool.log_call
  def __exit__ (self, *args):
    if self.write:
      self.save ()
