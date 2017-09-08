#! /usr/bin/env python

import fnmatch, logtool
from addict import Dict

@logtool.log_call (log_args = False, log_rc = False)
def matches (data, specs):
  for spec in specs.split (","):
    if fnmatch.fnmatch (data, spec.strip ()):
      return data
  return None

@logtool.log_call (log_args = False, log_rc = False)
def filter_list (data, specs):
  rc = []
  for spec in specs.split (","):
    rc.extend ([k for k in data
                if fnmatch.fnmatch (k, spec.strip ())])
  return rc

@logtool.log_call
def filter_dict (data, specs):
  rc = {}
  for spec in specs.split (","):
    rc.update ({k: v for k, v in data.items ()
                if fnmatch.fnmatch (k, spec.strip ())})
  if isinstance (data, Dict):
    return Dict (rc)
  return rc

@logtool.log_call (log_args = False, log_rc = False)
def dict_merge (a, b, path = None):
  if path is None:
    path = []
  for key in b:
    if key in a:
      if isinstance (a[key], dict) and isinstance (b[key], dict):
        dict_merge (a[key], b[key], path + [str(key)])
      elif a[key] == b[key]:
        pass # same leaf value
      else:
        raise Exception ('Conflict at %s'
                         % '.'.join (path + [str(key)]))
    else:
      a[key] = b[key]
  return a
