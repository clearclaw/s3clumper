#!/usr//env python

import clip, json, logging, logtool, termcolor, yaml
from addict import Dict

LOG = logging.getLogger (__name__)
COLOUR_ERROR = "red"
COLOUR_DEBUG = "blue"
COLOUR_INFO = "white"
COLOUR_INFO_BAD = "magenta"
COLOUR_WARN = "yellow"
COLOUR_DATA = "green"
COLOUR_REPORT = "green"
COLOUR_FIELDNAME = "cyan"
COLOUR_VALUE = "green"

class CmdIO (object):

  @logtool.log_call
  def __init__ (self, conf = None):
    self.conf = conf if conf else Dict ()

  @logtool.log_call (log_args = False, log_rc = False)
  def _safe_echo (self, msg, nl):
    if isinstance (msg, str):
      msg = unicode (msg, encoding = "utf8")
    return clip.echo (msg, nl = nl)

  @logtool.log_call (log_args = False, log_rc = False)
  def colourise (self, msg, colour):
    return (termcolor.colored (msg, colour) if not self.conf.nocolour
            else msg)

  @logtool.log_call (log_args = False, log_rc = False)
  def debug (self, msg, nl = True):
    if not self.conf.quiet and self.conf.verbose:
      self._safe_echo (self.colourise (msg, COLOUR_DEBUG), nl = nl)

  @logtool.log_call (log_args = False, log_rc = False)
  def info (self, msg, err = False, nl = True):
    if not self.conf.quiet:
      self._safe_echo (self.colourise (
        "INFO: " + msg, COLOUR_INFO if not err else COLOUR_INFO_BAD),
                       nl = nl)

  @logtool.log_call (log_args = False, log_rc = False)
  def error (self, msg, nl = True):
    if not self.conf.quiet:
      self._safe_echo (self.colourise ("ERROR: " + msg, COLOUR_ERROR),
                       nl = nl)

  @logtool.log_call (log_args = False, log_rc = False)
  def warn (self, msg, nl = True):
    if not self.conf.quiet:
      self._safe_echo (self.colourise ("WARNING: " + msg, COLOUR_WARN),
                       nl = nl)

  @logtool.log_call (log_args = False, log_rc = False)
  def report (self, msg, err = False, nl = True):
    if not self.conf.quiet:
      self._safe_echo (self.colourise (
          msg, COLOUR_REPORT if not err else COLOUR_ERROR), nl = nl)

  @logtool.log_call (log_args = False, log_rc = False)
  def data (self, msg, err = False, nl = True):
    if not self.conf.quiet:
      if self.conf.json:
        s = json.dumps (msg, indent = 2)
      else:
        s = yaml.dump (msg, width = 70, indent = 2,
                       default_flow_style = False)
      self._safe_echo (self.colourise (
          s, COLOUR_DATA if not err else COLOUR_ERROR), nl = nl)
