#! /usr/bin/env python

from __future__ import absolute_import
import clip, logtool, logging, sys
from addict import Dict
from encodings.utf_8 import StreamWriter
from functools import partial
from path import Path
from .cmdio import CmdIO
from .cmd import Action

from ._version import get_versions
__version__ = get_versions ()['version']
del get_versions

logging.basicConfig (level = logging.WARN)
LOG = logging.getLogger (__name__)
TIME_T, TIME_STR = logtool.now ()

# Python 2 will try to coerce its output stream to match the terminal it is
# printing to. If we pipe the output, it will attempt to do plain ASCII
# encoding, which breaks on unicode characters. The following will change
# the default encoding for pipes from ascii to utf-8.
#   See : https://wiki.python.org/moin/PrintFails
sys.stdout = StreamWriter(sys.stdout)
sys.stderr = StreamWriter(sys.stderr)

APP = clip.App (name = "s3clumper")

CONFIG = Dict ({
  "check": False,
  "force": False,
  "nocolour": False,
  "delete": False,
  "quiet": False,
  "time_str": TIME_STR,
  "time_t": TIME_T,
  "verbose": False,
})
IO = CmdIO (conf = CONFIG)

@logtool.log_call
def option_setopt (option, value):
  CONFIG[option] = value

@logtool.log_call
def option_version (opt): # pylint: disable = W0613
  print __version__
  clip.exit ()

@logtool.log_call
def option_logging (flag):
  logging.root.setLevel (logging.DEBUG)
  CONFIG.debug = flag

@APP.main (name = Path (sys.argv[0]).basename (),
           description = "Aggregate @3 prefixes into tarballs in S3",
           tree_view = "-H")
@clip.flag ("-H", "--HELP",
            help = "Help for all sub-commands")
@clip.flag ("-c", "--check", name = "check",
            help = "Don't check for target (may over-write)",
            callback = partial (option_setopt, "check"))
@clip.flag ("-C", "--nocolour", name = "nocolour",
            help = "Suppress colours in reports",
            callback = partial (option_setopt, "nocolour"))
@clip.flag ("-D", "--debug", name = "debug", help = "Enable debug logging",
            callback = option_logging)
@clip.flag ("-d", "--delete", name = "delete",
            help = "Don't delete source files",
            callback = partial (option_setopt, "delete"))
@clip.flag ("-q", "--quiet", name = "quiet",
            help = "Be quiet, be vewy vewy quiet",
            callback = partial (option_setopt, "verbose"))
@clip.flag ("-v", "--verbose", name = "verbose",
            help = "Verbose output",
            callback = option_version)
@clip.flag ("-V", "--version", name = "verbose",
            help = "Report installed version",
            callback = option_version)
@clip.flag ("-z", "--compress", name = "compress",
            help = "Don't compress the target",
            callback = partial (option_setopt, "compress"))
@clip.arg ("from", help = "S3 URL prefix to clump")
@clip.arg ("to", help = "S3 URL for target clump")
@logtool.log_call
def app_main (**kwargs):
  if not CONFIG.conf.debug:
    logging.basicConfig (level = logging.ERROR)
  if not sys.stdout.isatty ():
    option_setopt ("nocolour", True)
  CONFIG.url_from = kwargs["from"]
  CONFIG.url_to = kwargs["to"]
  Action (CONFIG).run ()

@logtool.log_call
def main ():
  try:
    APP.run ()
  except KeyboardInterrupt:
    pass
  except clip.ClipExit as e:
    sys.exit (e.status)
  except Exception as e:
    logtool.log_fault (e)
    sys.exit (1)

if __name__ == "__main__":
  main ()
