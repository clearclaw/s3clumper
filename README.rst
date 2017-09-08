S3Clumper
=========

Makes a tarball (optionally compressed with gzip) of the contents of
an S3 prefix (preserving interior paths and filenames), posts the
tarball to a stated S3 URL and optionally deletes the source files.

::

  s3clumper: Aggregate @3 prefixes into tarballs in S3

  Usage: s3clumper {{arguments}} {{options}}

  Arguments:
    from [text]  S3 URL prefix to clump
    to [text]    S3 URL for target clump

  Options:
    -h, --help      Show this help message and exit
    -H, --HELP      Help for all sub-commands
    - c, --check     Don't check for target (may over-write)
    -C, --nocolour  Suppress colours in reports
    -D, --debug     Enable debug logging
    -d, --delete    Don't delete source files
    -q, --quiet     Be quiet, be vewy vewy quiet
    -v, --verbose   Verbose output
    -V, --version   Report installed version
   - z, --compress  Don't compress the target

Example:

::

  $ s3clumper s3://bucket1/data/ s3://bucket2/archive/logs-20170302.tar.gz
  Fetching |################################| 55/55
  Sending |################################| 100/100
  Deleting |################################| 55/55
