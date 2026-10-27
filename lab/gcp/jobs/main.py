"""Dataproc Serverless entry point. The shopco package arrives via --py-files; argv is forwarded unchanged."""

import sys

from shopco.cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
