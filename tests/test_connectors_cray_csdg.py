#!/usr/bin/env python
import sys
import json
import argparse

from tokio.common import JSONEncoder
import tokio.connectors.cray_csdg

import tokiotest

def test_smartctl(filenames=None):
    """tokio.connectors.cray_csdg.Smartctl"""
    if filenames is None:
        filenames = [tokiotest.SAMPLE_SMARTCTL_A]

    blah = tokio.connectors.cray_csdg.Smartctl()
    for filename in filenames:
        blah.load_file(filename)
    print(blah.to_json(indent=4, sort_keys=True))

def test_smartctl_parser(filenames=None):
    """tokio.connectors.cray_csdg.SmartctlParser"""
    if filenames is None:
        filenames = [tokiotest.SAMPLE_SMARTCTL]

    blah = []
    parser = tokio.connectors.cray_csdg.SmartctlParser()
    for filename in filenames:
        this_file = {}
        with open(filename, 'r') as smartoutput:
            for line in smartoutput:
                this_file.update(parser.parse_scrub(line))
        blah.append(this_file)
    print(json.dumps(blah, cls=JSONEncoder, indent=4, sort_keys=True))

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs='+', type=str, help="ascii file(s) containing CSDG output")
    parser.add_argument("-s", "--smartctl", action="store_true", help="expect bare smartctl output, not CSDG output")
    args = parser.parse_args(argv)

    if args.smartctl:
        test_smartctl_parser(args.filenames)
    else:
        test_smartctl(args.filenames)

if __name__ == "__main__":
    main()
