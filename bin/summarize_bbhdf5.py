#!/usr/bin/env python
"""
This is a proof-of-concept code to summarize the contents of an HDF5 file
generated by cache_collectdes_supplemental.py.  It should be converted to use
tokio.connectors.hdf5 once the TOKIOfile HDF5 schema is nailed down and
implemented correctly.
"""

import json
import math
import argparse
import numpy
import h5py

BYTES_TO_TIBS = 1.0 / 2.0 ** 40.0

def missing_data(matrix):
    """
    Because we initialize datasets with -0.0, we can scan the sign bit of every
    element of an array to determine how many data were never populated.  This
    converts negative zeros to ones and all other data into zeros then count up
    the number of missing elements in the array.
    """
    converter = numpy.vectorize(lambda x: 1 if math.copysign(1, x) < 0.0 else 0)
    return converter(matrix).sum()

def summarize_bbhdf5(hdf5_filename):
    hdf5_file = h5py.File(hdf5_filename, 'r')
    timestep = hdf5_file['/bytes/timestamps'][1] - hdf5_file['/bytes/timestamps'][0]
    read_tibs = hdf5_file['/bytes/readrates'][:,:].sum() * BYTES_TO_TIBS * timestep
    write_tibs = hdf5_file['/bytes/writerates'][:,:].sum() * BYTES_TO_TIBS * timestep

    # readrates and writerates come via the same collectd message, so if one is
    # missing, both are missing
    values = hdf5_file['/bytes/readrates'][:,:]
    num_missing = missing_data(values)
    total = values.shape[0] * values.shape[1]

    return {
        'read_tibs': read_tibs,
        'write_tibs': write_tibs,
        'missing_pts': num_missing,
        'total_pts': total,
        'missing_pct': (100.0 * float(num_missing) / total),
    }

def _summarize_bbhdf5():
    """
    Summarize the contents of an HDF5 file generated by cache_collectdes_supplemental.py
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=str, help="HDF5 file to summarize")
    parser.add_argument('-j', '--json', action='store_true', help='output as json')
    args = parser.parse_args()

    results = summarize_bbhdf5(args.file)

    if args.json:
        print json.dumps(results, indent=4, sort_keys=True)
    else:
        print "Data Read (TiB):      %.1f" % results['read_tibs']
        print "Data Written (TiB):   %.1f" % results['write_tibs']
        print "Missing data points:  %d" % results['missing_pts']
        print "Expected data points: %d" % results['total_pts']
        print "Percent data missing: %.1f%%" % results['missing_pct']

if __name__ == '__main__':
    _summarize_bbhdf5()