#!/usr/bin/env python3

"""Converter from the Amazon Review Data (2018) dataset to Monpoly's log format

The dataset is described at https://nijianmo.github.io/amazon/index.html.
Only a subset of attributes is exported (see amazon.sig). The 'brand' attribute
is normalized to lowercase.

Warning: The tool reads the entire input into memory, unless the --limit option
is used. It is strongly advised to use the latter for large inputs.
"""

import argparse
import gzip
import json
import random
import sys
from itertools import groupby
from operator import itemgetter

def format_bool(b):
    return '1' if b else '0'

def format_float(f):
    return str(f)

def format_string(s):
    # Monpoly does not support escape sequences
    return '"' + s.replace('"', "'") + '"'

def format_strnum(s):
    # Some numbers are represented as strings with thousands separators
    return s.replace(',', '')

def format_product(p):
    tup = '(' \
        + format_string(p['asin']) + ',' \
        + format_string(p['brand'].lower()) + ',' \
        + format_string(p.get('title', '')) + ')'
    return (0, 'p', tup)

def format_review(r):
    tup = '(' \
        + format_string(r['reviewerID']) + ',' \
        + format_string(r.get('reviewerName', '')) + ',' \
        + format_string(r['asin']) + ',' \
        + format_bool(r['verified']) + ',' \
        + format_float(r['overall']) + ',' \
        + format_strnum(r.get('vote', '0')) + ',' \
        + format_string(r.get('summary', '')) + ',' \
        + format_string(r.get('reviewText', '')) + ')'
    return (r['unixReviewTime'], 'r', tup)

class Stop(Exception):
    pass

class LogCollector:
    def __init__(self, limit=None, fraction=None):
        self._limit = limit
        self._fraction = fraction
        self._d = {}
        self._n_in = 0
        self._n_out = 0

    def add(self, event):
        ts, rel, tup = event
        key = (ts, rel)
        if key in self._d:
            self._d[key].append(tup)
        else:
            self._d[key] = [tup]
        self._n_in += 1
        if self._limit and self._n_in >= self._limit:
            raise Stop()

    def num_records_read(self):
        return self._n_in

    def num_records_written(self):
        return self._n_out

    def write_sample(self, keys, out):
        for ts, rel in keys:
            out.write(rel + ' ')
            for tup in self._d[(ts, rel)]:
                if not self._fraction or random.random() < self._fraction:
                    out.write(tup)
                    out.write('\n')
                    self._n_out += 1

    def write_log(self, out):
        for ts, keys in groupby(sorted(self._d), itemgetter(0)):
            out.write('@' + str(ts) + ' ')
            self.write_sample(keys, out)


def read_json_lines(path):
    try:
        with gzip.open(path, 'r') as f:
            for line in f:
                yield json.loads(line)
    except gzip.BadGzipFile:
        with open(path, 'r') as f:
            for line in f:
                yield json.loads(line)

def process_file(coll, path, formatter):
    for p in read_json_lines(path):
        coll.add(formatter(p))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert Amazon review data to Monpoly log.')
    parser.add_argument('--meta', help='file with product meta data')
    parser.add_argument('--reviews', help='file with reviews')
    parser.add_argument('--limit', type=int, help='maximum number of total tuples in output')
    parser.add_argument('--fraction', type=float, help='fraction of tuples to be sampled randomly')
    parser.add_argument('--seed', type=int, help='seed for random generator')
    args = parser.parse_args()

    if args.seed:
        random.seed(args.seed)
    if args.meta or args.reviews:
        coll = LogCollector(args.limit, args.fraction)
        try:
            if args.meta:
                process_file(coll, args.meta, format_product)
            if args.reviews:
                process_file(coll, args.reviews, format_review)
        except Stop:
            pass
        coll.write_log(sys.stdout)
        sys.stderr.write('%d records written\n' % coll.num_records_written())
    else:
        parser.print_help()
