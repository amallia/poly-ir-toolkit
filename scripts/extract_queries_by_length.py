#!/usr/bin/env python3
'''
Author: Roman Khmelichek

Takes TREC efficiency track query log filename as input and the length of the queries to extract.
'''
import sys

if len(sys.argv) < 3:
    sys.exit("Must specify an input file and the length of the queries to extract.")

try:
    query_log_file_stream = open(sys.argv[1], 'r')
except IOError:
    sys.exit("Couldn't open file '" + sys.argv[1] + "' for reading.")

for line in query_log_file_stream:
    if len(line.split()) == int(sys.argv[2]):
        print(line.rstrip())
