#!/usr/bin/env python3
'''
Author: Roman Khmelichek

Takes the pre-processed TREC efficiency track query log filename as input and
outputs the average query length in words.
'''
import sys

if len(sys.argv) < 2:
    sys.exit("Must specify an input file.")

try:
    query_log_file_stream = open(sys.argv[1], 'r')
except IOError:
    sys.exit("Couldn't open file '" + sys.argv[1] + "' for reading.")

avg = 0.0
count = 0
for line in query_log_file_stream:
    avg += len(line.split())
    count += 1

avg /= count

print('Average query length:', avg)
