#!/usr/bin/env python3
'''
Author: Roman Khmelichek

Converts TREC ad hoc and named page retrieval topics to queries usable by
PolyIRTK when used with the --query-mode=batch-all and --result-format=trec options.
'''
import re
import sys

if len(sys.argv) < 2:
    sys.exit("Must specify an input file.")

try:
    topics_file_stream = open(sys.argv[1], 'r')
except IOError:
    sys.exit("Couldn't open file '" + sys.argv[1] + "' for reading.")

topics = topics_file_stream.read()
num_title_pattern = re.compile('<num> Number: (?:NP)?(.*)\\n*<title> (.*)')
iterator = num_title_pattern.finditer(topics)
for match in iterator:
    query = match.group(2).lower()
    query_pattern = re.compile('\W+')
    query = query_pattern.sub(' ', query)
    print(match.group(1) + ":" + query)
