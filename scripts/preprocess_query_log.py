#!/usr/bin/env python3
'''
Author: Roman Khmelichek

Takes TREC efficiency track query log filename as input as well as a new
filename as output, which will process the query log to remove non-ASCII
characters as well as replace any non-alphanumeric characters with spaces.
The query number + colon at the start of each line will also be removed.
'''
import re
import sys
import unicodedata

if len(sys.argv) < 3:
    sys.exit("Must specify an input file, followed by an output file.")

'''
The new file written will be identical to the input, except that all non-ASCII
characters, such as those with accents and such, will be replaced by their
corresponding ASCII simplifications.

http://stackoverflow.com/questions/1382998/latin-1-to-ascii
'''

try:
    query_log_file_stream = open(sys.argv[1], 'rb')
except IOError:
    sys.exit("Couldn't open file '" + sys.argv[1] + "' for reading.")

filedata = query_log_file_stream.read()
asciified = unicodedata.normalize('NFKD', filedata.decode("iso-8859-15")).encode('ascii', 'ignore').decode('ascii')

'''
Since the PolyIRTK query processor will simply replace all non-alphanumeric
characters with spaces, we have to do the same here to get the correct query
lengths as will be executed by our system.
'''

try:
    output_stream = open(sys.argv[2], 'w')
except IOError:
    sys.exit("Couldn't open file '" + sys.argv[2] + "' for writing.")

pattern = re.compile('\d+:(.*)\\n')
iterator = pattern.finditer(asciified)
for match in iterator:
    query = match.group(1)
    query_pattern = re.compile('[^a-zA-Z0-9 ]')
    
    # Check that's it matching correctly.
#    query_iterator = query_pattern.finditer(query)
#    for query_match in query_iterator:
#        print(query_match.group(0))
    
    query = query_pattern.sub(' ', query)
    output_stream.write(query)
    output_stream.write('\n')
