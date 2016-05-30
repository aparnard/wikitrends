#!/usr/bin/env python
from operator import itemgetter
import sys
current_key=None
current_count=0
for line in sys.stdin:
	key,value=line.split('\t')
	date,topic=key.split(',')
	word=[date[:-3],topic]
	try:
        count = int(value)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

     if current_key == key:
        current_count += count
    else:
        if current_key:
            # write result to STDOUT
            print '%s\t%s' % ("\t".join(current_key), current_count)
        current_count = count
        current_key = key

# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%s' % ("\t".join(current_key), current_count)