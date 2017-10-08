#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
def process_uinfo():
    for line in sys.stdin:
        line = line.strip()
        line_arr = line.split('\t')
        if len(line_arr) != 4:
        	continue
        (uid, urlid, ts, label) = line_arr
        uid = uid[2:]
        urlid = urlid[2:]
        print "\t".join([uid, urlid, ts, label])

if __name__=='__main__':
    process_uinfo()

