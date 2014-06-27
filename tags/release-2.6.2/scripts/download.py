#!/bin/python

import os
import urllib
import sys

# this script is a tool to download files by it's url
# download list is specified in a file, one file url per line
# an index file will be generated specified by 'data_index' 

if (len(sys.argv) != 2):
	print 'download.py data_file_path'
	sys.exit()
data_file = sys.argv[1]

prefix_len=len('http://img.taobaocdn.com')
data_dir = './data_dir'
data_index = './data_index'
delim = ':'

f = open(data_file, 'r')
idxf = open(data_index, 'a+')

while True:
	url = f.readline()
	if not url:
		break
	url = url[:len(url)-1]  # remove trailing 'r'
	print 'downloading ', url
	cmd = 'wget -x ' + url
	os.system(cmd) # download file
	
	# write index
	idx = url[prefix_len:] + ':' + os.path.abspath('./' + url[7:]) + '\n'
	idxf.write(idx)

f.close()
idxf.close()

print 'download successfully'

