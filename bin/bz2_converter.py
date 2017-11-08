#!/usr/bin/python
import bz2
import gzip
import os
import sys
import re

output_file=""
input1=""
input2=""

if len((sys.argv[1:]))==3:
    input1, input2, output_file = sys.argv[1:]
else:
    input1, output_file = sys.argv[1:]
    
    

#'example.txt.bz2'

output_fh = bz2.BZ2File(output_file, 'wb')
#try:
#    output.write('Contents of the example file go here.\n')
#finally:
#    output.close()

#os.system('file example.txt.bz2')

fh1=""
fh2=""

if re.search('.gz$',input1):
    fh1 = gzip.open(input1, 'rt')
    fh2 = gzip.open(input2, 'rt')
    
else:
    fh1 = open(input1,"r")
    fh2 = open(input2,"r")


while True:
    
    h1 = fh1.readline().lstrip().rstrip()
    s1 = fh1.readline().lstrip().rstrip()
    h2 = fh1.readline().lstrip().rstrip()
    q1 = fh1.readline().lstrip().rstrip()
    
    h21 = fh2.readline().lstrip().rstrip()
    s2 = fh2.readline().lstrip().rstrip()
    h22 = fh2.readline().lstrip().rstrip()
    q2 = fh2.readline().lstrip().rstrip()
    
    if h1==None or h1=="":
        break
    
    
    #print h1, h21, h1[:-2]
    
    seq_id = h1[:-2]
    
    to_print = "\t".join(map(str, [seq_id, s1, q1, s2, q2] ))
    output_fh.write(str(to_print)+"\n")
    
output_fh.close()
 
