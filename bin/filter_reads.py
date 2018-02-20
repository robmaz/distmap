#!/usr/bin/env python
import os
import sys
import re

fastq1,fastq2,read_file,out_fastq1,out_fastq2 = sys.argv[1:]





def read_read_id(read_file):
    fh = open(read_file,"r")
    read_dict = {}
    for l in fh:
        if l:
            if not re.search('^@',l):
                l=l.strip()
                col = l.split("\t")

                read_id = col[0].lstrip().rstrip()

                print read_id
                read_dict[read_id] = read_id

    fh.close()

    return read_dict

read_dict = read_read_id(read_file)

print len(read_dict)

fh1 = open(fastq1,"r")
fh2 = open(fastq2,"r")

ofh1 = open(out_fastq1,"w")
ofh2 = open(out_fastq2,"w")

while True:
    h1 = fh1.readline().lstrip().rstrip()
    s1 = fh1.readline().lstrip().rstrip()
    h12 = fh1.readline().lstrip().rstrip()
    q1 = fh1.readline().lstrip().rstrip()

    h2 = fh2.readline().lstrip().rstrip()
    s2 = fh2.readline().lstrip().rstrip()
    h22 = fh2.readline().lstrip().rstrip()
    q2 = fh2.readline().lstrip().rstrip()

    if h1=="" or h2=="":
        break
    #FCD20ENACXX:3:2107:9999:7489#GGCTACAT
    #print h1[1:-2]
    if h1[1:-2] == h2[1:-2]:
        if h1[1:-2] in read_dict:
            print >> ofh1, h1
            print >> ofh1, s1
            print >> ofh1, h12
            print >> ofh1, q1

            print >> ofh2, h2
            print >> ofh2, s2
            print >> ofh2, h22
            print >> ofh2, q2


            del(read_dict[h1[1:-2]])


fh1.close()
fh2.close()
ofh1.close()
ofh2.close()
