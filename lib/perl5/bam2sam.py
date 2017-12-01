# this script by ram vinay pandey converts all bam files contained in one folder in sam files and then merges again the cluster output


#!/usr/bin/python
import os
import sys
import re

#example python /Volumes/Temp/bottleneck/03_tools/scripts/bam2sam.py /Volumes/Temp/bottleneck/01_data/mapping/37a/37a/fastq_paired_end_mapping /Volumes/Temp/bottleneck/01_data/mapping/37a/bam2sam /Applications/picard-tools-1.56/picard-tools-1.56/SortSam.jar /Applications/picard-tools-1.56/picard-tools-1.56/MergeSamFiles.jar /Volumes/Temp/bottleneck/01_data/mapping/37a/new_sam

input_dir,output_dir,sortsam_jar, mergesam_jar, merge_file = sys.argv[1:]
    
output_dir.rstrip("/")

merge_limit = 300


def bam2sam(input_dir,output_dir,sortsam_jar):
    
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    
    file_list = os.listdir(input_dir)
    
    for f in file_list:
        if re.search(r'.bam$',f,re.I):
            fileName, fileExtension = os.path.splitext(f)
            infile = str(input_dir)+"/"+str(f)
            outfile = str(output_dir)+"/"+str(fileName)+".sam"
            
            command="java -Xmx10g -Dsnappy.disable=true -jar "+str(sortsam_jar)+" I="+str(infile)+" O="+str(outfile)+" SO=coordinate VALIDATION_STRINGENCY=SILENT"
            print command
            os.system(command)

def mergesam(input_dir,merge_file,mergesam_jar,merge_limit):
    file_list = os.listdir(input_dir)
    #sys.exit()
    #merge_limit
    file_list1 = []
    for f in file_list:
        if re.search(r'.sam$',f,re.I):
            file = str(input_dir)+"/"+str(f)
            file_list1.append(file)
    
    index=1
    merged_file_list = []
    while True:
        #print str(index),": ",str(len(file_list1))
        new_list = file_list1[0:int(merge_limit)]
        input_str = " I=".join(new_list)
        merge_file1 = str(merge_file)+"_"+str(index)+".sam"
        merge_command = "java -Xmx10g -Dsnappy.disable=true -jar "+str(mergesam_jar)+" I="+str(input_str)+" O="+str(merge_file1)+" SO=coordinate VALIDATION_STRINGENCY=SILENT"
        os.system(merge_command)
        #print merge_command
        merged_file_list.append(merge_file1)
        new_list=[]
        
        del(file_list1[0:int(merge_limit)])
        index = index+1
        
        if len(file_list1)< int(merge_limit):
            break
    
    index=index+1
    new_list = file_list1
    #print str(index),": ",str(len(file_list1))
    input_str = " I=".join(new_list)
    merge_file1 = str(merge_file)+"_"+str(index)+".sam"
    merge_command = "java -Xmx10g -Dsnappy.disable=true -jar "+str(mergesam_jar)+" I="+str(input_str)+" O="+str(merge_file1)+" SO=coordinate VALIDATION_STRINGENCY=SILENT"
    os.system(merge_command)
    #print merge_command
    merged_file_list.append(merge_file1)
        

    input_str = " I=".join(merged_file_list)
    merge_file1 = str(merge_file)+".sam"
    
    merge_command = "java -Xmx10g -Dsnappy.disable=true -jar "+str(mergesam_jar)+" I="+str(input_str)+" O="+str(merge_file1)+" SO=coordinate VALIDATION_STRINGENCY=SILENT"
    
    #print merge_command
    os.system(merge_command)
    
    
    
#bam2sam(input_dir,output_dir,sortsam_jar)


mergesam(output_dir,merge_file,mergesam_jar,merge_limit)