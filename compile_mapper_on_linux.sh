#!/bin/sh

#sudo -E yum install zlib-devel -y
# logn as root and install CPAN and Archive::Tar.pm perl module for DistMap pipeline
#yum -y install perl-CPAN
#force install Archive::Tar.pm


################# Information ###########################
#
# This script will install BWA, Bowtie2, GSNAP and PICARD. But for Novoalign user need to download and unzip precompiled binary manually.
# This script tries to automaticall download Novoalign tar file but there is some connection security problem and does not allow to download by wget command.
#
# COMMAND to run this script:  sudo sh compile_mapper_on_linux.sh <path_to_a_valid_folder_path>
#
########################################################


exec_dir=$1
mkdir $exec_dir


### How to get BWA?

## Install/Compile BWA
cd $exec_dir && \
wget https://sourceforge.net/projects/bio-bwa/files/bwa-0.7.13.tar.bz2 && \
bzip2 -d bwa-0.7.13.tar.bz2 && \
tar -xvf bwa-0.7.13.tar && \
cd bwa-0.7.13 && \
sudo make 






### How to get BOWTIE2?

#Step1:
cd $exec_dir

#Step2: Download bowtie2 in $exec_dir directory
#https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.2.6/bowtie2-2.2.6-linux-x86_64.zip
wget "https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.2.6/bowtie2-2.2.6-linux-x86_64.zip"

#Step3:
unzip bowtie2-2.2.6-linux-x86_64.zip



### How to get PICARD?

#Step1:
cd $exec_dir

#Step2: Download PICARD in $exec_dir directory
wget "https://github.com/broadinstitute/picard/releases/download/2.7.0/picard.jar"
#https://github.com/broadinstitute/picard/releases/download/2.7.0/picard.jar


### How to get GSNAP?

## Install/Compile GSNAP
cd $exec_dir && \
wget http://research-pub.gene.com/gmap/src/gmap-gsnap-2016-09-14.tar.gz && \
tar -xzvf gmap-gsnap-2016-09-14.tar.gz && \
cd gmap-2016-09-14 && \
sudo ./configure --prefix="$exec_dir/gmap-2016-09-14" && \
sudo make && \
sudo make install && \



#### old gsnap version

cd /Volumes/cluster/DistMap_v2.7.1/Linux_executables && \
wget http://research-pub.gene.com/gmap/src/gmap-gsnap-2015-12-31.tar.gz && \
tar -xzvf gmap-gsnap-2015-12-31.tar.gz && \
cd gmap-2015-12-31 && \
sudo ./configure --prefix="$exec_dir/gmap-2015-12-31" && \
sudo make && \
sudo make install && \





### How to get novoalign?
#Step1:

cd $exec_dir

#Step2: Download Novoalign in $exec_dir directory
wget "http://www.novocraft.com/support/download/download.php?filename=V3.05.01/novocraftV3.05.01.Linux3.0.tar.gz"

#Step3: unzip the downloaded file

tar -xzvf novocraftV3.05.01.Linux3.0.tar.gz




### How to get samtools?
## Install/Compile samtools
#cd $exec_dir && \
#wget https://sourceforge.net/projects/samtools/files/samtools/1.3.1/samtools-1.3.1.tar.bz2 && \
#bzip2 -d samtools-1.3.1.tar.bz2 && \
#tar -xvf samtools-1.3.1.tar && \
#cd samtools-1.3.1 && \
#sudo ./configure --prefix="$exec_dir/samtools-1.3.1" --without-curses && \
#sudo make && \
#sudo make install



