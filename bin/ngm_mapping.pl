#!/usr/bin/env perl -w
use strict;
use warnings;
use Getopt::Long;
use Pod::Usage;
use POSIX q/strftime/;
use Utility;
use File::stat;

{
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

my $counter = strftime( q/%Y%m%dT%H%M%S/, localtime());
$counter = "mapping_".$counter;

my $read1_fastq = $counter."read1.fastq";
my $read2_fastq = $counter."read2.fastq";

my $aln_ext = ".sai";
my $read1_sai_file = $counter."read1".$aln_ext;
my $read2_sai_file = $counter."read2".$aln_ext;

my $out_bam = "out.bam";
my $out_sam = "out.sam";
my $sam_output = $counter.$out_sam;
my $bam_output = $counter.$out_bam;


my $output_dir = "";
my $ref_fasta = "";
my $mapper_path = "";
my $hadoop="";
my $hdfs="";
my $sartsam_jar = "";
my $output_format = "bam";
my $mapper_args = "";

my $verbose = 0;
my $test = 0;

GetOptions(
    "output-dir=s"	    	=>\$output_dir,
    "ref-fasta=s"	    	=>\$ref_fasta,
    "mapper-path=s"   		=>\$mapper_path,
    "hadoop=s"   		=>\$hadoop,
    "hdfs=s"   			=>\$hdfs,
    "picard-sartsam-jar=s"	=>\$sartsam_jar,
    "output-format=s"		=>\$output_format,
    "mapper-args=s"		=>\$mapper_args,
    "test"          		=>\$test,
    "verbose"        		=> \$verbose
) or pod2usage(-msg=>"Wrong options",-verbose=>1);


if($verbose) {
	print STDERR "Command line input parameters:\n";
	print STDERR "  output directory: $output_dir\n";
	print STDERR "  reference fasta: $ref_fasta\n";
	print STDERR "  bwa executable: $mapper_path\n";
	print STDERR "  picard sartsam jar: $sartsam_jar\n";
	print STDERR "  output format: $output_format\n";
	print STDERR "  NGM options: $mapper_args\n";
}



if ($mapper_args) {
	$mapper_args=$mapper_args;
}
else {
	$mapper_args="";
}




print STDERR "read1_fastq: $read1_fastq\n";
print STDERR "read2_fastq: $read2_fastq\n";

open my $ofh1,">$read1_fastq" or die "Could not open $read1_fastq for write $!";
open my $ofh2,">$read2_fastq" or die "Could not open $read2_fastq for write $!";

my $read_type1="";
while(<>){
	my $line = $_;
	chomp($line);

	my @col = split(/\t/,$line);
	if (scalar(@col)==3) {
		my $readname = $col[0];
		my $read1 = $readname."/1"."\n";
		$read1 .= $col[1]."\n";
		$read1 .= "+\n";
		$read1 .= $col[2]."\n";
		print $ofh1 $read1;
		$read_type1="se"

	}
	elsif (scalar(@col)==5) {
		my $readname = $col[0];
		my $read1 = $readname."/1"."\n";
		my $read2 = $readname."/2"."\n";

		$read1 .= $col[1]."\n";
		$read1 .= "+\n";
		$read1 .= $col[2]."\n";

                $read2 .= $col[3]."\n";
                $read2 .= "+\n";
                $read2 .= $col[4]."\n";

		print $ofh1 $read1;
		print $ofh2 $read2;
		#print STDERR "$read1\n";
		#print STDERR "$read2\n";
		$read_type1="pe"
	}
	else {
		#die "Bad number of read columns ; expected 3 or 5:\n$_\n";
	}

}

close $ofh1;
close $ofh2;


my $ref= $ref_fasta;




if ($read_type1 =~ /(pe|pair)/i) {

	#$ENV{DISTMAP_HOME}/executables/ngm-0.4.13/ngm -b -r /Volumes/disk3/slaves-test/input/dmel_genome.fasta -1 /Volumes/disk3/slaves-test/input/read1_1M.fastq -2 /Volumes/disk3/slaves-test/input/read2_1M.fastq --sensitive -t 2 --local -o /Volumes/disk3/slaves-test/output/ngm_test1/ngm_mapres.bam


	#Usage:  ngm [-c <path>] {-q <reads> [-p] | -1 <mates 1> -2 <mates 2>} -r <reference> -o <output> [parameter]
	my $cmd1 = "$mapper_path  -p -1 $read1_fastq -2 $read2_fastq -r $ref  --bam --sensitive -t 2 --local $mapper_args";
	print STDERR "cmd1: $cmd1\n";
	Utility::runCommand($cmd1, "ngm of $read1_fastq and $read2_fastq") == 0 || die "Error bwa aln of $read1_fastq and $read2_fastq";

	#Utility::runCommand("$hdfs dfs -put $bam_output $output_dir/ >&2", "hdfsp dfs -put") == 0 || die "hdfs dfs -put command failed";

}
else {

	my $cmd1 = "$mapper_path -q $read1_fastq -r $ref --bam --local $mapper_args -o $bam_output";

	print STDERR "cmd1: $cmd1\n";
	Utility::runCommand($cmd1, "ngm of $read1_fastq") == 0 || die "Error bwa aln of $read1_fastq";

	Utility::runCommand("$hdfs dfs -put $bam_output $output_dir/ >&2", "hdfsp dfs -put") == 0 || die "hdfs dfs -put command failed";
}




print STDERR "END_OF NEM_mapping\n";
