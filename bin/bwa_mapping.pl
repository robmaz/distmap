#!/usr/bin/env perl
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
my $mapper_args = "";
my $bwa_sampe_options = "";

my $verbose = 0;
my $test = 0;

GetOptions(
    "output-dir=s"	    	=>\$output_dir,
    "ref-fasta=s"	    	=>\$ref_fasta,
    "mapper-path=s"   		=>\$mapper_path,
    "hadoop=s"   		=>\$hadoop,
    "hdfs=s"   			=>\$hdfs,
    "mapper-args=s"		=>\$mapper_args,
    "bwa-sampe-options=s"	=>\$bwa_sampe_options,
    "test"          		=>\$test,
    "verbose"        		=> \$verbose
) or pod2usage(-msg=>"Wrong options",-verbose=>1);


if($verbose) {
	print STDERR "Command line input parameters:\n";
	print STDERR "  output directory: $output_dir\n";
	print STDERR "  reference fasta: $ref_fasta\n";
	print STDERR "  bwa executable: $mapper_path\n";
	print STDERR "  bwa aln options: $mapper_args\n";
	print STDERR "  bwa sampe/samse options: $bwa_sampe_options\n";
}


if ($bwa_sampe_options) {
	$bwa_sampe_options=$bwa_sampe_options;
}
else {
	$bwa_sampe_options="";
}


if ($mapper_args) {
	$mapper_args=$mapper_args;
}
else {
	$mapper_args="";
}

my $mem_mapping=0;
my $bwasw_mapping=0;


if ($mapper_args =~ /\-\-bwamem/i) {

	print STDERR "$mapper_args\n";
	$mapper_args =~ s/\-\-bwamem//im;
	$mem_mapping=1;
}
elsif ($mapper_args =~ /\-bwamem/i) {
	print STDERR "$mapper_args\n";
	$mapper_args =~ s/\-bwamem//im;
	$mem_mapping=1;
}
elsif ($mapper_args =~ /bwamem/i) {
	print STDERR "$mapper_args\n";
	$mapper_args =~ s/bwamem//im;
	$mem_mapping=1;
}


if ($mapper_args =~ /\-\-bwasw/i) {

	print STDERR "$mapper_args\n";
	$mapper_args =~ s/\-\-bwasw//im;
	$bwasw_mapping=1;
}
elsif ($mapper_args =~ /\-bwasw/i) {
	print STDERR "$mapper_args\n";
	$mapper_args =~ s/\-bwasw//im;
	$bwasw_mapping=1;
}
elsif ($mapper_args =~ /bwasw/i) {
	print STDERR "$mapper_args\n";
	$mapper_args =~ s/bwasw//im;
	$bwasw_mapping=1;
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


##$bwa_exe="bwaexe/bwa";
##my $ref= "bwaexe/reference.fa";
my $ref= $ref_fasta;

if (system("$hdfs dfs -test -e $output_dir/$bam_output")==0) {
	exit();

}

#my $index_command = "$mapper_path index ref.fa > $output_dir/";

#print STDERR "index_command: $index_command\n";
#Utility::runCommand($index_command, "bwa index command is running") == 0 || die "Error bwa index command";



if ($read_type1 =~ /(pe|pair)/i) {

	if ($mem_mapping) {
		my $cmd1 = "$mapper_path mem $mapper_args -M $ref $read1_fastq $read2_fastq";
		print STDERR "cmd1: $cmd1\n";
		Utility::runCommand($cmd1, "bwa mem of $read1_fastq and $read2_fastq") == 0 || die "Error bwa mem of $read1_fastq and $read2_fastq";
	}
	elsif ($bwasw_mapping) {
		my $cmd1 = "$mapper_path bwasw $mapper_args $ref $read1_fastq $read2_fastq";
		print STDERR "cmd1: $cmd1\n";
		Utility::runCommand($cmd1, "bwasw of $read1_fastq and $read2_fastq") == 0 || die "Error bwasw of $read1_fastq and $read2_fastq";
	}
	else {
		my $cmd1 = "$mapper_path aln $mapper_args $ref $read1_fastq > $read1_sai_file";
		my $cmd2 = "$mapper_path aln $mapper_args $ref $read2_fastq > $read2_sai_file";
		#my $cmd3 = "$mapper_path sampe $bwa_sampe_options $ref $read1_sai_file $read2_sai_file $read1_fastq $read2_fastq  > $sam_output";
		my $cmd3 = "$mapper_path sampe $bwa_sampe_options $ref $read1_sai_file $read2_sai_file $read1_fastq $read2_fastq";
		print STDERR "cmd1: $cmd1\n";
		print STDERR "cmd2: $cmd2\n";
		print STDERR "cmd3: $cmd3\n";


		Utility::runCommand($cmd1, "bwa aln of $read1_fastq") == 0 || die "Error bwa aln of $read1_fastq";
		Utility::runCommand($cmd2, "bwa aln of $read2_fastq") == 0 || die "Error bwa aln of $read2_fastq";
		Utility::runCommand($cmd3, "bwa sampe of $read1_sai_file and $read2_sai_file") == 0 || die "Error bwa sampe of $read1_sai_file and $read2_sai_file";
	}
}
else {
	if ($mem_mapping) {
		my $cmd1 = "$mapper_path mem $mapper_args -M $ref $read1_fastq";
		print STDERR "cmd1: $cmd1\n";
		Utility::runCommand($cmd1, "bwa mem of $read1_fastq") == 0 || die "Error bwa aln of $read1_fastq";
	}
	elsif ($bwasw_mapping) {
		my $cmd1 = "$mapper_path bwasw $mapper_args $ref $read1_fastq";
		print STDERR "cmd1: $cmd1\n";
		Utility::runCommand($cmd1, "bwa bwasw of $read1_fastq") == 0 || die "Error bwasw of $read1_fastq";
	}
	else {
		my $cmd1 = "$mapper_path aln $mapper_args $ref $read1_fastq > $read1_sai_file";
		my $cmd3 = "$mapper_path samse $bwa_sampe_options $ref $read1_sai_file $read1_fastq";
		#my $cmd3 = "$mapper_path samse $bwa_sampe_options $ref $read1_sai_file $read1_fastq  > $sam_output";
		Utility::runCommand($cmd1, "bwa aln of $read1_fastq") == 0 || die "Error bwa aln of $read1_fastq";
		Utility::runCommand($cmd3, "bwa sampe of $read1_sai_file") == 0 || die "Error bwa sampe of $read1_sai_file";
	}

}

print STDERR "END_OF bwa_mapping\n";
