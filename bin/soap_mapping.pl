#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
use Pod::Usage;
use POSIX q/strftime/;
use Utility;

{
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}


my $counter = strftime( q/%Y%m%dT%H%M%S/, localtime());
$counter = "mapping_".$counter;

my $read1_fastq = $counter."read1.fq";
my $read2_fastq = $counter."read2.fq";
my $out_bam = "out.bam";
my $out_soap = "out.soap";
my $out_sam = "out.sam";
my $soap_output = $counter.$out_soap;
my $sam_output = $counter.$out_sam;
my $bam_output = $counter.$out_bam;
my $unpaired_hits = $counter."unpaired_hits";

my $output_dir = "";
my $ref_fasta = "";
my $ref_dir="";
my $mapper_path = "";
my $soap2sam_path = "";
my $mapper_args = "";
my $hadoop="";
my $hdfs="";
my $quality_encoding;
my $verbose = 0;
my $test = 0;

GetOptions(
    "output-dir=s"	    	=>\$output_dir,
    "ref-fasta=s"	    	=>\$ref_fasta,
    "ref-dir=s"	    		=>\$ref_dir,
    "mapper-path=s"   		=>\$mapper_path,
    "soap2sam-path=s"   	=>\$soap2sam_path,
    "hadoop=s"   		=>\$hadoop,
    "hdfs=s"   			=>\$hdfs,
    "mapper-args=s"		=>\$mapper_args,
    "quality-encoding=s"	=>\$quality_encoding,
    "test"          		=>\$test,
    "verbose"        		=> \$verbose
) or pod2usage(-msg=>"Wrong options",-verbose=>1);


if($verbose) {
	print STDERR "Command line input parameters:\n";
	print STDERR "  output directory: $output_dir\n";
	print STDERR "  reference fasta: $ref_fasta\n";
	print STDERR "  reference directory: $ref_dir\n";
	print STDERR "  mapper path: $mapper_path\n";
	print STDERR "  soap2sam.pl path: $soap2sam_path\n";
	print STDERR "  GSNAP arguments: $mapper_args\n";
	print STDERR "  quality encoding: $quality_encoding\n";
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

	my @col = split(/\t/);
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
		$read_type1="pe"
	}
	else {
		die "Bad number of read columns ; expected 3 or 5:\n$_\n";
	}
}

close $ofh1;
close $ofh2;


if ($read_type1 =~ /(pe|pair)/i) {
	my $cmd1 = "$mapper_path -D $ref_dir/$ref_fasta -a $read1_fastq -b $read2_fastq -o $soap_output -2 $unpaired_hits $mapper_args";
	Utility::runCommand($cmd1, "soap mapping of $read1_fastq and $read2_fastq") == 0 || die "Error soap mapping of $read1_fastq and $read2_fastq";

	my $cmd2 = "$soap2sam_path -p $soap_output > $sam_output";
	Utility::runCommand($cmd2, "converting $soap_output to $sam_output") == 0 || die "Error converting $soap_output to $sam_output";
}
else {
	my $cmd1 = "$mapper_path -D $ref_dir/$ref_fasta -a $read1_fastq -o $soap_output -2 $unpaired_hits $mapper_args";
	Utility::runCommand($cmd1, "soap mapping of $read1_fastq") == 0 || die "Error soap mapping of $read1_fastq and $read2_fastq";

	my $cmd2 = "$soap2sam_path -p $soap_output > $sam_output";
	Utility::runCommand($cmd2, "converting $soap_output to $sam_output") == 0 || die "Error converting $soap_output to $sam_output";
}

print STDERR "END_OF soap_mapping\n";
