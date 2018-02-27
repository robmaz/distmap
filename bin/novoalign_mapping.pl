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
my $ref_dir="";
my $mapper_path = "";
my $hadoop="";
my $hdfs="";
my $output_format = "bam";
my $mapper_args = "";
my $bwa_sampe_options = "";

my $verbose = 0;
my $test = 0;

GetOptions(
    "output-dir=s"	    	=>\$output_dir,
    "ref-fasta=s"	    	=>\$ref_fasta,
    "ref-dir=s"	    		=>\$ref_dir,
    "mapper-path=s"   		=>\$mapper_path,
    "hadoop=s"   		=>\$hadoop,
    "hdfs=s"   			=>\$hdfs,
    "output-format=s"		=>\$output_format,
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
	print STDERR "  output format: $output_format\n";
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
my $ref= "$ref_dir/$ref_fasta";

if (system("$hdfs dfs -test -e $output_dir/$bam_output")==0) {
	exit();

}



if ($read_type1 =~ /(pe|pair)/i) {
	#novoalign -d ../reference/2R-2Mbp.nix -f $f1 $f2 -i 350,50 -F STDFQ -o SAM > $o
	my $cmd1 = "$mapper_path -d $ref -f $read1_fastq $read2_fastq $mapper_args -o SAM";
	print STDERR "cmd1: $cmd1\n";
	Utility::runCommand($cmd1, "novoalign of $read1_fastq and $read2_fastq") == 0 || die "Error novoalign of $read1_fastq and $read2_fastq";

}
else {
	my $cmd1 = "$mapper_path -d $ref -f $read1_fastq $mapper_args -o SAM";
	print STDERR "cmd1: $cmd1\n";
	Utility::runCommand($cmd1, "novoalign of $read1_fastq") == 0 || die "Error novoalign aln of $read1_fastq";


}



print STDERR "END_OF novoalign_mapping\n";
