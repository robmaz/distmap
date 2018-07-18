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

my $read1_fastq = $counter."read1.fasta";

my $out_exon = "out.exon";
my $exonerate_output = $counter.$out_exon;



my $output_dir = "";
my $ref_fasta = "";
my $mapper_path = "";
my $hadoop="";
my $hdfs="";
my $mapper_args = "";
my $verbose = 0;
my $test = 0;

GetOptions(
    "output-dir=s"	    	=>\$output_dir,
    "ref-fasta=s"	    	=>\$ref_fasta,
    "mapper-path=s"   		=>\$mapper_path,
    "hadoop=s"   		=>\$hadoop,
    "hdfs=s"   			=>\$hdfs,
    "mapper-args=s"		=>\$mapper_args,
    "test"          		=>\$test,
    "verbose"        		=> \$verbose
) or pod2usage(-msg=>"Wrong options",-verbose=>1);


if($verbose) {
	print STDERR "Command line input parameters:\n";
	print STDERR "  output directory: $output_dir\n";
	print STDERR "  reference fasta: $ref_fasta\n";
	print STDERR "  exonerate executable: $mapper_path\n";
	print STDERR "  exonerate options: $mapper_args\n";
}


if ($mapper_args) {
	$mapper_args=$mapper_args;
}
else {
	$mapper_args="";
}


print STDERR "Query_fasta: $read1_fastq\n";

open my $ofh1,">$read1_fastq" or die "Could not open $read1_fastq for write $!";

my $read_type1="";
while(<>){
	my $line = $_;
	chomp($line);
	print $ofh1 "$line\n";
	#my @col = split(/\t/);
	#if (scalar(@col)==2) {
	#	my $readname = $col[0];
	#	my $read1 = ">$readname\n";
	#	$read1 .= $col[1]."\n";
	#	print $ofh1 $read1;
	#	$read_type1="se"
	#
	#}
	#else {
	#	die "Bad number of fasta columns ; expected 2:\n$_\n";
	#}

}

close $ofh1;


my $ref= $ref_fasta;

my $cmd1 = "$mapper_path -q $read1_fastq -t $ref $mapper_args > $exonerate_output";
Utility::runCommand($cmd1, "exonerate $read1_fastq") == 0 || die "Error exonerate of $read1_fastq; $cmd1";
Utility::runCommand("$hdfs dfs -put $exonerate_output $output_dir >&2", "hdfs dfs -put") == 0 || die "hdfs dfs -put command failed";
#Utility::runCommand("$hadoop fs -put $read1_fastq $output_dir >&2", "hadoop fs -put") == 0 || die "hadoop fs -put command failed";

#open my $sfh,"<$exonerate_output" or die "Could not open $exonerate_output for write $!";
#
#	while(<$sfh>){
#		#chomp;
#		print "$_";
#
#	}
#	close $sfh;
#

print STDERR "END_OF exonerate_mapping\n";
