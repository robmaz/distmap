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
$counter = "trimming_".$counter;

my $read1_fastq = $counter."read1.fastq";
my $read2_fastq = $counter."read2.fastq";

my $read1_fastq_out = $counter."trimmed_read1.fastq";
my $read2_fastq_out = $counter."trimmed_read2.fastq";
my $read_se_output = $counter."trimmed_read_se.fastq";

my $trim_output_prefix = $counter."trimmed_read";
my $trim_output_prefix_read1 = $counter."trimmed_read_1.fq";
my $trim_output_prefix_read2 = $counter."trimmed_read_2.fq";
my $trim_output_prefix_SE = $counter."trimmed_read.fq";

my $merged_output =  $counter."trimmed";

my $std_output =  $counter."_std";

my $output_dir = "";
my $hadoop="";
my $exe_path="";
my $hdfs="";
my $trim_args = "";
my $verbose = 0;
my $test = 0;

GetOptions(
    "output-dir=s"	    	=>\$output_dir,
    "hadoop=s"   		=>\$hadoop,
    "exe-path=s"   		=>\$exe_path,
    "hdfs=s"   			=>\$hdfs,
    "trim-args=s"		=>\$trim_args,
    "test"          		=>\$test,
    "verbose"        		=> \$verbose
) or pod2usage(-msg=>"Wrong options",-verbose=>1);


if($verbose) {
	print STDERR "Command line input parameters:\n";
	print STDERR "  output directory: $output_dir\n";
	print STDERR "  trimming options: $trim_args\n";

}

print STDERR "Using trim-args\t$trim_args\n";


if ($trim_args) {
	$trim_args=$trim_args;
}
else {
	$trim_args="";
}


print STDERR "read1_fastq: $read1_fastq\n";
print STDERR "read2_fastq: $read2_fastq\n";

open my $ofh1,">$read1_fastq" or die "Could not open $read1_fastq for write $!";
open my $ofh2,">$read2_fastq" or die "Could not open $read2_fastq for write $!";

my $read_type1="";
while(<>){
	my $line = $_;
	chomp($line);
	#print STDERR "read:$line\n";
	#my @col = split(/\t/);
	my @col = split("\t", $line);

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
		#print STDERR "read1: $read1\n";
		#print STDERR "read2: $read2\n";
		$read_type1="pe"
	}
	else {
		die "Bad number of read columns ; expected 3 or 5:\n$_\n";
	}

}

close $ofh1;
close $ofh2;



#if (system("$hadoop dfs -test -e $output_dir/$merged_output")==0) {
#	exit();#

#}

#my $trim_perl_script="$exe_path/trim-fastq.pl";
my $trim_perl_script="trim-fastq.pl";
#$exe_path="TrimFastq.jar";

if ($read_type1 =~ /(pe|pair)/i) {

	my $cmd="";
	my $cmd1 = "perl trim-fastq.pl $trim_args --disable-zipped-output --input1 $read1_fastq --input2 $read2_fastq --output1 $read1_fastq_out --output2 $read2_fastq_out --outputse $read_se_output > $std_output";

	$cmd = "perl trim-fastq.pl $trim_args --disable-zipped-output --input1 $read1_fastq --input2 $read2_fastq --output1 $read1_fastq_out --output2 $read2_fastq_out --outputse $read_se_output > $std_output";


	unless (-e $trim_perl_script) {
		$cmd = "java -Xmx4g -jar $exe_path TrimFastq $trim_args --disable-zipped-output --input1 $read1_fastq --input2 $read2_fastq --number-of-thread 1 --output $trim_output_prefix";
	}

	print STDERR "cmd: $cmd\n";

	Utility::runCommand($cmd, "perl trim-fastq.pl of $read1_fastq and $read2_fastq") == 0 || die "Error perl trim-fastq.pl of $read1_fastq and $read2_fastq";

	open (my $ofh,">".$merged_output) or die "could not open $_";


	my $fh1 = undef;
	my $fh2 = undef;

	if (-e $read1_fastq_out and -e $read2_fastq_out) {
		open $fh1,$read1_fastq_out or die "could not open $_";
		open $fh2,$read2_fastq_out or die "could not open $_";
	}
	else {
		open $fh1,$trim_output_prefix_read1 or die "could not open $_";
		open $fh2,$trim_output_prefix_read2 or die "could not open $_";
	}

	my $line_f1;
	my $line_f2;
	my $ct = 0;

	while (defined($line_f1 = <$fh1>)&& defined($line_f2 = <$fh2>)){


		my $temp;
		$ct++;

		my $read_name = $line_f1;
		chomp($read_name);

		$read_name =~ s/(.*)(\/[0-9])/$1/;
		$temp = $read_name."\t";

		$line_f1 = <$fh1>;
		my $seq_data = $line_f1;
		chomp($seq_data);
		$temp .= $seq_data."\t";

		$line_f1 = <$fh1>;
		$line_f1 = <$fh1>;
		my $q_data = $line_f1;
		chomp($q_data);
		$temp .= $q_data."\t";

		$line_f2 = <$fh2>;
		$seq_data = $line_f2;
		chomp($seq_data);
		$temp .= $seq_data."\t";

		$line_f2 = <$fh2>;
		$line_f2 = <$fh2>;
		$q_data = $line_f2;
		chomp($q_data);
		$temp .= $q_data."\n";

		#print $ofh $temp;
		print $temp;
		$ct++;

	}

	print STDERR "file: $ct Paired-end reads\n";

}
else {
	my $cmd="";
	my $cmd1 = "perl trim-fastq.pl $trim_args --disable-zipped-output --input1 $read1_fastq --output1 $read1_fastq_out > $std_output";
	print STDERR "cmd1: $cmd1\n";


	$cmd = "perl trim-fastq.pl $trim_args --disable-zipped-output --input1 $read1_fastq --output1 $read1_fastq_out > $std_output";


	unless (-e $trim_perl_script) {
		$cmd = "java -Xmx4g -jar $exe_path TrimFastq $trim_args --disable-zipped-output --input1 $read1_fastq --number-of-thread 1 --output $trim_output_prefix";
	}

	Utility::runCommand($cmd, "perl trim-fastq.pl of $read1_fastq") == 0 || die "Error perl trim-fastq.pl of $read1_fastq";

	open (my $ofh,">".$merged_output) or die "could not open $_";


	my $fh = undef;

	if (-e $read1_fastq_out) {
		open $fh,$read1_fastq_out or die "could not open $_";
	}
	else {

		open $fh,$trim_output_prefix_SE or die "could not open $_";
	}


	my $line_f;
	my $ct = 0;

	while (defined($line_f = <$fh>)){


		my $temp;
		$counter++;

		my $read_name = $line_f;
		chomp($read_name);

		$read_name =~ s/(.*)(\/[0-9])/$1/;
		$temp = $read_name."\t";

		$line_f = <$fh>;
		my $seq_data = $line_f;
		chomp($seq_data);
		$temp .= $seq_data."\t";

		$line_f = <$fh>;
		$line_f = <$fh>;
		my $q_data = $line_f;
		chomp($q_data);
		$temp .= $q_data."\n";
		#print STDERR "FASTQ: $temp\n";
		#print $ofh $temp;
		print $temp;
		$ct++;

	}

	print STDERR "file: $ct Single-end reads\n";

	#Utility::runCommand("$hadoop dfs -put $merged_output $output_dir/ >&2", "hdfsp dfs -put $merged_output") == 0 || die "hdfs dfs -put $merged_output command failed";
	#Utility::runCommand("$hadoop dfs -put $read1_fastq_out $output_dir/ >&2", "hdfsp dfs -put $merged_output") == 0 || die "hdfs dfs -put $merged_output command failed";


}


#print STDERR "Putting trimmed file $merged_output in $output_dir\n";
#print STDERR "$hdfs dfs -put $merged_output $output_dir/ >&2\n";
#Utility::runCommand("$hdfs dfs -put $merged_output $output_dir/ >&2", "hdfsp dfs -put $merged_output") == 0 || die "hdfs dfs -put $merged_output command failed";


print STDERR "END_OF cluster_trimming\n";
