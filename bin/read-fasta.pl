#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
use Pod::Usage;
use File::Path;
use File::Basename; # to get the file path, file name and file extension

#   Author: Ram Vinay Pandey

#   Date: 24-06-2011

my $input;
my $output;
my $help=0;
my $test=0;


GetOptions(
    "input=s"	        =>\$input,
    "output=s"	        =>\$output,
    "test"              =>\$test,
    "help"	        =>\$help
) or pod2usage(-msg=>"Wrong options",-verbose=>1);

pod2usage(-verbose=>2) if $help;
pod2usage(-verbose=>2) if $test;
#SyncTest::runTests() if $test;
pod2usage(-msg=>"Input file does not exist. Please provide an input pileup file. ",-verbose=>1) unless -e $input;
pod2usage(-msg=>"No output directory/folder path has been provided",-verbose=>1) unless $output;



my $sequence = get_fasta_seq($input);

print "$sequence\n";
open my $ofh, ">",$output or die "Could not open $output\n";

foreach (@$sequence) {
     my $id = $_->{id};
     print $ofh ">$id\n";
     print $ofh "$_->{sequence}\n";
     my $seq = split_sequence($_->{sequence});

     #foreach my $s (@$seq) {
     #     print $ofh "$s\n";
     #}
}


close $ofh;

   sub get_fasta_seq {

	my ($file) = @_;

	my $sequence = [];
	open(FILE, $file) || die("Couldn't read file $file\n");

	local $/ = "\n>";  # read by FASTA record

	my $header = "";

	while (my $seq = <FILE>) {
	    chomp $seq;
	    if($seq =~ m/^>*.+\n/) {
		$seq =~ m/(^>*.+\n)/;
		$header=$1;
	    }
	    $seq =~ s/^>*.+\n//;  # remove FASTA header
	    $seq =~ s/\n//g;  # remove endlines

	    if($header =~ /^\>/) {
		$header =~ s/^\>//;
	    }
	    $header=~ s/\n//;
	    chomp($header);
	    chomp($seq);
	    $seq =~ s/\s+//g;
	    $seq =~ s/\s+$//;
	    my $e = {
		id=>$header,
		sequence=>$seq
	    };
	    push (@$sequence,$e);

	}

	return $sequence;

    }

    sub split_sequence  {
	my ($sequence) = @_;
	my $seq = [];

	my $length=60;

	    # Print sequence in lines of $length
	    my $j=0;
	    for(my $pos = 0;$pos<length($sequence);$pos+=$length)
	    {
		    $seq->[$j]=uc(substr($sequence,$pos,$length));
		    $j++;
	    }
	    return $seq;
    }
