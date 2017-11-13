#!/usr/bin/perl -w
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

my $counter = strftime( q/%Y%m%dT%H%M%S/, localtime() );
$counter = "mapping_" . $counter;

my $read1_fastq = $counter . "read1.fastq";
my $read2_fastq = $counter . "read2.fastq";
my $out_bam     = "out.bam";
my $out_sam     = "out.sam";
my $sam_output  = $counter . $out_sam;
my $bam_output  = $counter . $out_bam;

my $output_dir    = "";
my $ref_fasta     = "";
my $ref_dir       = "";
my $mapper_path   = "";
my $mapper_args   = "";
my $hadoop        = "";
my $hdfs          = "";
my $sartsam_jar   = "";
my $output_format = "bam";
my $quality_encoding;
my $verbose = 0;
my $test    = 0;

GetOptions(
    "output-dir=s"         => \$output_dir,
    "ref-fasta=s"          => \$ref_fasta,
    "ref-dir=s"            => \$ref_dir,
    "mapper-path=s"        => \$mapper_path,
    "hadoop=s"             => \$hadoop,
    "hdfs=s"               => \$hdfs,
    "picard-sartsam-jar=s" => \$sartsam_jar,
    "output-format=s"      => \$output_format,
    "mapper-args=s"        => \$mapper_args,
    "quality-encoding=s"   => \$quality_encoding,
    "test"                 => \$test,
    "verbose"              => \$verbose
) or pod2usage( -msg => "Wrong options", -verbose => 1 );

if ($verbose) {
    print STDERR "Command line input parameters:\n";
    print STDERR "  output directory: $output_dir\n";
    print STDERR "  reference fasta: $ref_fasta\n";
    print STDERR "  reference directory: $ref_dir\n";
    print STDERR "  mapper path: $mapper_path\n";
    print STDERR "  picard sartsam jar: $sartsam_jar\n";
    print STDERR "  output format: $output_format\n";
    print STDERR "  TopHat arguments: $mapper_args\n";
    print STDERR "  quality encoding: $quality_encoding\n";
}

if ( $output_dir =~ /\/$/ ) {
    $output_dir =~ s/\/$//;
}
if ($mapper_args) {
    $mapper_args = $mapper_args;
}
else {
    $mapper_args = "";
}

print STDERR "read1_fastq: $read1_fastq\n";
print STDERR "read2_fastq: $read2_fastq\n";

open my $ofh1, ">$read1_fastq"
  or die "Could not open $read1_fastq for write $!";
open my $ofh2, ">$read2_fastq"
  or die "Could not open $read2_fastq for write $!";

my $read_type1 = "";

while (<>) {
    my $line = $_;
    chomp($line);

    my @col = split(/\t/);
    if ( scalar(@col) == 3 ) {
        my $readname = $col[0];
        my $read1    = $readname . "/1" . "\n";
        $read1 .= $col[1] . "\n";
        $read1 .= "+\n";
        $read1 .= $col[2] . "\n";
        print $ofh1 $read1;
        $read_type1 = "se"

    }
    elsif ( scalar(@col) == 5 ) {
        my $readname = $col[0];
        my $read1    = $readname . "/1" . "\n";
        my $read2    = $readname . "/2" . "\n";

        $read1 .= $col[1] . "\n";
        $read1 .= "+\n";
        $read1 .= $col[2] . "\n";

        $read2 .= $col[3] . "\n";
        $read2 .= "+\n";
        $read2 .= $col[4] . "\n";

        print $ofh1 $read1;
        print $ofh2 $read2;
        $read_type1 = "pe";
    }
    else {
        die "Bad number of read columns ; expected 3 or 5:\n$_\n";
    }
}

close $ofh1;
close $ofh2;

if ( $read_type1 =~ /(pe|pair)/i ) {
    my $outdir = $counter;
    my $cmd1 =
"$mapper_path $mapper_args -o $outdir $ref_dir/$ref_fasta $read1_fastq $read2_fastq";
    print STDERR "TopHat Command: $cmd1\n";
    Utility::runCommand( $cmd1,
        "tophat mapping of $read1_fastq and $read2_fastq" ) == 0
      || die "Error tophat mapping of $read1_fastq and $read2_fastq";
}
else {
    my $outdir = $counter;
    my $cmd1 =
      "$mapper_path $mapper_args -o $outdir $ref_dir/$ref_fasta $read1_fastq";
    Utility::runCommand( $cmd1, "tophat mapping of $read1_fastq" ) == 0
      || die "Error tophat mapping of $read1_fastq and $read2_fastq";
}

my $tophat_output_dir = $counter;

my $tophat_output_dir1 = "$output_dir/$counter";

my $cmd2 = "$hdfs dfs -mkdir $tophat_output_dir1";
Utility::runCommand( $cmd2, "hdfs dfs -mkdir" ) == 0
  || warn "hdfs dfs -mkdir command failed";

Utility::runCommand( "$hdfs dfs -put $counter/* $tophat_output_dir1/ >&2",
    "hdfs dfs -put" ) == 0
  || die "hdfs dfs -put command failed";

#if ($output_format =~ /bam/i) {
#	my $cmd2 = "$hadoop jar $sartsam_jar I=$sam_output O=$bam_output SO=coordinate VALIDATION_STRINGENCY=SILENT";
#	Utility::runCommand($cmd2, "converting SAM into BAM") == 0 || die "Error in converting SAM into BAM";
#	Utility::runCommand("$hadoop fs -put $bam_output $output_dir >&2", "hadoop fs -put") == 0 || die "hadoop fs -put command failed";
#}
#
#else {
#	open my $sfh,"<$sam_output" or die "Could not open $sam_output for write $!";
#
#	while(<$sfh>){
#		chomp;
#		print "$_\n";
#
#	}
#	close $sfh;
#}

my $cmd3 = "$hdfs dfs -chmod -R 777 $tophat_output_dir1";
Utility::runCommand( $cmd3, "hdfs dfs -chmod -R 777" ) == 0
  || warn "hdfs dfs -chmod -R 777 command failed";

print STDERR "END_OF tophat_mapping\n";
