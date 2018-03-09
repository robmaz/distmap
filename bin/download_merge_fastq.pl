#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
use Pod::Usage;
use IO::Compress::Gzip;

my $verbose = 0;
my $test = 0;
my $help = 0;
my $nozip=0;

my $output="";
my $hdfs_dir="";
my $hdfs_exe="";


GetOptions(
	"output=s"	=>\$output,
	"hdfs-dir=s"    => \$hdfs_dir,
        "hdfs=s"        => \$hdfs_exe,
        "disable-zipped-output" =>\$nozip,
	"verbose"       => \$verbose,
	"help"          =>\$help,
) or pod2usage(-msg=>"Wrong options",-verbose=>1);

write_fastq();

sub write_fastq {
    my $temp_file = $output."_temp";
    my $download_command = "$hdfs_exe dfs -getmerge $hdfs_dir $temp_file";
    print "\n\tdownload: $download_command\n";
    system($download_command);

    my $read1_fastq = $output."_trimmed_1.fastq";
    my $read2_fastq = $output."_trimmed_2.fastq";

    my $ofh1 = getofhcreater($nozip,$read1_fastq);
    my $ofh2 = getofhcreater($nozip,$read2_fastq);

    #open my $ofh1,">$read1_fastq" or die "Could not open $read1_fastq for write $!";
    #open my $ofh2,">$read2_fastq" or die "Could not open $read2_fastq for write $!";

    open my $fh,"<$temp_file" or die "Could not open $temp_file for reading $!";


    my $read_type1="";
    while(<$fh>){
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
                    $read2 .= $col[4]."";

                    print $ofh1 $read1;
                    print $ofh2 $read2;
                    #print STDERR "$read1\n";
                    #print STDERR "$read2\n";
                    $read_type1="pe"
            }
            else {
                    die "Bad number of read columns ; expected 3 or 5:\n$_\n";
            }

    }

    close $ofh1;
    close $ofh2;


    system("rm -r $read2_fastq") if ($read_type1 !~ /(pe|pair)/i);

    system("rm -r $temp_file") if (-e $temp_file);

}


sub getofhcreater{
    my $nozip=shift;
    my $outfile=shift;
    my $ofh=undef;
    if($nozip){
        open $ofh, ">", $outfile or die "Could not open output file $outfile $!";
    }
    else{
        $outfile=$outfile . ".gz";
        $ofh = new IO::Compress::Gzip $outfile or die "Could not open gzipped output file $outfile $!";
    }
    return $ofh;
}
