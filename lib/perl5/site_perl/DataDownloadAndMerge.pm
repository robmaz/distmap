#!/usr/bin/env perl
use strict;
use warnings;

package DataDownloadAndMerge;
use FindBin qw/$RealBin/;
use lib "$RealBin/../lib/perl5/site_perl";
use Utility;
use File::Basename;
use File::Temp qw/tempdir/;

sub new {
    my $class = shift;
    my $self  = {};
    bless $self, $class;
    return $self;
}

sub start {
    my ( $self, $args_dict ) = @_;

    print STDERR "\n\n";
    print STDERR
"======================================================================\n";
    print STDERR
"    Step4+5: Merge/Downloading from HDFS file system to local directory \n";
    print STDERR
"======================================================================\n";

    ################### Time start #########################
    my $start_time       = time();
    my $time_stamp_start = Utility::get_time_stamp();
    print STDERR "\nStarted at:  $time_stamp_start\n";

    ### Paired-end data download
    $self->paired_end_data($args_dict);

    ### Single-end data download
    $self->single_end_data($args_dict);

    ################### Time end #########################
    my $end_time       = time();
    my $execution_time = Utility::get_executation_time( $start_time, $end_time );
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $execution_time\n\n";
}

sub paired_end_data {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"}       = "fastq_paired_end";
    $args_dict->{"read_type"}         = "pe";
    $args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads";

    my $hadoop_exe    = $args_dict->{'hadoop_exe'};
    my $hdfs_exe      = $args_dict->{"hdfs_exe"};
    my $readtools     = $args_dict->{"readtools"};
    my $output        = "/$args_dict->{'hdfs_home'}" . "_output";
    my $output_folder = "$output/$args_dict->{'read_folder'}" . "_mapping";

    my $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}"
    );

    my $hdfs_input_folder =
      "/$args_dict->{'hdfs_home'}" . "_input/fastq_paired_end";

    my @cmd = ();
    if ( ( system("$hdfs_exe dfs -test -d $hdfs_input_folder") == 0 ) ) {
        @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
    }

    my $hdfs_file_count = 0;

    if ( scalar(@cmd) > 1 ) {
        $hdfs_file_count = $cmd[0];
        $hdfs_file_count =~ s/\D//g;
    }

    chomp(my $hdfs = `$hdfs_exe getconf -confKey "fs.defaultFS"`);

    if ( $file_count > 0 or $hdfs_file_count > 0 ) {

        my $i = 0;
        foreach my $mapper_name ( @{ $args_dict->{"mapper"} } ) {
            my $mapper_path = $args_dict->{"mapper_path"}->[$i];
            my $mapper_args = $args_dict->{"mapper_args"}->[$i];

            $args_dict->{"final_output_file"} =
              lc($mapper_name) . "_DistMap_output_Paired_end_reads";

            my $output_folder =
                "$output/$args_dict->{'read_folder'}"
              . "_mapping_"
              . lc($mapper_name);
            my $local_output_dir = $args_dict->{"output_directory"};
            my $output_file =
                $args_dict->{"final_output_file"} . "."
              . $args_dict->{"output_format"};
            my $tmp_dir = ( $args_dict->{"tmp_dir"} or $local_output_dir );
            $tmp_dir = tempdir( ".readtools.DDR.XXXXX",
                                              DIR => $tmp_dir,
                                              CLEANUP => 1 );
            my $download_command =
            qq( JAVA_OPTS="-Xmx8g -Dsnappy.disable=true" $readtools DownloadDistmapResult --input $hdfs/$output_folder --output $local_output_dir/$output_file --TMP_DIR $tmp_dir --forceOverwrite );

            print STDERR "Data merge/download from hdfs file system started ",
              localtime(), "\n";
            system("$download_command") == 0
              || warn
              "Error in merge/downloading the $args_dict->{'job_arch'}\n";
            print STDERR "Data merge/download from hdfs file system finished ",
              localtime(), "\n";

            $i++;
        }
    }
}

sub single_end_data {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"}       = "fastq_single_end";
    $args_dict->{"read_type"}         = "se";
    $args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads";

    my $hadoop_exe    = $args_dict->{'hadoop_exe'};
    my $hdfs_exe      = $args_dict->{"hdfs_exe"};
    my $readtools     = $args_dict->{"readtools"};
    my $output        = "/$args_dict->{'hdfs_home'}" . "_output";
    my $output_folder = "$output/$args_dict->{'read_folder'}" . "_mapping";

    my $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}"
    );

    my $hdfs_input_folder =
      "/$args_dict->{'hdfs_home'}" . "_input/fastq_single_end";

    my @cmd = ();
    if ( ( system("$hdfs_exe dfs -test -d $hdfs_input_folder") == 0 ) ) {
        @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
    }

    my $hdfs_file_count = 0;

    if ( scalar(@cmd) > 1 ) {
        $hdfs_file_count = $cmd[0];
        $hdfs_file_count =~ s/\D//g;
    }

    chomp(my $hdfs = `$hdfs_exe getconf -confKey "fs.defaultFS"`);

    if ( $file_count > 0 or $hdfs_file_count > 0 ) {

        my $i = 0;
        foreach my $mapper_name ( @{ $args_dict->{"mapper"} } ) {
            my $mapper_path = $args_dict->{"mapper_path"}->[$i];
            my $mapper_args = $args_dict->{"mapper_args"}->[$i];

            $args_dict->{"final_output_file"} =
              lc($mapper_name) . "_DistMap_output_Paired_end_reads";

            my $output_folder =
                "$output/$args_dict->{'read_folder'}"
              . "_mapping_"
              . lc($mapper_name);
            my $local_output_dir = $args_dict->{"output_directory"};
            my $output_file =
                $args_dict->{"final_output_file"} . "."
              . $args_dict->{"output_format"};
            my $tmp_dir = ( $args_dict->{"tmp_dir"} or $local_output_dir );
            $tmp_dir = tempdir( ".readtools.DDR.XXXXX",
                                                DIR => $tmp_dir,
                                                CLEANUP => 1 );
              my $download_command =
  qq( JAVA_OPTS="-Xmx8g -Dsnappy.disable=true" $readtools DownloadDistmapResult --input $hdfs/$output_folder --output $local_output_dir/$output_file --TMP_DIR $tmp_dir --forceOverwrite );

            print STDERR "Data merge/download from hdfs file system started ",
              localtime(), "\n";
            system("$download_command") == 0
              || warn
              "Error in merge/downloading the $args_dict->{'job_arch'}\n";
            print STDERR "Data merge/download from hdfs file system finished ",
              localtime(), "\n";

            $i++;
        }
    }
}

sub get_file_list {
    my ( $self, $dir ) = @_;
    opendir( DIR, $dir ) || die("Cannot open directory");
    my @files = readdir(DIR);
    closedir(DIR);

    my @f = ();
    foreach my $file (@files) {
        if ( $file =~ /^fastq/i ) {
            push( @f, $file );
        }
    }

    return scalar(@f);
}

1;
