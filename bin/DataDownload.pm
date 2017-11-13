#!/usr/bin/perl -w
use strict;
use warnings;

package DataDownload;
use FindBin qw/$RealBin/;
use lib "$RealBin/bin";
use Utility;
use File::Basename;

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
      "    Step4: Data downloading from HDFS file system to local directory \n";
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
    my $execution_time = Utility::get_execution_time( $start_time, $end_time );
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $execution_time\n\n";

}
#

sub paired_end_data {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"}       = "fastq_paired_end";
    $args_dict->{"read_type"}         = "pe";
    $args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads";

    my $hadoop_exe    = $args_dict->{'hadoop_exe'};
    my $hdfs_exe      = $args_dict->{"hdfs_exe"};
    my $output        = "/$args_dict->{'random_id'}" . "_output";
    my $output_folder = "$output/$args_dict->{'read_folder'}" . "_mapping";

    my $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
    );

    my $hdfs_input_folder =
      "/$args_dict->{'random_id'}" . "_input/fastq_paired_end";

    #my $cmd = "$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder | `wc -l`";
    my @cmd = ();
    if ( ( system("$hdfs_exe dfs -test -d $hdfs_input_folder") == 0 ) ) {
        @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
    }

    #my @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
    my $hdfs_file_count = 0;

    if ( scalar(@cmd) > 1 ) {
        $hdfs_file_count = $cmd[0];
        $hdfs_file_count =~ s/\D//g;
    }

    if ( $file_count > 0 or $hdfs_file_count > 0 ) {

        my $i = 0;
        foreach my $mapper_name ( @{ $args_dict->{"mapper"} } ) {
            my $mapper_path = $args_dict->{"mapper_path"}->[$i];
            my $mapper_args = $args_dict->{"mapper_args"}->[$i];

            $args_dict->{"final_output_file"} =
              lc($mapper_name) . "_DistMap_output_Paired_end_reads";

            #### Deleting the download folder if already exists

            my $local_output_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
              . "_mapping_"
              . lc($mapper_name);
            Utility::deletedir($local_output_dir);

            my $output_folder =
                "$output/$args_dict->{'read_folder'}"
              . "_mapping_"
              . lc($mapper_name);

            my $download_command =
"$hdfs_exe dfs -D dfs.blocksize=$args_dict->{'block_size'} -copyToLocal $output_folder $args_dict->{'output_directory'}/$args_dict->{'random_id'}/";

            print STDERR "Data download from hdfs file system started ",
              localtime(), "\n";
            system($download_command) == 0
              || warn "Error in uploading the $args_dict->{'job_arch'}\n";
            print STDERR "Data download from hdfs file system finished ",
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
    my $output        = "/$args_dict->{'random_id'}" . "_output";
    my $output_folder = "$output/$args_dict->{'read_folder'}" . "_mapping";

    my $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
    );

    my $hdfs_input_folder =
      "/$args_dict->{'random_id'}" . "_input/fastq_single_end";

    #my $cmd = "$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder | `wc -l`";
    my @cmd = ();
    if ( ( system("$hdfs_exe dfs -test -d $hdfs_input_folder") == 0 ) ) {
        @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
    }

    #my @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
    my $hdfs_file_count = 0;

    if ( scalar(@cmd) > 1 ) {
        $hdfs_file_count = $cmd[0];
        $hdfs_file_count =~ s/\D//g;
    }

    if ( $file_count > 0 or $hdfs_file_count > 0 ) {

        #### Deleting the download folder if already exists
#Utility::deletedir("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"."_mapping");
#
#my $download_command = "$hdfs_exe dfs -copyToLocal $output_folder $args_dict->{'output_directory'}/$args_dict->{'random_id'}/";
#
#print STDERR "Data download from hdfs file system started ", localtime(), "\n";
#system($download_command) == 0 || warn "Error in uploading the $args_dict->{'job_arch'}\n";
#print STDERR "Data download from hdfs file system finished ", localtime(), "\n";

        my $i = 0;
        foreach my $mapper_name ( @{ $args_dict->{"mapper"} } ) {
            my $mapper_path = $args_dict->{"mapper_path"}->[$i];
            my $mapper_args = $args_dict->{"mapper_args"}->[$i];

            $args_dict->{"final_output_file"} =
              lc($mapper_name) . "_DistMap_output_Paired_end_reads";

            #### Deleting the download folder if already exists

            my $local_output_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
              . "_mapping_"
              . lc($mapper_name);
            Utility::deletedir($local_output_dir);

            my $output_folder =
                "$output/$args_dict->{'read_folder'}"
              . "_mapping_"
              . lc($mapper_name);

            my $download_command =
"$hdfs_exe dfs -D dfs.blocksize=$args_dict->{'block_size'} -copyToLocal $output_folder $args_dict->{'output_directory'}/$args_dict->{'random_id'}/";

            print STDERR "Data download from hdfs file system started ",
              localtime(), "\n";
            system($download_command) == 0
              || warn "Error in downloading data\n";
            print STDERR "Data download from hdfs file system finished ",
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
