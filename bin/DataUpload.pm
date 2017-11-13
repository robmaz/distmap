#!/usr/bin/perl -w
use strict;
use warnings;

package DataUpload;
use FindBin qw/$RealBin/;
use lib "$RealBin/bin";
use Utility;
use File::Basename;

##
# Author: Ram Vinay Pandey
#   Date: February 24, 2013
#
# This script uploads all input files and archives into HDFS for Hadoop mapping.
#

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
      "===============================================================\n";
    print STDERR "    Step3: Data loading in HDFS file system\n";
    print STDERR
      "===============================================================\n";

    ################### Time start #########################
    my $start_time       = time();
    my $time_stamp_start = Utility::get_time_stamp();
    print STDERR "\nStarted at:  $time_stamp_start\n";

    my $hdfs_exe = $args_dict->{'hdfs_exe'};
    my $input    = "/$args_dict->{'random_id'}" . "_input";

    if ( system("$hdfs_exe dfs -test -d $input") == 0 ) {
        system("$hdfs_exe dfs -chmod -R 777 $input") == 0
          || warn "Error in changing the permission of $input folder\n";
    }
    else {
        system("$hdfs_exe dfs -mkdir $input") == 0
          || die
"Error could not create input directory $input folder on hdfs file system\n";
        system("$hdfs_exe dfs -chmod -R 777 $input") == 0
          || warn "Error in changing the permission of $input folder\n";
    }

    ### uploading the genome index archive
    if ( $args_dict->{'refindex_archive'} ne ""
        and exists $args_dict->{'refindex_archive'} )
    {
        unless ( $args_dict->{'refindex_archive'} =~ /^hdfs\:\/\/\//
            or $args_dict->{'refindex_archive'} =~ /^hdfs\:\/\// )
        {
            my $ref_arch_file_name =
              basename( $args_dict->{'refindex_archive'} );

            if (
                system("$hdfs_exe dfs -test -e $input/$ref_arch_file_name") ==
                0 )
            {
                system("$hdfs_exe dfs -rm -r $input/$ref_arch_file_name") == 0
                  || warn
"Error in deleting the $input/$ref_arch_file_name file on hdfs file system.\n";
                system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'refindex_archive'} $input/"
                  ) == 0
                  || warn
                  "Error in uploading the $args_dict->{'refindex_archive'}\n";
            }
            else {
                system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'refindex_archive'} $input/"
                  ) == 0
                  || warn
                  "Error in uploading the $args_dict->{'refindex_archive'}\n";
            }
            system("$hdfs_exe dfs -chmod -R 777 $input") == 0
              || warn "Error in changing the permission of $input folder\n";
        }
    }
    else {
        if (
            system("$hdfs_exe dfs -test -e $input/$args_dict->{'ref_arch'}") ==
            0 )
        {
            system("$hdfs_exe dfs -rm -r $input/$args_dict->{'ref_arch'}") == 0
              || warn
"Error in deleting the $input/$args_dict->{'ref_arch'} file on hdfs file system.\n";
            system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'output_directory'}/$args_dict->{'ref_arch'} $input/"
              ) == 0
              || warn "Error in uploading the $args_dict->{'ref_arch'}\n";
        }
        else {
            system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'output_directory'}/$args_dict->{'ref_arch'} $input/"
              ) == 0
              || warn "Error in uploading the $args_dict->{'ref_arch'}\n";
        }
        system("$hdfs_exe dfs -chmod -R 777 $input") == 0
          || warn "Error in changing the permission of $input folder\n";
    }
    if ( $args_dict->{'mappers_exe_archive'} ne ""
        and exists $args_dict->{'mappers_exe_archive'} )
    {
        unless ( $args_dict->{'mappers_exe_archive'} =~ /^hdfs\:\/\/\//
            or $args_dict->{'mappers_exe_archive'} =~ /^hdfs\:\/\// )
        {
            my $exec_arch_file_name =
              basename( $args_dict->{'mappers_exe_archive'} );

            if (
                system("$hdfs_exe dfs -test -e $input/$exec_arch_file_name") ==
                0 )
            {
                system("$hdfs_exe dfs -rm -r $input/$exec_arch_file_name") == 0
                  || warn
"Error in deleting the $input/$exec_arch_file_name file on hdfs file system.\n";
                system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'mappers_exe_archive'} $input/"
                  ) == 0
                  || warn
                  "Error in uploading the $args_dict->{'refindex_archive'}\n";
            }
            else {
                system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'mappers_exe_archive'} $input/"
                  ) == 0
                  || warn
"Error in uploading the $args_dict->{'mappers_exe_archive'}\n";
            }
            system("$hdfs_exe dfs -chmod -R 777 $input") == 0
              || warn "Error in changing the permission of $input folder\n";
        }
    }
    else {
        ### uploading the job executables
        if (
            system("$hdfs_exe dfs -test -e $input/$args_dict->{'exec_arch'}")
            == 0 )
        {
            system("$hdfs_exe dfs -rm -r $input/$args_dict->{'exec_arch'}") == 0
              || warn
"Error in deleting the $input/$args_dict->{'exec_arch'} file on hdfs file system.\n";
            system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'output_directory'}/$args_dict->{'exec_arch'} $input/"
              ) == 0
              || warn "Error in uploading the $args_dict->{'exec_arch'}\n";
        }
        else {
            system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -put $args_dict->{'output_directory'}/$args_dict->{'exec_arch'} $input/"
              ) == 0
              || warn "Error in uploading the $args_dict->{'exec_arch'}\n";
        }
    }
    system("$hdfs_exe dfs -chmod -R 777 $input") == 0
      || warn "Error in changing the permission of $input folder\n";

    system("$hdfs_exe dfs -chmod -R 777 $input") == 0
      || warn "Error in changing the permission of $input folder\n";

    ################### Time end #########################
    my $end_time       = time();
    my $execution_time = Utility::get_execution_time( $start_time, $end_time );
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $execution_time\n\n";
}

sub paired_end_data {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"}       = "fastq_paired_end";
    $args_dict->{"read_type"}         = "pe";
    $args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads";
    my $shell_script =
        $args_dict->{"output_directory"} . "/"
      . lc( $args_dict->{"mapper"} )
      . "_pe_hadoop.sh";
    $args_dict->{"shell_script"} = $shell_script;

    my $file_count = 0;
    if (
        -d "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
      )
    {
        $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
        );
    }

    my $hdfs_exe = $args_dict->{'hdfs_exe'};

    my $input       = "/$args_dict->{'random_id'}" . "_input";
    my $read_folder = "$input/$args_dict->{'read_folder'}";

    if ( $file_count > 0 ) {

        if ( system("$hdfs_exe dfs -test -d $read_folder") == 0 ) {
            system("$hdfs_exe dfs -rm -r $read_folder") == 0
              || warn
"Error in deleting the $read_folder folder on hdfs file system. $read_folder does not exists\n";
            system("$hdfs_exe dfs -mkdir $read_folder") == 0
              || die
"Error could not create input directory $read_folder folder on hdfs file system\n";
        }
        else {
            system("$hdfs_exe dfs -mkdir $read_folder") == 0
              || die
"Error could not create input directory $read_folder folder on hdfs file system\n";
        }
        print
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -copyFromLocal $args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}/* $read_folder/\n";
        system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -copyFromLocal $args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}/* $read_folder/"
          ) == 0
          || warn "Error in uploading the $args_dict->{'job_arch'}\n";
    }
}

sub single_end_data {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"}       = "fastq_single_end";
    $args_dict->{"read_type"}         = "se";
    $args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads";
    my $shell_script =
        $args_dict->{"output_directory"} . "/"
      . lc( $args_dict->{"mapper"} )
      . "_se_hadoop.sh";
    $args_dict->{"shell_script"} = $shell_script;

    my $file_count = 0;

    if (
        -d "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
      )
    {
        $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
        );
    }

    my $hdfs_exe = $args_dict->{'hdfs_exe'};

    my $input       = "/$args_dict->{'random_id'}" . "_input";
    my $read_folder = "$input/$args_dict->{'read_folder'}";

    if ( $file_count > 0 ) {
        if ( system("$hdfs_exe dfs -test -d $read_folder") == 0 ) {
            system("$hdfs_exe dfs -rm -r $read_folder") == 0
              || warn
"Error in deleting the $read_folder folder on hdfs file system. $read_folder does not exists\n";
            system("$hdfs_exe dfs -mkdir $read_folder") == 0
              || die
"Error could not create input directory $read_folder folder on hdfs file system\n";
        }
        else {
            system("$hdfs_exe dfs -mkdir $read_folder") == 0
              || die
"Error could not create input directory $read_folder folder on hdfs file system\n";
        }

        system(
"$hdfs_exe dfs -D dfs.block.size=$args_dict->{'block_size'} -copyFromLocal $args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}/* $read_folder/"
          ) == 0
          || warn "Error in uploading the $args_dict->{'job_arch'}\n";
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
