#!/usr/bin/perl -w
use strict;
use warnings;

package HadoopTrimming;
use Getopt::Long;
use POSIX q/strftime/;
use Cwd 'abs_path';
use FindBin qw/$RealBin/;
use lib "$RealBin/bin";
use Utility;

sub new {
    my $class = shift;
    my $self  = {};
    bless $self, $class;
    return $self;
}

sub start {
    my ( $self, $args_dict ) = @_;

    ################### Time start #########################
    my $start_time       = time();
    my $time_stamp_start = Utility::get_time_stamp();
    print STDERR "\nStarted at:  $time_stamp_start\n";

    ### Paired-end read trimming.
    $self->paired_end_trimming($args_dict);

    ### Single-end read trimming.
    $self->single_end_trimming($args_dict);

    ################### Time end #########################
    my $end_time       = time();
    my $execution_time = Utility::get_execution_time( $start_time, $end_time );
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $execution_time\n\n";

}

#sub delete_temp {
#	my ($args_dict) = @_;
#	print STDERR "=======================================================================\n";
#	print STDERR "Step6: Cleaning the temperory files and folders\n";
#	print STDERR "=======================================================================\n";
#
#	Utility::deletedir("$args_dict->{'output_directory'}/$args_dict->{'random_id'}");
#
#}

sub paired_end_trimming {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"} = "fastq_paired_end";
    $args_dict->{"read_type"}   = "pe";
    $args_dict->{"final_output_file"} =
      "DistMap_output_Paired_end_trimmed_reads";
    my $shell_script = $args_dict->{"output_directory"}
      . "/$args_dict->{'random_id'}/trim_pe_hadoop.sh";
    $args_dict->{"shell_script"} = $shell_script;

    my $file_count = 0;

    my $to_read_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}";
    if ( -d $to_read_dir ) {
        $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
        );
    }

    #my $output_string = `ls`;

    my $hdfs_input_folder =
      "/$args_dict->{'random_id'}" . "_input/fastq_paired_end";

    #my $cmd = "$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder | `wc -l`";

    my @cmd      = ();
    my $hdfs_exe = $args_dict->{"hdfs_exe"};
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
        $self->get_trim_command($args_dict);
        $self->write_hadoop_trimming_job($args_dict);
        system("sh $shell_script");
        if ( $args_dict->{"only_download_reads"} ) {
            $self->download_merge_trimmed_reads($args_dict);
        }
    }

}

sub single_end_trimming {
    my ( $self, $args_dict ) = @_;

    $args_dict->{"read_folder"} = "fastq_single_end";
    $args_dict->{"read_type"}   = "se";
    $args_dict->{"final_output_file"} =
      "DistMap_output_Single_end_trimmed_reads";
    my $shell_script = $args_dict->{"output_directory"}
      . "/$args_dict->{'random_id'}/trim_se_hadoop.sh";
    $args_dict->{"shell_script"} = $shell_script;

    my $file_count = 0;

    my $to_read_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}";
    if ( -d $to_read_dir ) {
        $file_count = $self->get_file_list(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}"
        );
    }

    my $hdfs_input_folder =
      "/$args_dict->{'random_id'}" . "_input/fastq_single_end";

    #my $cmd = "$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder | `wc -l`";
    my @cmd      = ();
    my $hdfs_exe = $args_dict->{"hdfs_exe"};
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
        $self->get_trim_command($args_dict);
        $self->write_hadoop_trimming_job($args_dict);
        system("sh $shell_script");
        if ( $args_dict->{"only_download_reads"} ) {
            $self->download_merge_trimmed_reads($args_dict);
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

sub get_trim_command {
    my ( $self, $args_dict ) = @_;

    my $script_current_file = abs_path($0);
    my ( $name, $path, $extension ) =
      File::Basename::fileparse( $script_current_file, '\..*' );
    my $script_current_directory = $path;
    $script_current_directory =~ s/\/$//;
    my $trim_command = "";

    ##########################################################################################
    # 				Hadoop input output folders

    my $input       = "/$args_dict->{'random_id'}" . "_input";
    my $output      = "/$args_dict->{'random_id'}" . "_output";
    my $read_folder = "$input/$args_dict->{'read_folder'}";

    #my $input_folder ="$read_folder/fastq";
    my $input_folder = "$read_folder";

    #my $archieve_folder = "$read_folder/archieve";
    my $archieve_folder = $input;

    my $output_folder = "$output/$args_dict->{'read_folder'}" . "_trimming";
    my $hadoop_exe    = $args_dict->{'hadoop_exe'};
    my $hdfs_exe      = $args_dict->{"hdfs_exe"};

    system("$hdfs_exe dfs -mkdir $output") == 0
      || warn
"Error could not create output directory $output folder on hdfs file system\n";
    system("$hdfs_exe dfs -chmod -R 777 $output") == 0
      || warn "Error in changing the permission of $output folder\n";
    system(
"$hdfs_exe dfs -chown -R $args_dict->{'username'}:$args_dict->{'groupname'} $output/"
      ) == 0
      || warn "Error in changing the ownership of $output folder\n";
    system(
"$hdfs_exe dfs -chown -R $args_dict->{'username'}:$args_dict->{'groupname'} $output_folder/"
      ) == 0
      || warn "Error in changing the ownership of $output_folder folder\n";

    $args_dict->{"trim_script_name"} = "cluster_trimming.pl";
    $trim_command =
"$args_dict->{'trim_script_name'} --output-dir $output_folder --trim-args '$args_dict->{trim_args}' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --exe-path $args_dict->{'extracted_execarch'}/bin/ReadTools.jar --verbose";

    $args_dict->{"cluster_trim_script_path"} =
      "$script_current_directory/bin/$args_dict->{'trim_script_name'}";
    $args_dict->{"utility_script_path"} =
      "$script_current_directory/bin/Utility.pm";
    $args_dict->{"input_folder"}    = $input_folder;
    $args_dict->{"output_folder"}   = $output_folder;
    $args_dict->{"archieve_folder"} = $archieve_folder;
    $args_dict->{"trim_command"}    = $trim_command;

    #$args_dict->{"trim_script_path"}
}

sub download_merge_trimmed_reads {
    my ( $self, $args_dict ) = @_;
    my $script_current_file = abs_path($0);
    my ( $name, $path, $extension ) =
      File::Basename::fileparse( $script_current_file, '\..*' );
    my $script_current_directory = $path;
    $script_current_directory =~ s/\/$//;
    my $merge_fastq_script =
      "$script_current_directory/bin/download_merge_fastq.pl";
    my $hadoop_exe = $args_dict->{'hadoop_exe'};
    my $hdfs_exe   = $args_dict->{"hdfs_exe"};

    my $output_file = $args_dict->{"output_directory"} . "/"
      . $args_dict->{"final_output_file"};
    my $command =
"perl $merge_fastq_script --output $output_file --hdfs-dir $args_dict->{'output_folder'} --hdfs $hdfs_exe --disable-zipped-output &";

    if ( $args_dict->{"nozip"} ) {
        $command =
"perl $merge_fastq_script --output $output_file --hdfs-dir $args_dict->{'output_folder'} --hdfs $hdfs_exe --disable-zipped-output &";
        print "command: $command\n";
    }

    system($command);

}

sub write_hadoop_trimming_job {
    my ( $self, $args_dict ) = @_;

    my $shell_script = "";
    $shell_script = $args_dict->{"shell_script"};
    open my $ofh, ">$shell_script"
      or die "Could not open $shell_script for write $!";

    print $ofh "#!/bin/sh\n\n";
    print $ofh "streaming_home=\"$args_dict->{'streaming_jar'}\"\n";
    print $ofh "hadoop_home=\"$args_dict->{'hadoop_exe'}\"\n\n";
    print $ofh "hdfs_home=\"$args_dict->{'hdfs_exe'}\"\n\n";
    print $ofh "input_folder=\"$args_dict->{'input_folder'}\"\n";
    print $ofh "output_folder=\"$args_dict->{'output_folder'}/\"\n";
    print $ofh "job=\"$args_dict->{'job_desc'}"
      . "_Trimming_"
      . "$args_dict->{'read_folder'}\"\n\n";

    print $ofh "time\n";
    print $ofh "\n\n";
    print $ofh
"echo \"===============================================================\"\n";
    print $ofh "echo \"Step3: Short read trimming on cluster started\"\n";
    print $ofh
"echo \"===============================================================\"\n";
    print $ofh "time\n";
    print $ofh "time \$hdfs_home dfs -rm -r \$output_folder\n\n";
    print $ofh "time \$hadoop_home  jar \$streaming_home \\\n";

#print $ofh "-archives 'hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'job_arch'}"."#"."$args_dict->{'extracted_arch'}' \\\n";

    if ( $args_dict->{"upload_index"} ) {
        print $ofh
"-archives 'hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'exec_arch'}"
          . "#"
          . "$args_dict->{'extracted_execarch'},hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'ref_arch'}"
          . "#"
          . "$args_dict->{'extracted_refarch'}' \\\n";
    }
    else {
        print $ofh
"-archives 'hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'exec_arch'}"
          . "#"
          . "$args_dict->{'extracted_execarch'},$args_dict->{'refindex_archive'}"
          . "#"
          . "$args_dict->{'extracted_refarch'}' \\\n";

    }

    print $ofh "-D mapreduce.tasktracker.map.tasks.maximum=1 \\\n";
    print $ofh "-D mapred.max.maps.per.node=1 \\\n";
    print $ofh "-D dfs.blocksize=$args_dict->{'block_size'} \\\n";

    print $ofh "-D mapreduce.map.memory.mb=3072 \\\n";
    print $ofh "-D mapreduce.map.java.opts=-Xmx2304m \\\n";
    print $ofh "-D mapred.child.java.opts=-Xmx2304m \\\n";
    print $ofh "-D mapreduce.reduce.memory.mb=3072 \\\n";
    print $ofh "-D mapreduce.reduce.java.opts=-Xmx2304m \\\n";

    #print $ofh "-D mapreduce.map.memory.mb=4096 \\\n";
    #print $ofh "-D mapreduce.map.java.opts=-Xmx3072m \\\n";
    #print $ofh "-D mapred.child.java.opts=-Xmx3072m \\\n";
    #print $ofh "-D mapreduce.reduce.memory.mb=4096 \\\n";
    #print $ofh "-D mapreduce.reduce.java.opts=-Xmx3072m \\\n";

    #print $ofh "-D mapreduce.map.memory.mb=6144 \\\n";
    #print $ofh "-D mapreduce.map.java.opts=-Xmx4608m \\\n";
    print $ofh "-D mapreduce.output.fileoutputformat.compress.type=RECORD \\\n";

    print $ofh
"-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec \\\n";
    print $ofh "-D mapreduce.map.output.compress=true \\\n";
    print $ofh "-D mapreduce.output.fileoutputformat.compress=true \\\n";
    print $ofh
"-D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.BZip2Codec \\\n";

    print $ofh "-D $args_dict->{'job_priority'} \\\n";
    print $ofh "-D $args_dict->{'queue_name'} \\\n";
    print $ofh "-D mapreduce.job.reduces=0 \\\n";
    print $ofh "-D mapreduce.job.name=\"\$job\" \\\n";    #mapreduce.job.tags
    print $ofh
"-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \\\n";
    print $ofh "-D stream.num.map.output.key.fields=4 \\\n";
    print $ofh "-D mapreduce.partition.keypartitioner.options=-k1,4 \\\n";
    print $ofh "-D mapreduce.partition.keycomparator.options=-k1,4 \\\n";
    print $ofh
      "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\\n";
    print $ofh "-input \"\$input_folder\" \\\n";
    print $ofh "-output \"\$output_folder\" \\\n";

    #print $ofh "-D mapred.child.java.opts=-Xmx4608m \\\n";
    #print $ofh "-D mapreduce.reduce.memory.mb=9216 \\\n";
    #print $ofh "-D mapreduce.reduce.java.opts=-Xmx6912m \\\n";

    print $ofh "-mapper \"$args_dict->{'trim_command'}\" \\\n";
    print $ofh "-file '$args_dict->{'cluster_trim_script_path'}' \\\n";
    print $ofh "-file '$args_dict->{'trim_script_path'}' \\\n";
    print $ofh "-file '$args_dict->{'utility_script_path'}'";
    close $ofh;
}

1;
