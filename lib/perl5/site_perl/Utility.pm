#!/usr/bin/env perl
use strict;
use warnings;
package Utility;
use File::Basename;
use File::Path qw(make_path remove_tree);
use POSIX ":sys_wait_h";




sub check_hadoop {

    my $args_dict = $_[0];
    my $hadoop_home = $args_dict->{"hadoop_home"};
    if ($hadoop_home =~ /\/$/) {
        $hadoop_home =~ s/\/$//;
    }

    my $streaming_home = "$hadoop_home/share/hadoop/tools/lib";

    my $streaming_jar="";
    my $hadoop_exe="";
    my $hdfs_exe="";
    my $mapred_exe="";
    my $yarn_exe="";

    opendir(DIR, "$streaming_home") || die "Can't opendir $streaming_home: $!\n";
    my @file_list = readdir(DIR);
    closedir(DIR);

    foreach my $file (@file_list) {
        if ($file =~ /streaming/i) {
            $streaming_jar=$file;
        }
    }

    $streaming_jar = "$streaming_home/$streaming_jar";
    $hadoop_exe = "$hadoop_home/bin/hadoop";
    $hdfs_exe="$hadoop_home/bin/hdfs";
    $mapred_exe="$hadoop_home/bin/mapred";
    $yarn_exe="$hadoop_home/bin/yarn";

    ## Check hadoop executables
    unless ((-e $hadoop_exe) && (-x $hadoop_exe)) {
	print STDERR "\tHADOOP executable $hadoop_exe does not exists.\n";
	exit(1);
    }
    unless (-x $hadoop_exe) {
	system("chmod -R +x $hadoop_exe");
    }

    ## Check hdfs executables
    unless ((-e $hdfs_exe) && (-x $hdfs_exe)) {
	print STDERR "\tHDFS executable $hdfs_exe does not exists.\n";
	exit(1);
    }
    unless (-x $hdfs_exe) {
	system("chmod -R +x $hdfs_exe");
    }

    unless (-e $streaming_jar) {
	print STDERR "\tHADOOP streaming jar file: $streaming_jar does not exists\n";

	exit(1);
    }
    $args_dict->{"hadoop_exe"} = $hadoop_exe;
    $args_dict->{"hdfs_exe"} = $hdfs_exe;
    $args_dict->{"mapred_exe"} = $mapred_exe;
    $args_dict->{"yarn_exe"} = $yarn_exe;
    $args_dict->{"streaming_jar"} = $streaming_jar;

}

sub create_dir {
    my $args_dict = $_[0];
    my $output_dir = $args_dict->{"output_directory"};

    createdir($output_dir);

    my $local_home = "distmap/".basename($output_dir);
    my $hdfs_home = "user/".$args_dict->{"username"}."/".$local_home;
    my $fastq_dir="fastq";
    my $fastq_dir_pe="fastq_paired_end";
    my $fastq_dir_se="fastq_single_end";
    my $input_fasta_dir = "input_fasta_dir";
    my $ref_dir="ref";
    my $tmp_dir="tmp";
    my $tmp_dir_pe="tmp_paired_end";
    my $tmp_dir_se="tmp_single_end";
    my $bin_dir="bin";

	createdir("$output_dir/$local_home/$fastq_dir_pe");
	createdir("$output_dir/$local_home/$fastq_dir_se");
    createdir("$output_dir/$ref_dir");
    createdir("$output_dir/$bin_dir");

   $args_dict->{"local_home"} = $local_home;
   $args_dict->{"hdfs_home"} = $hdfs_home;
   $args_dict->{"fastq_dir_pe"} = $fastq_dir_pe;
   $args_dict->{"fastq_dir_se"} = $fastq_dir_se;
   $args_dict->{"input_fasta_dir"} = $input_fasta_dir;
   $args_dict->{"ref_dir"} = $ref_dir;
   #$args_dict->{"tmp_dir_pe"} = $tmp_dir_pe;
   $args_dict->{"bin_dir"} = $bin_dir;

}


sub createdir {
    my ($dir) = @_;
    unless (-d "$dir") {
        make_path($dir, { chmod => 0755 }) || die "Could not create Directory $dir $!\n";
    }
}

sub deletedir {

    my ($dir) = @_;
    if (-d "$dir") { remove_tree("$dir") || die "Could not delete Directory $dir $!\n"; }

}

# This function generates random strings of a given length
sub generate_random_string {
    my $length_of_randomstring=10;

    my @chars=('a'..'z','A'..'Z','0'..'9','_');
    my $random_string;
    foreach (1..$length_of_randomstring) {
	$random_string.=$chars[rand @chars];
    }
	return $random_string;
}

sub check_executbles {
    my ($file) = @_;
    my ( $name, $path, $extension ) = File::Basename::fileparse ( $file, '\..*' );

}


sub runCommand {
	my ($cmd, $shortname) = @_;
	print STDERR "$cmd\n";
	my $f = fork();
	if($f == 0) {
		# Run the command, echoing its stdout to our stdout
		open(CMD, "$cmd |");
		while(<CMD>) { print $_; }
		close(CMD);
		# Check its exitlevel
		my $ret = $?;
		# Write its exitlevel to a file.
		open(OUT, ">.Utility.pm.$$") || die "Could not open .Utility.pm.$$ for writing\n";
		print OUT "$ret\n";
		close(OUT);
		exit $ret;
	}
	#print STDERR "runCommand: Child's PID is $f\n";
	my $ret;
	my $cnt = 0;
	while(1) {
		$ret = waitpid(-1, &WNOHANG);
		last if $ret == $f;
		sleep (5);
		my $secs = ++$cnt * 5;
		print STDERR "Waiting for $shortname (it's been $secs secs)...\n";
	}
	my $lev = int(`cat .Utility.pm.$ret`);
	unlink(".Utility.pm.$ret");
	return $lev;
}



sub get_time_stamp {

    my @weekday = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday");
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);

    $year = $year + 1900;
    $mon += 1;
    if (length($mday)<2) {
        $mday = "0$mday";
    }

    if (length($mon)<2) {
        $mon = "0$mon";
    }

    my $time_stamp = "$mday/$mon/$year $hour:$min:$sec $weekday[$wday]";
    #print "Formated time = $mday/$mon/$year $hour:$min:$sec $weekday[$wday]\n";
    return $time_stamp;
}


sub get_executation_time {
    my ($start_time,$end_time) = @_;
    my $duration = ($end_time-$start_time)+1;

    my ($seconds, $minutes, $hour, $dayOfMonth, $month, $year, $dayOfWeek, $dayOfYear) = localtime($duration);
    $hour--;
    if (length($hour)<2) {
        $hour = "0$hour";
    }

    if (length($minutes)<2) {
        $minutes = "0$minutes";
    }
    if (length($seconds)<2) {
        $seconds = "0$seconds";
    }

    my $executation_time = "$hour:$minutes:$seconds";
    #print "$hour:$minutes:$seconds\n";
    return $executation_time;

}


sub check_mapper {
	my ($args_dict, $mapper,$mapper_args,$mapper_path) = @_;

	if (!exists$args_dict->{'number_of_processors'}) {
	    $args_dict->{'number_of_processors'} = 3;
	}
	if ($mapper =~ /bwa/i) {
		if ($args_dict->{"mapper_args"} =~ /\-t/) {
		    $args_dict->{"mapper_args"} =~ s/\-t\s*\d+\s*//;
		    $mapper_args = $mapper_args." -t $args_dict->{'number_of_processors'}";
		}
		else {
		    $mapper_args = $mapper_args." -t $args_dict->{'number_of_processors'}";
		}
	}
	elsif ($mapper =~ /gsnap/i) {

		if ($args_dict->{"mapper_args"} =~ /\-t/) {

		    $args_dict->{"mapper_args"} =~ s/\-t\s*\d+\s*//;
		     $mapper_args = $mapper_args." -t $args_dict->{'number_of_processors'}";

		}
		elsif ($args_dict->{"mapper_args"} =~ /\-\-nthreads/) {

		    $args_dict->{"mapper_args"} =~ s/\-\-nthreads\s*\d+\s*//;
		     $mapper_args = $mapper_args." --nthreads $args_dict->{'number_of_processors'}";
		}
		else {
		    $mapper_args = $mapper_args." -t $args_dict->{'number_of_processors'}";
		}
	}
	elsif ($mapper =~ /tophat/i) {
		if ($args_dict->{"mapper_args"} =~ /\-p/) {

		    $args_dict->{"mapper_args"} =~ s/\-p\s*\d+\s*//;
		     $mapper_args = $mapper_args." -p $args_dict->{'number_of_processors'}";

		}
		elsif ($args_dict->{"mapper_args"} =~ /\-\-num\-threads/) {

		    $args_dict->{"mapper_args"} =~ s/\-\-num\-threads\s*\d+\s*//;
		     $mapper_args = $mapper_args." --num-threads $args_dict->{'number_of_processors'}";
		}
		else {
		    $mapper_args = $mapper_args." -p $args_dict->{'number_of_processors'}";
		}
	}
	elsif ($mapper =~ /bowtie/i) {
		if ($args_dict->{"mapper_args"} =~ /\-p/) {

		    $args_dict->{"mapper_args"} =~ s/\-p\s*\d+\s*//;
		     $mapper_args = $mapper_args." -p $args_dict->{'number_of_processors'}";

		}
		elsif ($args_dict->{"mapper_args"} =~ /\-\-threads/) {

		    $args_dict->{"mapper_args"} =~ s/\-\-threads\s*\d+\s*//;
		     $mapper_args = $mapper_args." --threads $args_dict->{'number_of_processors'}";
		}
		else {
		    $mapper_args = $mapper_args." -p $args_dict->{'number_of_processors'}";
		}
	}
	elsif ($mapper =~ /soap/i) {
		if ($args_dict->{"mapper_args"} =~ /\-p/) {
		    $args_dict->{"mapper_args"} =~ s/\-p\s*\d+\s*//;
		    $mapper_args = $mapper_args." -p $args_dict->{'number_of_processors'}";
		}
		else {
		    $mapper_args = $mapper_args." -p $args_dict->{'number_of_processors'}";
		}
	}
	elsif ($mapper =~ /novo/i) {
		if ($args_dict->{"mapper_args"} =~ /\-t/) {
		    $args_dict->{"mapper_args"} =~ s/\-t\s*\d+\s*//;
		    $mapper_args = $mapper_args." -t 1";
		}
		else {
		    $mapper_args = $mapper_args."";
		}
	}
	elsif ($mapper =~ /ngm/i) {
		if ($args_dict->{"mapper_args"} =~ /\-t/) {
		    $args_dict->{"mapper_args"} =~ s/\-t\s*\d+\s*//;
		    $mapper_args = $mapper_args." -t $args_dict->{'number_of_processors'}";
		}
		else {
		    $mapper_args = $mapper_args."";
		}
	}

	elsif ($mapper =~ /exonerate/i) {

	}
	else {
		print STDERR "\n\tERROR: --mapper $mapper not supported in DistMap\n";
		print STDERR "$args_dict->{'usage'}\n";
		#exit(1);
	}

	unless (-e $mapper_path) {
	    print STDERR "\n\tERROR: --mapper-path $mapper_path file does not exists\n";
	    print STDERR "$args_dict->{'usage'}\n";
	    #exit(1);
	}

	if ($mapper !~ /exonerate/i) {
		if (-e $args_dict->{"picard_jar"}) {
			print STDERR "\n\tWARNING: --picard-jar $args_dict->{'picard_jar'} does not exists.\n";
			if (-e $args_dict->{"picard_mergesamfiles_jar"} or -e $args_dict->{"picard_sortsam_jar"}) {
				print STDERR "\n\tERROR: --picard-mergesamfiles-jar $args_dict->{'picard_mergesamfiles_jar'} and/or --picard-sortsam-jar $args_dict->{'picard_sortsam_jar'} file does not exists\n";
				print STDERR "$args_dict->{'usage'}\n";
				exit(1)
			} else {
				print STDERR "\n\tWARNING: using DEPRECATED options --picard-mergesamfiles-jar and --picard-sortsam-jar.\n";
			}
		}
	}

	#$args_dict->{"mapper_args"} = $mapper_args;

	if ($args_dict->{"mapper_args"} =~ // ) {

	}

    return ($mapper,$mapper_args,$mapper_path);
}






sub run_whole_pipeline {
	my ($args_dict) = @_;

	my $genomeindex_object = GenomeIndex->new();
	$genomeindex_object->start($args_dict);

	my $dataprocess_object = DataProcess->new();
	$dataprocess_object->start($args_dict);

	my $dataupload_object = DataUpload->new();
	$dataupload_object->start($args_dict);

	if (!$args_dict->{"no_trim"})  {
		print "Trimming is now done on upload to the HDFS.";
		# print "trim: ", $args_dict->{"no_trim"}," | ", $args_dict->{"only_trim"},"\n";
		# my $trimming_object = HadoopTrimming->new();
		# $trimming_object->start($args_dict);
	}

	my $mapping_object = HadoopMapping->new();
	$mapping_object->start($args_dict);

	my $datadownloadandmerge_object = DataDownloadAndMerge->new();
	$datadownloadandmerge_object->start($args_dict);
}


sub data_cleanup {
	my ($args_dict) = @_;
	my $step_hash = {};
	if ($args_dict->{"only_delete_temp"}) {
		my $datacleanup_object = DataCleanup->new();
		#$datacleanup_object->clean($args_dict);
		$step_hash->{6} = $datacleanup_object;
	}
	return $step_hash;
}

sub get_steps {
	my ($args_dict) = @_;
	my $step_hash = {};

	if ($args_dict->{"only_index"}) {
	    my $genomeindex_object = GenomeIndex->new();
	    $step_hash->{1} = $genomeindex_object;
	}

	if ($args_dict->{"only_process"}) {
		my $dataprocess_object = DataProcess->new();
		#$dataprocess_object->process($args_dict);
		$step_hash->{2} = $dataprocess_object;
	}

	if ($args_dict->{"only_hdfs_upload"}) {

		my $dataupload_object = DataUpload->new();
		#$dataupload_object->upload($args_dict);
		$step_hash->{3} = $dataupload_object;
	}

	if ($args_dict->{"only_trim"}) {
		print "WARNING: trimming is now done on upload to the HDFS (--only-trim does not have any effect)";
		# my $trimming_object = HadoopTrimming->new();

		# $trimming_object->trim($args_dict);
		# $step_hash->{4} = $trimming_object;
	}

	if ($args_dict->{"only_map"}) {

		my $mapping_object = HadoopMapping->new();
		#$mapping_object->map($args_dict);
		$step_hash->{5} = $mapping_object;
	}

	if ( $args_dict->{"only_hdfs_download"} or $args_dict->{"only_merge"} ) {
		print "--only-hdfs-download now merges also the data in the same step";
		if ( $args_dict->{"only_merge"} ) {
			print "WARNING: --only-merge is deprecated. Use --only-hdfs-download instead";
		}
		my $datadownloadandmerge_object = DataDownloadAndMerge->new();
		$step_hash->{6} = $datadownloadandmerge_object;
	}

	if ($args_dict->{"only_download_reads"}) {
		die("OBSOLETE: --only-download-trimmed-reads is not supported anymore (and has not work for a while). Use ReadTools for trimming if you need the raw reads, or convert output from mapping using Picard/ReadTools");
	    # my $read_download_object = DownloadTrimmedRead->new();
	    # $step_hash->{8} = $read_download_object;

	}
	if ($args_dict->{"only_delete_temp"}) {
		my $datacleanup_object = DataCleanup->new();
		#$datacleanup_object->clean($args_dict);
		$step_hash->{9} = $datacleanup_object;
	}

	return $step_hash;

}


sub check_genome_index {
    my ($args_dict,$step_hash) = @_;

    $args_dict->{"upload_index"} = 1;

    if ($args_dict->{'refindex_archive'} ne "" and exists$args_dict->{'refindex_archive'}) {
	if ($args_dict->{'refindex_archive'} =~ /^hdfs\:\/\/\//) {
	    $args_dict->{"upload_index"}=0;
	}
	elsif ($args_dict->{'refindex_archive'} =~ /^hdfs\:\/\//) {
	    $args_dict->{'refindex_archive'} =~ s/hdfs\:\/\//hdfs\:\/\/\//;
	    $args_dict->{"upload_index"}=0;
	}
	else {
	    if ($args_dict->{'refindex_archive'} =~ /.zip$/) {

		my $file_count = `unzip -l $args_dict->{"refindex_archive"} | wc -l`;
		$file_count =~ s/\s*//;
		chomp($file_count);
		if ($file_count>1) {
		    $args_dict->{"upload_index"}=0;
		}
		else {
		    print STDERR "WARNING: This archive does not exists!!! going to index the reference genome fasta file\n";
		    $args_dict->{"upload_index"}=1;
		}
		#print $args_dict->{"upload_index"}, "\n";
		#exit();

	    }
	    else {
		if (system("tar -tvf $args_dict->{'refindex_archive'}")!=0) {
		    print STDERR "WARNING: This archive does not exists!!! going to index the reference genome fasta file\n";
		    $args_dict->{"upload_index"}=1;
		}
		else {
		    $args_dict->{"upload_index"}=0;
		}
	    }

	}

    }
    else {

	$args_dict->{"upload_index"}=1;
    }


}



1;
