#!/usr/bin/env perl -w
use strict;
use warnings;
package DataMerge;
use FindBin qw/$RealBin/;
use lib "$RealBin/../lib/perl5/site_perl";
use Utility;
use File::Basename;
use File::stat;
use Cwd;
use Cwd 'abs_path';


sub new {
	die("OBSOLETE: DataMerge::new shouldn't be called. Use DataDownloadAnMerge instead");
	my $class=shift;
	my $self = {};
	bless $self, $class;
	return $self;
}


sub start {
	die("OBSOLETE: DataMerge::start shouldn't be called. Use DataDownloadAnMerge instead");
    my ($self,$args_dict) = @_;

    print STDERR "=======================================================================\n";
    print STDERR "Step5: Merging all SAM files and writing final output\n";
    print STDERR "=======================================================================\n";

    ################### Time start #########################
    my $start_time = time();
    my $time_stamp_start = Utility::get_time_stamp();
    print STDERR "\nStarted at:  $time_stamp_start\n";

    ### Paired-end data upload
    $self->paired_end_data($args_dict);

    ### Single-end data upload
    $self->single_end_data($args_dict);

    ################### Time end #########################
    my $end_time = time();
    my $executation_time = Utility::get_executation_time($start_time,$end_time);
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $executation_time\n\n";

}


sub compare_fastq_bam_reads {
	die("OBSOLETE: DataMerge::compare_fastq_bam_reads shouldn't be called. Use DataDownloadAnMerge instead");
	my ($self,$args_dict,$fastq_dir,$final_output_file,$output_dir) = @_;

	my $fastq_file_std = "$output_dir/fastq_count1";
	my $mapped_file_std = "$output_dir/mapped_count1";



	my ( $name, $path, $extension ) = File::Basename::fileparse ( abs_path($0), '\..*' );


	my $output =  "/$args_dict->{'random_id'}"."_output";
	my $output_folder_trim = "$output/$args_dict->{'read_folder'}"."_trimming";
	my $hadoop_exe = $args_dict->{'hadoop_exe'};
	$args_dict->{"final_output_file"} = "DistMap_output_Paired_end_trimmed_reads";
	my $trimmed_output_file = $args_dict->{"output_directory"}."/".$args_dict->{"final_output_file"};

	$args_dict->{"output_folder"} = $output_folder_trim;

	if ($args_dict->{"trimming_flag"}) {
		my $temp_file = $trimmed_output_file."_temp";
		my $download_command = "$hadoop_exe dfs -getmerge $output_folder_trim $temp_file";
		print "D1: $download_command\n";
		system($download_command);


		my $command = "wc -l $temp_file > $fastq_file_std";
		system($command);

		#$self->download_merge_trimmed_reads($args_dict);

		if (-e $temp_file) {
			system("rm -r $temp_file");
		}

	}
	elsif ((system("$hadoop_exe dfs -test -d $output_folder_trim")==0)) {
		my $temp_file = $trimmed_output_file."_temp";
		my $download_command = "$hadoop_exe dfs -getmerge $output_folder_trim $temp_file";
		print "D2: $download_command\n";
		system($download_command);

		my $command = "wc -l $temp_file > $fastq_file_std";

		system($command);

		#$self->download_merge_trimmed_reads($args_dict);

		if (-e $temp_file) {
			system("rm -r $temp_file");
		}

	}
	else {

		my @files = ();
		opendir(DIR, $fastq_dir) || die("Cannot open directory");
		@files= readdir(DIR);
		closedir(DIR);


		if (-e $fastq_file_std) {
			system("rm -r $fastq_file_std");
		}



		foreach my $file (@files) {
			unless ($file =~ /^\./) {
				my $file_path = "$fastq_dir/$file";
				my $command = "wc -l $file_path >> $fastq_file_std";
				system($command);

			}
		}
	}

	exit();
	
	## TODO - this code is not really used anywhere: the exit before would end the subroutine and in addition compare_fastq_bam_reads
	## TODO - is not really used

	## TODO - this use to be done with samtools instead of picard, but it might be completely unnecessary with ReadTools download
	## TODO - the picard.jar is supposed to be in the $path the same as samtools was (should be extracted in the same way)
	## TODO - samtools was hardcoded to be in "$path."executables/samtools-0.1.19/samtools" and maybe there is a component in the args map
	## TODO - which gets the picard.jar provided (or command in the PATH)
	my $picard_jar = $path."picard.jar";
	## TODO - this was "$samtools index $final_output_file" instead, but picard should provide the same result
	my $command1 = "java -Xmx8g -Dsnappy.disable=true -jar $picard_jar BuildBamIndex I=$final_output_file VALIDATION_STRINGENCY=SILENT";
	## TODO - this was "$samtools idxstats $final_output_file | awk '{i+=\$3+\$4} END {print i}' > $mapped_file_std" instead
	## TODO - picard has a slightly different format, which requires chaning the awk command
	my $command2 = "java -Xmx8g -Dsnappy.disable=true -jar $picard_jar BamIndexStats I=$final_output_file | awk '{i+=\$5+\$7} END {print i}' > $mapped_file_std";

	print "$command1\n$command2\n";
	system($command1);
	system($command2);

	open (my $fh1,"<".$fastq_file_std) or die "could not open $_";
	open (my $fh2,"<".$mapped_file_std) or die "could not open $_";
	#open (my $ofh,">".$output_file) or die "could not open $_";

	my $total_reads = 0;
	while(my $line=<$fh1>) {
		chomp($line);

		$line =~ s/^\s*//g;
		$line =~ s/\s*$//g;
		print "$line\n";

		my @col = split(" ",$line);
		#print "@col\n";
		my $read_count = $col[0];
		$read_count =~ s/^\s*//g;
		$read_count =~ s/\s*$//g;
		$total_reads = $total_reads+($read_count*2);

	}

	close($fh1);


	my $mapped_reads = 0;
	while(my $line=<$fh2>) {
		chomp($line);

		$line =~ s/^\s*//g;
		$line =~ s/\s*$//g;
		#print "LINE:$line\n";
		my @col = split(" ",$line);
		$mapped_reads = $col[0];
		$mapped_reads =~ s/^\s*//g;
		$mapped_reads =~ s/\s*$//g;

	}

	close($fh2);

	#print "$total_reads|$mapped_reads\n";

	my $missing_reads = 0;
	$missing_reads = $mapped_reads-$total_reads;
	if ($missing_reads>0) {
		print STDERR "\n\tERROR: missing_reads reads are missing from BAM file\n\tERROR: Fastq file read count: $total_reads and BAM file read count: $mapped_reads\n";
		print STDERR "\tERROR: Run command with --only-hdfs-download --only-merge two flags\n\n";

		my $command = "rm -r $final_output_file"."*";
		system($command);

	}

}


sub download_merge_trimmed_reads {
	die("OBSOLETE: DataMerge::download_merge_trimmed_reads shouldn't be called. Use DataDownloadAnMerge instead");
	my ($self,$args_dict) = @_;
	my $script_current_file = abs_path($0);
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $script_current_file, '\..*' );
	my $script_current_directory = $path;
	$script_current_directory =~ s/\/$//;
	my $merge_fastq_script = "$script_current_directory/bin/download_merge_fastq.pl";
	my $hadoop_exe = $args_dict->{'hadoop_exe'};
	#my $hdfs_exe = $args_dict->{"hdfs_exe"};

	my $output_file = $args_dict->{"output_directory"}."/".$args_dict->{"final_output_file"};
	##my $command = "perl $merge_fastq_script --output $output_file --hdfs-dir $args_dict->{'output_folder'} --hdfs $hadoop_exe &";

	if ($args_dict->{"nozip"}) {
		#$command = "perl $merge_fastq_script --output $output_file --hdfs-dir $args_dict->{'output_folder'} --hdfs $hadoop_exe --disable-zipped-output &";

	}
	my $command = "perl $merge_fastq_script --output $output_file --hdfs-dir $args_dict->{'output_folder'} --hdfs $hadoop_exe --disable-zipped-output &";

	print "command: $command\n";
	system($command);

}



sub paired_end_data {
	die("OBSOLETE: DataMerge::paired_end_data shouldn't be called. Use DataDownloadAnMerge instead");
	my ($self,$args_dict) = @_;

	$args_dict->{"read_folder"} = "fastq_paired_end";
	$args_dict->{"read_type"} = "pe";
	#$args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads";
	#$args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping";

	my $hdfs_input_folder = "/$args_dict->{'random_id'}"."_input/fastq_paired_end";
	#my $cmd = "$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder | `wc -l`";
	my @cmd =();
	my $hdfs_exe = $args_dict->{"hdfs_exe"};
	if ((system("$hdfs_exe dfs -test -d $hdfs_input_folder")==0)) {
		@cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
	}

	#my @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
	my $hdfs_file_count = 0;

	if (scalar(@cmd)>1) {
		$hdfs_file_count = $cmd[0];
		$hdfs_file_count =~ s/\D//g;
	}


	my $i=0;
	foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
		my $mapper_path = $args_dict->{"mapper_path"}->[$i];
		my $mapper_args = $args_dict->{"mapper_args"}->[$i];

		$args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping_".lc($mapper_name);
		$args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads_".lc($mapper_name);

		my $file_count=0;
		my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
		if (-d $to_read_dir) {
		    $file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}");
		}


		my $output_format = $args_dict->{"output_format"};
		my $output_file = "";

		if ($output_format =~ /sam/i) {
		    $output_file = $args_dict->{"final_output_file"}.".sam";
		}
		else {
		    $output_file = $args_dict->{"final_output_file"}.".bam";
		}







		if ($file_count>0 or $hdfs_file_count>0) {
			if ($mapper_name =~ /tophat/i) {
				$self->tophat_output($args_dict,$mapper_name);
			}
			else {
				$self->bwa_output($args_dict,$mapper_name);
			}

			my $output_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}";
			my $bam_output_file = "$args_dict->{'output_directory'}/$output_file";
		}

		$i++;
	}



}


sub single_end_data {
	die("OBSOLETE: DataMerge::single_end_data shouldn't be called. Use DataDownloadAnMerge instead");
    my ($self,$args_dict) = @_;

    $args_dict->{"read_folder"} = "fastq_single_end";
    $args_dict->{"read_type"} = "se";
    #$args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads";
    #$args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping";


	my $hdfs_input_folder = "/$args_dict->{'random_id'}"."_input/fastq_single_end";
	#my $cmd = "$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder | `wc -l`";
	my @cmd =();
	my $hdfs_exe = $args_dict->{"hdfs_exe"};
	if ((system("$hdfs_exe dfs -test -d $hdfs_input_folder")==0)) {
		@cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
	}

	#my @cmd = `$args_dict->{'hdfs_exe'} dfs -ls $hdfs_input_folder`;
	my $hdfs_file_count = 0;

	if (scalar(@cmd)>1) {
		$hdfs_file_count = $cmd[0];
		$hdfs_file_count =~ s/\D//g;
	}


	my $i=0;
	foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
		my $mapper_path = $args_dict->{"mapper_path"}->[$i];
		my $mapper_args = $args_dict->{"mapper_args"}->[$i];

		$args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping_".lc($mapper_name);
		$args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads_".lc($mapper_name);

		my $file_count=0;
		my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
		if (-d $to_read_dir) {
		    $file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}");
		}


		my $output_format = $args_dict->{"output_format"};
		my $output_file = "";

		if ($output_format =~ /sam/i) {
		    $output_file = $args_dict->{"final_output_file"}.".sam";
		}
		else {
		    $output_file = $args_dict->{"final_output_file"}.".bam";
		}

		if ($file_count>0 or $hdfs_file_count>0) {
			if ($mapper_name =~ /tophat/i) {
				$self->tophat_output($args_dict,$mapper_name);
			}
			else {
				$self->bwa_output($args_dict,$mapper_name);
			}

			my $output_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}";
			my $bam_output_file = "$args_dict->{'output_directory'}/$output_file";
		}

		$i++;
	}



    my $file_count=0;
    my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
    if (-d $to_read_dir) {
	$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}");
    }

    if ($file_count>0 or $hdfs_file_count>0) {


	my $i=0;
	foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
		my $mapper_path = $args_dict->{"mapper_path"}->[$i];
		my $mapper_args = $args_dict->{"mapper_args"}->[$i];

		$args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping_".lc($mapper_name);
		$args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads_".lc($mapper_name);

		if ($args_dict->{"mapper"} =~ /tophat/i) {
		    $self->tophat_output($args_dict);
		}
		elsif ($args_dict->{"mapper"} =~ /exonerate/i) {
		    $self->exonerate_output($args_dict,$mapper_name);
		}
		else {
		    $self->bwa_output($args_dict,$mapper_name);
		}
	}

    }

}


sub get_file_list {
	die("OBSOLETE: DataMerge::get_file_list shouldn't be called. Use DataDownloadAnMerge instead");
    my ($self,$dir) = @_;
    opendir(DIR, $dir) || die("Cannot open directory");
    my @files= readdir(DIR);
    closedir(DIR);

    my @f = ();
    foreach my $file (@files) {
    	unless ($file eq "." || $file eq ".." || $file =~ /^\_/ || $file eq "MD5" || $file eq ".DS_Store") {
			my $file_size=0;
			$file_size = stat("$dir/$file")->size;

			if ($file_size>1) {

				push(@f,$file);
			}
    	}
    }

	#if ($file =~ /^fastq/i) {
	#    push(@f,$file);
	#}
    #}

    return scalar(@f);

}

sub bwa_output {
	die("OBSOLETE: DataMerge::bwa_output shouldn't be called. Use DataDownloadAnMerge instead");
    my ($self,$args_dict, $mapper_name) = @_;

    my $temp_output_folder = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";

    my $picard_temp = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/".$mapper_name."_picard_tmp";
    unless (-d "$picard_temp") { mkdir $picard_temp || warn "Could not create Directory $picard_temp $!\n"; }


    my $picard_mergesamfiles_jar = $args_dict->{"picard_mergesamfiles_jar"};
    my $output_dir = $args_dict->{"output_directory"};
    my $output_format = $args_dict->{"output_format"};

    my $output_file = "";

    if ($output_format =~ /sam/i) {
	$output_file = $args_dict->{"final_output_file"}.".sam";
    }
    else {
	$output_file = $args_dict->{"final_output_file"}.".bam";
    }
	my $files_to_discard = {};
    #my $files_to_discard = $self->check_mapping_files($temp_output_folder);

#exit();
    my $merge_input = "";
    opendir(DIR, $temp_output_folder) || die("Cannot open directory $temp_output_folder");
    my @files= readdir(DIR);
    closedir(DIR);

	my $files_to_merge = [];
	foreach my $file (@files) {
	    unless ($file eq "." || $file eq ".." || $file =~ /^\_/ || $file eq "MD5" || $file eq ".DS_Store") {
		my $file_size=0;
		$file_size = stat("$temp_output_folder/$file")->size;

		if ($file_size>1) {

                    if (!exists$files_to_discard->{$file}) {
                        $merge_input .= " I=$temp_output_folder/$file";
			push @$files_to_merge,"$temp_output_folder/$file";
                    }
		}

	    }
	}

	my $merge_limit = 200;
	if (scalar(@$files_to_merge)>$merge_limit) {

		my @final_temp_bam = ();
		my $temp_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$mapper_name";
		unless (-d $temp_dir) {
			mkdir($temp_dir);
		}

		my $file_count = scalar(@$files_to_merge);


		my $temp_merge_file_count=0;
		my $reminder = $file_count%$merge_limit;
		if ($reminder==0) {
			$temp_merge_file_count = int($file_count/$merge_limit);
		}
		else {
			$temp_merge_file_count = int($file_count/$merge_limit)+1;
		}


		for (my $i=1;$i<=$temp_merge_file_count;$i++) {
			my $temp_bam = $i.".bam";
			my $merge_input = "";
			my @files = ();

			my $to_cut=0;
			if (scalar(@$files_to_merge)>=$merge_limit) {
				$to_cut = $merge_limit;
			}
			else {
				$to_cut = scalar(@$files_to_merge);
			}
			@files = splice(@$files_to_merge,0,$to_cut);

			foreach my $ff (@files) {
				$merge_input .= " I=$ff";
			}


			if ($merge_input ne "") {

				push(@final_temp_bam, "$temp_dir/$temp_bam");
				### Merging all BAM files into one
				if ($i>=2) {
				my $mergebam_command="java -Xmx8g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$temp_dir/$temp_bam SO=coordinate VALIDATION_STRINGENCY=SILENT MAX_RECORDS_IN_RAM=5000000 TMP_DIR=$picard_temp";
				system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
				}
			}

			@files = ();
			$merge_input = "";
			#last if scalar(@$files_to_merge)<1;
		}


		### now merging the intermediate temp bam files
		my $merge_input = "";
		foreach my $ff (@final_temp_bam) {
			$merge_input .= " I=$ff";
		}

		if ($merge_input ne "") {
		    ### Merging all BAM files into one
		    my $mergebam_command="java -Xmx8g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT MAX_RECORDS_IN_RAM=5000000 TMP_DIR=$picard_temp";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}



	}

	else {
		if ($merge_input ne "") {
		    ### Merging all BAM files into one
		    my $mergebam_command="java -Xmx8g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT MAX_RECORDS_IN_RAM=5000000 TMP_DIR=$picard_temp";
		    print "$mergebam_command<br>";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";

		    #print "CMD: $mergebam_command\n";
		}
	}


}

sub tophat_output {
	die("OBSOLETE: DataMerge::tophat_output shouldn't be called. Use DataDownloadAnMerge instead");
    my ($self,$args_dict, $mapper_name) = @_;

    my $temp_output_folder = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
    my $picard_mergesamfiles_jar = $args_dict->{"picard_mergesamfiles_jar"};
    my $output_dir = $args_dict->{"output_directory"};
    my $output_format = $args_dict->{"output_format"};

    my $picard_temp = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/picard_tmp";
    unless (-d "$picard_temp") { mkdir $picard_temp || warn "Could not create Directory $picard_temp $!\n"; }

    my $output_file = "";

    if ($output_format =~ /sam/i) {
	$output_file = $args_dict->{"final_output_file"}.".sam";
    }
    else {
	$output_file = $args_dict->{"final_output_file"}.".bam";
    }


    my $merge_input = "";
    my $files_to_merge = [];
    opendir(DIR, $temp_output_folder) || die("Cannot open directory $temp_output_folder");
    my @files= readdir(DIR);
	closedir(DIR);

	my @f = ();
	foreach my $file (@files) {
	    unless ($file eq "." || $file eq ".." || $file =~ /^\_/) {
	    $file = "$file/accepted_hits.bam";
	    if (-e "$temp_output_folder/$file") {
		my $file_size=0;
		$file_size = stat("$temp_output_folder/$file")->size;

		if ($file_size>1) {
		    $merge_input .= " I=$temp_output_folder/$file";
		    push @$files_to_merge,"$temp_output_folder/$file";
		}
	    }

	    }
	}

#    if ($merge_input ne "") {
#	### Merging all BAM files into one
#	my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT";
#	#print "$mergebam_command\n";
#	system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
#    }


	my $merge_limit = 200;
	if (scalar(@$files_to_merge)>$merge_limit) {

		my @final_temp_bam = ();
		my $temp_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$mapper_name";
		unless (-d $temp_dir) {
			mkdir($temp_dir);
		}

		my $file_count = scalar(@$files_to_merge);

		my $temp_merge_file_count = int($file_count/$merge_limit)+1;
		print "$temp_merge_file_count\n";

		for (my $i=1;$i<=$temp_merge_file_count;$i++) {
			my $temp_bam = $i.".bam";
			my $merge_input = "";
			my @files = ();

			my $to_cut=0;
			if (scalar(@$files_to_merge)>=$merge_limit) {
				$to_cut = $merge_limit;
			}
			else {
				$to_cut = scalar(@$files_to_merge);
			}
			@files = splice(@$files_to_merge,0,$to_cut);

			foreach my $ff (@files) {
				$merge_input .= " I=$ff";
			}

			push(@final_temp_bam,"$temp_dir/$temp_bam");
			if ($merge_input ne "") {
				### Merging all BAM files into one
				my $mergebam_command="java -Xmx8g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$temp_dir/$temp_bam SO=coordinate VALIDATION_STRINGENCY=SILENT TMP_DIR=$picard_temp";
				system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
			}

			@files = ();
			$merge_input = "";
		}


		### now merging the intermediate temp bam files
		my $merge_input = "";
		foreach my $ff (@final_temp_bam) {
			$merge_input .= " I=$ff";
		}

		if ($merge_input ne "") {
		    ### Merging all BAM files into one
		    my $mergebam_command="java -Xmx8g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT TMP_DIR=$picard_temp";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}



	}

	else {
		if ($merge_input ne "") {
		    ### Merging all BAM files into one
		    my $mergebam_command="java -Xmx8g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT TMP_DIR=$picard_temp";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}
	}


}


sub check_mapping_files {
	die("OBSOLETE: DataMerge::check_mapping_files shouldn't be called. Use DataDownloadAnMerge instead");
	my ($self,$temp_output_folder) = @_;

	my $files_to_discard = {};

	opendir(DIR, $temp_output_folder) || die("Cannot open directory $temp_output_folder");
	my @files= readdir(DIR);
	closedir(DIR);
	my $f1 = {};
        my $f2 = {};

	my $block_count_conf_xml=0;
	foreach my $file (@files) {

		if ($file eq "_logs") {
			my $history_dir = "$temp_output_folder/$file/history";
			opendir(DIR, $history_dir) || die("Cannot open directory $history_dir");
			my @hfiles = readdir(DIR);
			closedir(DIR);

			my $job_xml = "";
			foreach my $f (@hfiles) {
				if ($f =~ /conf.xml$/) {
					$job_xml=$f;
				}

			}


			$job_xml = "$history_dir/$job_xml";

			if (-e $job_xml) {
				open my $fh,"<$job_xml" or warn "Could not open $job_xml for write $!";
				while (my $line = <$fh>) {
					chomp $line;
					#<property><!--Loaded from /Volumes/cluster/hadoop-1.2.1/tmp/mapred/local/jobTracker/job_201408111315_0056.xml--><name>mapred.map.tasks</name><value>1186</value></property>

					if ($line =~ /\<name\>mapred\.map\.tasks\<\/name\>/) {
						$line =~ /.*\<value\>(\d+)\<\/value\>.*/;
						$block_count_conf_xml = $1;

					}
				}
				close $fh;

			}

			#print $block_count_conf_xml;
		}



	    unless ($file eq "." || $file eq ".." || $file =~ /^\_/) {

		my $file_size=0;
		$file_size = stat("$temp_output_folder/$file")->size;


                if ($file =~ /^mapping/) {
                    $f1->{$file} = "$temp_output_folder/$file";
                }

                if ($file =~ /^part/) {
                    $f2->{$file} = "$temp_output_folder/$file";
                }

	    }
	}

	#exit();

        my $mapping_file_count = scalar(keys %$f1);
        my $part_file_count = scalar(keys %$f2);

        #print "mapping: $mapping_file_count\n";
        #print "part: $part_file_count\n";

	#exit();
	if ($part_file_count<$block_count_conf_xml) {

            my $missing_blocks = ($block_count_conf_xml - $part_file_count);
            print STDERR "\nERROR: $missing_blocks blocks are missing\nplease run same command only with --only-hdfs-download flag\n\n";
            exit(0);
        }
        elsif ($part_file_count > $block_count_conf_xml ) {

            foreach my $map_file (keys %$f2) {
                system("md5 $f2->{$map_file} >> $temp_output_folder/MD5");

            }


            open my $fh,"<$temp_output_folder/MD5" or die "Could not open $temp_output_folder/MD5 for write $!";

            my $md5_list = [];
            my $md5_hash = {};
            my $md5_hash1 = {};
                while (my $line=<$fh>) {
                    chomp($line);

                    #print "$line\n";

                    foreach my $map_file (keys %$f2) {

                        if ($line =~ /$map_file/i) {

                            my @l = split("=",$line);
                            my $md5 = $l[-1];
                            $md5 =~ s/\s*//g;

                            push(@$md5_list,$md5);
                            $md5_hash1->{$map_file} = $md5;
                            $md5_hash->{$md5} = $map_file;

                        }
                    }


                }

            my $ct=0;


            my @files = ();
            foreach my $md51 (keys %$md5_hash) {
                #print "$md5\n";

                foreach my $md52 (keys %$md5_hash1) {
                    #print "$md52\n";
                    if ($md5_hash1->{$md52} eq $md51) {
                        $ct++;
                        push(@files,$md52);
                    }
                }

                if ($ct>1) {
                    #print "$md51\t$ct\t$md5_hash->{$md51}\n";
                    $files_to_discard->{$md5_hash->{$md51}} = $md5_hash->{$md51};
                    shift(@files);

                    foreach (@files) {
                        $files_to_discard->{$_} = $_;
                    }

                    #print "@files\n";
                }
                $ct=0;
                @files = ();
            }

            unlink("$temp_output_folder/MD5");

        }

        return $files_to_discard;



}


=cut
sub check_mapping_files {
	my ($self,$temp_output_folder) = @_;

	my $files_to_discard = {};

	opendir(DIR, $temp_output_folder) || die("Cannot open directory $temp_output_folder");
	my @files= readdir(DIR);
	closedir(DIR);
	my $f1 = {};
        my $f2 = {};


	foreach my $file (@files) {

	    unless ($file eq "." || $file eq ".." || $file =~ /^\_/) {

		my $file_size=0;
		$file_size = stat("$temp_output_folder/$file")->size;


                if ($file =~ /^mapping/) {
                    $f1->{$file} = "$temp_output_folder/$file";
                }

                if ($file =~ /^part/) {
                    $f2->{$file} = "$temp_output_folder/$file";
                }

	    }
	}


        my $mapping_file_count = scalar(keys %$f1);
        my $part_file_count = scalar(keys %$f2);

        print "mapping: $mapping_file_count\n";
        print "part: $part_file_count\n";

	exit();
        if ($mapping_file_count>$part_file_count) {

            foreach my $map_file (keys %$f1) {
                system("md5 $f1->{$map_file} >> $temp_output_folder/MD5");

            }


            open my $fh,"<$temp_output_folder/MD5" or die "Could not open $temp_output_folder/MD5 for write $!";

            my $md5_list = [];
            my $md5_hash = {};
            my $md5_hash1 = {};
                while (my $line=<$fh>) {
                    chomp($line);

                    #print "$line\n";

                    foreach my $map_file (keys %$f1) {

                        if ($line =~ /$map_file/i) {

                            my @l = split("=",$line);
                            my $md5 = $l[-1];
                            $md5 =~ s/\s*//g;

                            push(@$md5_list,$md5);
                            $md5_hash1->{$map_file} = $md5;
                            $md5_hash->{$md5} = $map_file;

                        }
                    }


                }

            my $ct=0;


            my @files = ();
            foreach my $md51 (keys %$md5_hash) {
                #print "$md5\n";

                foreach my $md52 (keys %$md5_hash1) {
                    #print "$md52\n";
                    if ($md5_hash1->{$md52} eq $md51) {
                        $ct++;
                        push(@files,$md52);
                    }
                }

                if ($ct>1) {
                    #print "$md51\t$ct\t$md5_hash->{$md51}\n";
                    $files_to_discard->{$md5_hash->{$md51}} = $md5_hash->{$md51};
                    shift(@files);

                    foreach (@files) {
                        $files_to_discard->{$_} = $_;
                    }

                    #print "@files\n";
                }
                $ct=0;
                @files = ();
            }

            unlink("$temp_output_folder/MD5");

        }
        elsif ($mapping_file_count<$part_file_count) {

            my $missing_blocks = ($part_file_count-$mapping_file_count);
            print STDERR "\nERROR: $missing_blocks blocks are missing\nplease run same command only with --only-hdfs-download flag\n\n";
            #exit(0);
        }

        return $files_to_discard;



}
=cut



sub exonerate_output {
	die("OBSOLETE: DataMerge::exonerate_output shouldn't be called. Use DataDownloadAnMerge instead");
    my ($self,$args_dict) = @_;

    my $temp_output_folder = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
    my $output_dir = $args_dict->{"output_directory"};
    my $output_format = $args_dict->{"output_format"};

    my $output_file = "";
    $output_file = $args_dict->{"final_output_file"}.".exon";


    my $files_to_discard = $self->check_mapping_files($temp_output_folder);

	open (my $ofh,">"."$output_dir/$output_file") or die "could not open $_";

    my $merge_input = "";
    opendir(DIR, $temp_output_folder) || die("Cannot open directory $temp_output_folder");
    my @files= readdir(DIR);
    closedir(DIR);

	my $files_to_merge = [];
	foreach my $file (@files) {
	    unless ($file eq "." || $file eq ".." || $file =~ /^\_/ || $file eq "MD5" || $file eq ".DS_Store") {
		my $file_size=0;
		$file_size = stat("$temp_output_folder/$file")->size;

		if ($file_size>1) {

                    if (!exists$files_to_discard->{$file}) {
			if ($file=~/exon$/) {
				open (my $fh,"<"."$temp_output_folder/$file") or die "could not open $_";
				while(my $line=<$fh>) {
					print $ofh "$line";
				}
			}
                    }
		}

	    }
	}

	print STDERR "\t====> Final output file is: $output_dir/$output_file\n\n";
}
1;
