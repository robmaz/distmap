#!/usr/bin/env perl
use strict;
use warnings;
package DownloadTrimmedRead;
use Getopt::Long;
use IO::Compress::Gzip;
use File::stat;
use POSIX q/strftime/;
use Cwd 'abs_path';
use FindBin qw/$RealBin/;
use lib "$RealBin/../lib/perl5/site_perl";
use Utility;


sub new {
	die("OBSOLETE: DownloadTrimmedRead::new shouldn't be called.");
	my $class=shift;
	my $self = {};
	bless $self, $class;
	return $self;
}


sub start {
	die("OBSOLETE: DownloadTrimmedRead::start shouldn't be called.");
	my ($self,$args_dict) = @_;


	print STDERR "=======================================================================\n";
	print STDERR "Step7: Downloading and merging the trimmed FASTQ files\n";
	print STDERR "=======================================================================\n";
	################### Time start #########################
	my $start_time = time();
	my $time_stamp_start = Utility::get_time_stamp();
	print STDERR "\nStarted at:  $time_stamp_start\n";



	### Paired-end read trimming.
	$self->paired_end_trimming($args_dict);


	### Single-end read trimming.
	$self->single_end_trimming($args_dict);

	################### Time end #########################
	my $end_time = time();
	my $executation_time = Utility::get_executation_time($start_time,$end_time);
	my $time_stamp_end = Utility::get_time_stamp();
	print STDERR "Finished at: $time_stamp_end\n";
	print STDERR "Duration: $executation_time\n\n";



}


sub paired_end_trimming {
	die("OBSOLETE: DownloadTrimmedRead::paired_end_trimming shouldn't be called.");
	my ($self,$args_dict) = @_;

	$args_dict->{"read_folder"} = "fastq_paired_end";
	$args_dict->{"read_type"} = "pe";
	$args_dict->{"final_output_file"} = "DistMap_output_Paired_end_trimmed_reads";

	my $output =  "/$args_dict->{'hdfs_home'}"."_output";
	my $output_folder = "$output/$args_dict->{'read_folder'}"."_trimming";
	$args_dict->{"output_folder"} = $output_folder;

	#my $file_count=0;
	#
	#my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}";
	#if (-d $to_read_dir) {
	#	$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}");
	#}
	#
	#
	#if ($file_count>0) {
	#
	#	$self->download_merge_trimmed_reads($args_dict);
	#}

	$self->download_merge_trimmed_reads($args_dict);

}



sub single_end_trimming {
	die("OBSOLETE: DownloadTrimmedRead::single_end_trimming shouldn't be called.");
	my ($self,$args_dict) = @_;

	$args_dict->{"read_folder"} = "fastq_single_end";
	$args_dict->{"read_type"} = "se";
	$args_dict->{"final_output_file"} = "DistMap_output_Single_end_trimmed_reads";

	my $output =  "/$args_dict->{'hdfs_home'}"."_output";
	my $output_folder = "$output/$args_dict->{'read_folder'}"."_trimming";
	$args_dict->{"output_folder"} = $output_folder;

	#my $file_count=0;

	#my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}";
	#if (-d $to_read_dir) {
	#	$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}");
	#}

	#if ($file_count>0) {

	#	$self->download_merge_trimmed_reads($args_dict);
	#}

	$self->download_merge_trimmed_reads($args_dict);

}


sub get_file_list {
	die("OBSOLETE: DownloadTrimmedRead::get_file_list shouldn't be called.");
	my ($self,$dir) = @_;
	opendir(DIR, $dir) || die("Cannot open directory");
	my @files= readdir(DIR);
	closedir(DIR);

	my @f = ();
	foreach my $file (@files) {
		if ($file =~ /^fastq/i) {
			push(@f,$file);
		}
	}

	return scalar(@f);

}




sub download_merge_trimmed_reads {
	die("OBSOLETE: DownloadTrimmedRead::download_merge_trimmed_reads shouldn't be called.");
	my ($self,$args_dict) = @_;
	my $script_current_file = abs_path($0);
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $script_current_file, '\..*' );
	my $script_current_directory = $path;
	$script_current_directory =~ s/\/$//;
	my $merge_fastq_script = "$script_current_directory/bin/download_merge_fastq.pl";
	my $hadoop_exe = $args_dict->{'hadoop_exe'};
	my $hdfs_exe = $args_dict->{"hdfs_exe"};

	my $output_folder_trim = $args_dict->{'output_folder'};
	my $trimmed_output_file = $args_dict->{"output_directory"}."/".$args_dict->{"final_output_file"};

	if ((system("$hdfs_exe dfs -test -d $output_folder_trim")==0)) {

		#### Deleting the download folder if already exists
		Utility::deletedir("$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}"."_trimming");


		my $download_command = "$hdfs_exe dfs -copyToLocal $output_folder_trim $args_dict->{'output_directory'}/$args_dict->{'local_home'}/";

		print "$download_command\n";
		print STDERR "Data download from hdfs file system started ", localtime(), "\n";
		system($download_command) == 0 || warn "Error in uploading the $args_dict->{'job_arch'}\n";
		print STDERR "Data download from hdfs file system finished ", localtime(), "\n";


		$self->merge_trimmed_reads($args_dict);

	}
	else {
		print STDERR "Trimmed fastq files does not exits on cluster\n";
		#exit(1);
	}






}





sub merge_trimmed_reads {
	die("OBSOLETE: DownloadTrimmedRead::merge_trimmed_reads shouldn't be called.");
    my ($self,$args_dict) = @_;

    my $temp_output_folder = "$args_dict->{'output_directory'}/$args_dict->{'local_home'}/$args_dict->{'read_folder'}"."_trimming";

    my $picard_mergesamfiles_jar = $args_dict->{"picard_mergesamfiles_jar"};
    my $output_dir = $args_dict->{"output_directory"};
    my $output_format = $args_dict->{"output_format"};



	my $nozip=0;

	if ($args_dict->{"nozip"}) {
		$nozip=1;
	}

	$nozip=1;

	my $output_file = $args_dict->{"output_directory"}."/".$args_dict->{'local_home'};

	my $read1_fastq = $output_file."_trimmed_1.fastq";
	my $read2_fastq = $output_file."_trimmed_2.fastq";

	my $ofh1=undef;
	my $ofh2 = undef;


	if ($args_dict->{"read_type"} eq "pe") {
		$ofh1 = $self->getofhcreater($nozip,$read1_fastq);
		$ofh2 = $self->getofhcreater($nozip,$read2_fastq);
	}
	else {
		$read1_fastq = $output_file."_trimmed.fastq";
		$ofh1 = $self->getofhcreater($nozip,$read1_fastq);
	}


	my $files_to_discard = $self->check_mapping_files($temp_output_folder);




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
			    my $infile = "$temp_output_folder/$file";
			    open my $fh,"<$infile" or die "Could not open $infile for reading $!";
			    $self->write_fastq($ofh1,$ofh2,$fh);
			    #push @$files_to_merge,"$temp_output_folder/$file";
			}
		    }

		}
	    }



}



sub write_fastq {
	die("OBSOLETE: DownloadTrimmedRead::write_fastq shouldn't be called.");
	my ($self,$ofh1,$ofh2,$fh) = @_;

	my $read_name = "";
	my $read1_seq = "";
	my $read1_qual = "";
	my $read2_seq = "";
	my $read2_qual = "";

	my $read_type1="";
	while(<$fh>){
		my $line = $_;
		chomp($line);
		#print "$line\n";
		#my @col = split(/\t/);
		my @col = split("\t", $line);

		if (scalar(@col)==3) {
			$read_name = $col[0];
			$read1_seq = $col[1];
			$read1_qual = $col[2];

			print $ofh1 "$read_name\n";
			print $ofh1 "$read1_seq\n";
			print $ofh1 "+\n";
			print $ofh1 "$read1_qual\n";

			$read_type1="se"

		}
		elsif (scalar(@col)==5) {

			$read_name = $col[0];
			$read1_seq = $col[1];
			$read1_qual = $col[2];
			$read2_seq = $col[3];
			$read2_qual = $col[4];

			print $ofh1 "$read_name/1\n";
			print $ofh1 "$read1_seq\n";
			print $ofh1 "+\n";
			print $ofh1 "$read1_qual\n";

			print $ofh2 "$read_name/2\n";
			print $ofh2 "$read2_seq\n";
			print $ofh2 "+\n";
			print $ofh2 "$read2_qual\n";

			$read_type1="pe"
		}
		else {
			warn "Bad number of read columns ; expected 3 or 5:\n$_\n";
		}

	}



}

	sub getofhcreater{
		die("OBSOLETE: DownloadTrimmedRead::getofhcreater shouldn't be called.");
		my $self=shift;
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




sub check_mapping_files {
	die("OBSOLETE: DownloadTrimmedRead::check_mapping_files shouldn't be called.");
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

1;
