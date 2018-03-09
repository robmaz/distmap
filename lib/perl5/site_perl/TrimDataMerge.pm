#!/usr/bin/env perl
use strict;
use warnings;
package TrimDataMerge;
use FindBin qw/$RealBin/;
use lib "$RealBin/../lib/perl5/site_perl";
use Utility;
use File::Basename;
use File::stat;


sub new {
	my $class=shift;
	my $self = {};
	bless $self, $class;
	return $self;
}


sub start {
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



sub paired_end_data {
    my ($self,$args_dict) = @_;

    $args_dict->{"read_folder"} = "fastq_paired_end";
    $args_dict->{"read_type"} = "pe";
    $args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads";
    $args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping";

    my $file_count=0;
    my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}";
    if (-d $to_read_dir) {
	$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}");
    }

    if ($file_count>0) {

        if ($args_dict->{"mapper"} =~ /tophat/i) {
            $self->tophat_output($args_dict);
        }
        else {
            $self->bwa_output($args_dict);
        }

    }


}


sub single_end_data {
    my ($self,$args_dict) = @_;

    $args_dict->{"read_folder"} = "fastq_single_end";
    $args_dict->{"read_type"} = "se";
    $args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads";
    $args_dict->{"read_output_folder"} = "$args_dict->{'read_folder'}"."_mapping";


    my $file_count=0;
    my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}";
    if (-d $to_read_dir) {
	$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}");
    }

    if ($file_count>0) {

        if ($args_dict->{"mapper"} =~ /tophat/i) {
            $self->tophat_output($args_dict);
        }
	elsif ($args_dict->{"mapper"} =~ /exonerate/i) {
            $self->exonerate_output($args_dict);
        }
        else {
            $self->bwa_output($args_dict);
        }

    }

}


sub get_file_list {
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

sub bwa_output {
    my ($self,$args_dict) = @_;

    my $temp_output_folder = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
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
                        $merge_input .= " I=$temp_output_folder/$file";
			push @$files_to_merge,"$temp_output_folder/$file";
                    }
		}

	    }
	}

	my $merge_limit = 1000;
	if (scalar(@$files_to_merge)>$merge_limit) {

		my @final_temp_bam = ();
		my $temp_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}";

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
				my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$temp_dir/$temp_bam SO=coordinate VALIDATION_STRINGENCY=SILENT";
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
		    my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}



	}

	else {
		if ($merge_input ne "") {
		    ### Merging all BAM files into one
		    my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}
	}


}

sub tophat_output {
    my ($self,$args_dict) = @_;

    my $temp_output_folder = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_output_folder'}";
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


	my $merge_limit = 1000;
	if (scalar(@$files_to_merge)>$merge_limit) {

		my @final_temp_bam = ();
		my $temp_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}";

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
				my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$temp_dir/$temp_bam SO=coordinate VALIDATION_STRINGENCY=SILENT";
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
		    my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}



	}

	else {
		if ($merge_input ne "") {
		    ### Merging all BAM files into one
		    my $mergebam_command="java -Xmx4g -Dsnappy.disable=true -jar $picard_mergesamfiles_jar $merge_input O=$output_dir/$output_file SO=coordinate VALIDATION_STRINGENCY=SILENT";
		    system($mergebam_command) == 0 || die "Error in Merging mapping files $mergebam_command";
		}
	}


}


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

        #print "mapping: $mapping_file_count\n";
        #print "part: $part_file_count\n";

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
            exit(0);
        }

        return $files_to_discard;



}




sub exonerate_output {
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
