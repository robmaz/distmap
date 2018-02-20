#!/usr/bin/perl -w
use strict;
use warnings;
package HadoopMapping;
use Getopt::Long;
use POSIX q/strftime/;
use Cwd 'abs_path';
use FindBin qw/$RealBin/;
use lib "$RealBin/../lib/perl5/site_perl";
use Utility;
use File::Basename;

sub new {
	my $class=shift;
	my $self = {};
	bless $self, $class;
	return $self;
}


sub start {
	my ($self,$args_dict) = @_;


	################### Time start #########################
	my $start_time = time();
	my $time_stamp_start = Utility::get_time_stamp();
	print STDERR "\nStarted at:  $time_stamp_start\n";

	### Paired-end read mapping.
	$self->paired_end_mapping($args_dict);


	### Single-end read mapping.
	$self->single_end_mapping($args_dict);

	################### Time end #########################
	my $end_time = time();
	my $executation_time = Utility::get_executation_time($start_time,$end_time);
	my $time_stamp_end = Utility::get_time_stamp();
	print STDERR "Finished at: $time_stamp_end\n";
	print STDERR "Duration: $executation_time\n\n";



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

sub paired_end_mapping {
	my ($self,$args_dict) = @_;

	$args_dict->{"read_folder"} = "fastq_paired_end";
	$args_dict->{"read_type"} = "pe";
	#$args_dict->{"final_output_file"} = "DistMap_output_Paired_end_reads";
	#my $shell_script = $args_dict->{"output_directory"}."/$args_dict->{'random_id'}/".lc($args_dict->{"mapper"})."_pe_hadoop.sh";
	#$args_dict->{"shell_script"} = $shell_script;

	my $file_count=0;

	my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}";
	if (-d $to_read_dir) {
		$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}");
	}


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

	if ($file_count>0 or $hdfs_file_count>0) {

		my $i=0;
		foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
			my $mapper_path = $args_dict->{"mapper_path"}->[$i];
			my $mapper_args = $args_dict->{"mapper_args"}->[$i];

			$args_dict->{"final_output_file"} = lc($mapper_name)."_DistMap_output_Paired_end_reads";

			my $shell_script = $args_dict->{"output_directory"}."/$args_dict->{'random_id'}/".lc($mapper_name)."_pe_hadoop.sh";
			$args_dict->{"shell_script"} = $shell_script;

			$self->get_mapper_command($args_dict, $mapper_name, $mapper_path, $mapper_args);
			$self->write_hadoop_mapping_job($args_dict);

			$i++;
		}

		$i=0;
		foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
			my $mapper_path = $args_dict->{"mapper_path"}->[$i];
			my $mapper_args = $args_dict->{"mapper_args"}->[$i];

			$args_dict->{"final_output_file"} = lc($mapper_name)."_DistMap_output_Paired_end_reads";
			my $shell_script = $args_dict->{"output_directory"}."/$args_dict->{'random_id'}/".lc($mapper_name)."_pe_hadoop.sh";
			$args_dict->{"shell_script"} = $shell_script;
			system("sh $shell_script");

			$i++;
		}

		#$self->write_hadoop_mapping_job($args_dict);
		#system("sh $shell_script");
	}

}

sub single_end_mapping {
	my ($self,$args_dict) = @_;

	$args_dict->{"read_folder"} = "fastq_single_end";
	$args_dict->{"read_type"} = "se";
	$args_dict->{"final_output_file"} = "DistMap_output_Single_end_reads";
	my $shell_script = $args_dict->{"output_directory"}."/$args_dict->{'random_id'}/".lc($args_dict->{"mapper"})."_se_hadoop.sh";
	$args_dict->{"shell_script"} = $shell_script;

	my $file_count=0;

	my $to_read_dir = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}";
	if (-d $to_read_dir) {
		$file_count = $self->get_file_list("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'read_folder'}");
	}

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

	if ($file_count>0 or $hdfs_file_count>0) {


		my $i=0;
		foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
			my $mapper_path = $args_dict->{"mapper_path"}->[$i];
			my $mapper_args = $args_dict->{"mapper_args"}->[$i];

			$args_dict->{"final_output_file"} = lc($mapper_name)."_DistMap_output_Paired_end_reads";

			my $shell_script = $args_dict->{"output_directory"}."/$args_dict->{'random_id'}/".lc($mapper_name)."_se_hadoop.sh";
			$args_dict->{"shell_script"} = $shell_script;

			$self->get_mapper_command($args_dict, $mapper_name, $mapper_path, $mapper_args);
			$self->write_hadoop_mapping_job($args_dict);

			$i++;
		}

		$i=0;
		foreach my $mapper_name (@{$args_dict->{"mapper"}}) {
			my $mapper_path = $args_dict->{"mapper_path"}->[$i];
			my $mapper_args = $args_dict->{"mapper_args"}->[$i];

			$args_dict->{"final_output_file"} = lc($mapper_name)."_DistMap_output_Single_end_reads";
			my $shell_script = $args_dict->{"output_directory"}."/$args_dict->{'random_id'}/".lc($mapper_name)."_se_hadoop.sh";
			$args_dict->{"shell_script"} = $shell_script;
			system("sh $shell_script");

			$i++;
		}

		#$self->get_mapper_command($args_dict);
		#$self->write_hadoop_mapping_job($args_dict);
		#system("sh $shell_script");
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


sub get_mapper_command {
	my ($self,$args_dict,$mapper_name,$mapper_path,$mapper_args) = @_;

	my $script_current_file = abs_path($0);
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $script_current_file, '\..*' );
	my $script_current_directory = $path;
	$script_current_directory =~ s/\/$//;
	my $mapper_command = "";
	my $reference_file = "reference.fa";

	##########################################################################################
	# 				Hadoop input output folders

	my $input =  "/$args_dict->{'random_id'}"."_input";
	my $output =  "/$args_dict->{'random_id'}"."_output";
	my $read_folder = "$input/$args_dict->{'read_folder'}";
	my $input_folder = $read_folder; #"$read_folder/fastq";
	my $archieve_folder = $input; #"$read_folder/archieve";
	my $output_folder = "$output/$args_dict->{'read_folder'}"."_mapping_".lc($mapper_name);
	my $output_folder_trim = "$output/$args_dict->{'read_folder'}"."_trimming";
	my $hadoop_exe = $args_dict->{'hadoop_exe'};
	my $hdfs_exe = $args_dict->{"hdfs_exe"};


	if ($args_dict->{"trimming_flag"}) {
		$input_folder = $output_folder_trim;
	}
	elsif ((system("$hdfs_exe dfs -test -d $output_folder_trim")==0)) {
		$input_folder = $output_folder_trim;
	}



	if (system ("$hdfs_exe dfs -test -d $input")==0) {
		system("$hdfs_exe dfs -chmod -R 777 $output") == 0 || warn "Error in changing the permission of $output folder\n";
		system("$hdfs_exe dfs -chown -R $args_dict->{'username'}:$args_dict->{'groupname'} $output/") == 0 || warn "Error in changing the ownership of $output folder\n";
	}
	else {
		system("$hdfs_exe dfs -mkdir $output") == 0 || warn "Error could not create output directory $output folder on hdfs file system\n";
		system("$hdfs_exe dfs -chmod -R 777 $output") == 0 || warn "Error in changing the permission of $output folder\n";
		system("$hdfs_exe dfs -chown -R $args_dict->{'username'}:$args_dict->{'groupname'} $output/") == 0 || warn "Error in changing the ownership of $output folder\n";
	}
	if (system ("$hdfs_exe dfs -test -d $output_folder")==0) {
		system("$hdfs_exe dfs -chown -R $args_dict->{'username'}:$args_dict->{'groupname'} $output_folder/") == 0 || warn "Error in changing the ownership of $output_folder folder\n";
	}

	if (system ("$hdfs_exe dfs -test -d $output_folder_trim")==0) {
		system("$hdfs_exe dfs -chown -R $args_dict->{'username'}:$args_dict->{'groupname'} $output_folder_trim/") == 0 || warn "Error in changing the ownership of $output_folder_trim folder\n";
	}

	##########################################################################################

	my $hadoop_mapping_format = "bam";


	if ($mapper_name =~ m/bwa/i) {
				$reference_file = "reference.fa";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "bwa_mapping.pl";

		if ($mapper_args ne "") {


			if (exists$args_dict->{'bwa_sampe_args'} and  defined($args_dict->{'bwa_sampe_args'}) and $args_dict->{'bwa_sampe_args'} ne "") {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/bwa --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --bwa-sampe-options '$args_dict->{bwa_sampe_args}' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
			}
			else {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/bwa --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
			}
		}
		else {

			if (exists$args_dict->{'bwa_sampe_args'} and  defined($args_dict->{'bwa_sampe_args'}) and $args_dict->{'bwa_sampe_args'} ne "") {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/bwa --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --bwa-sampe-options '$args_dict->{bwa_sampe_args}' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
			}
			else {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/bwa --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
			}

		}



	}

	if ($mapper_name =~ m/exonerate/i) {
		$reference_file = "reference.fa";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "exonerate.pl";
		$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/exonerate --mapper-args '$mapper_args' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

	}

	if ($mapper_name =~ m/gsnap/i) {
		$reference_file = "reference";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "gsnap_mapping.pl";

		if ($mapper_args ne "") {
			if ($args_dict->{'gsnap_output_split'}) {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/gsnap --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --quality-encoding illumina --hadoop $args_dict->{hadoop_exe} --gsnap-output-split --verbose";
			}
			else {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/gsnap --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --quality-encoding illumina --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

			}
		}
		else {
			if ($args_dict->{'gsnap_output_split'}) {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/gsnap --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --quality-encoding illumina --hadoop $args_dict->{hadoop_exe} --gsnap-output-split --verbose";
			}
			else {
				$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/gsnap --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --quality-encoding illumina --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

			}

		}

	}

	if ($mapper_name =~ m/novoalign/i) {
		$reference_file = "reference.nix";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "novoalign_mapping.pl";

		if ($mapper_args ne "") {

			$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/novoalign --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

		}


	}

	elsif ($mapper_name =~ /tophat/i) {
		$reference_file = "reference";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "tophat_mapping.pl";
		$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/tophat --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --quality-encoding illumina --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

	}

	elsif ($mapper_name =~ /bowtie$/i) {

		$reference_file = "reference";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "bowtie_mapping.pl";

		$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/bowtie --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

	}
	elsif ($mapper_name =~ /bowtie2$/i) {
		$reference_file = "reference";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "bowtie_mapping.pl";


		$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/bowtie2 --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
		#print "command: $mapper_command\n";
	}

	elsif ($mapper_name =~ /soap/i) {
		$reference_file = "reference.fa.index";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "soap_mapping.pl";

		$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-dir $args_dict->{'extracted_refarch'}/ref --ref-fasta $args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/soap --soap2sam-path $args_dict->{'extracted_execarch'}/bin/soap2sam.pl --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --quality-encoding illumina --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";

	}


	if ($mapper_name =~ m/ngm/i) {
		$reference_file = "reference.fa";
		$args_dict->{"index_name"} = $reference_file;
		$args_dict->{"mapper_script_name"} = "ngm_mapping.pl";

		if ($mapper_args ne "") {

		$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/ngm --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --mapper-args '$mapper_args' --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
		print "$mapper_command\n";
		#exit();
		}
		else {
			$mapper_command = "$args_dict->{'mapper_script_name'} --output-dir $output_folder --ref-fasta $args_dict->{'extracted_refarch'}/ref/$args_dict->{'index_name'} --mapper-path $args_dict->{'extracted_execarch'}/bin/ngm --picard-sartsam-jar \"$args_dict->{'extracted_execarch'}/bin/$args_dict->{'picard_sortsam_jar'}\" --output-format $hadoop_mapping_format --hadoop $args_dict->{hadoop_exe} --hdfs $args_dict->{hdfs_exe} --verbose";
		print "$mapper_command\n";
		#exit();
		}
	}




	$args_dict->{"mapper_script_path"} = "$script_current_directory/bin/$args_dict->{'mapper_script_name'}";
	$args_dict->{"utility_script_path"} = "$script_current_directory/bin/Utility.pm";
	$args_dict->{"input_folder"} = $input_folder;
	$args_dict->{"output_folder"} = $output_folder;
	$args_dict->{"output_folder_trim"} = $output_folder_trim;
	$args_dict->{"archieve_folder"} = $archieve_folder;
	$args_dict->{"mapper_command"} = $mapper_command;


}




sub write_hadoop_mapping_job {
	my ($self,$args_dict) = @_;


	my $exe_archive_path="";
	my $ref_index_archive_path="";

	if ($args_dict->{'refindex_archive'} ne "" and exists$args_dict->{'refindex_archive'}) {
		my $refindex_archive = $args_dict->{"refindex_archive"};
		my $exec_arch_file_name = basename($refindex_archive);
		$ref_index_archive_path = "hdfs://$args_dict->{'archieve_folder'}/$exec_arch_file_name";

	}
	else {
		$ref_index_archive_path = "hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'ref_arch'}"
	}

	if ($args_dict->{'mappers_exe_archive'} ne "" and exists$args_dict->{'mappers_exe_archive'}) {
		my $exec_arch_file_name = basename($args_dict->{'mappers_exe_archive'});
		$exe_archive_path = "'hdfs://$args_dict->{'archieve_folder'}/$exec_arch_file_name"."#"."$args_dict->{'extracted_execarch'},$ref_index_archive_path"."#"."$args_dict->{'extracted_refarch'}'";
	}
	else {
		$exe_archive_path = "'hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'exec_arch'}"."#"."$args_dict->{'extracted_execarch'},$ref_index_archive_path"."#"."$args_dict->{'extracted_refarch'}'";
	}

	print "$exe_archive_path\n";
	#exit();

	my $shell_script = "";
	$shell_script = $args_dict->{"shell_script"};
	open my $ofh,">$shell_script" or die "Could not open $shell_script for write $!";

	print $ofh "#!/bin/sh\n\n";
	print $ofh "streaming_home=\"$args_dict->{'streaming_jar'}\"\n";
	print $ofh "hadoop_home=\"$args_dict->{'hadoop_exe'}\"\n\n";
	print $ofh "hdfs_home=\"$args_dict->{'hdfs_exe'}\"\n\n";
	print $ofh "input_folder=\"$args_dict->{'input_folder'}\"\n";
	print $ofh "output_folder=\"$args_dict->{'output_folder'}/\"\n";
	print $ofh "job=\"$args_dict->{'job_desc'}"."_Mapping_"."$args_dict->{'read_folder'}\"\n\n";

	print $ofh "time\n";
	print $ofh "\n\n";
	print $ofh "echo \"===============================================================\"\n";
	print $ofh "echo \"Step3: Short read mapping on cluster started\"\n";
	print $ofh "echo \"===============================================================\"\n";
	print $ofh "time\n";
	print $ofh "time \$hdfs_home dfs -rm -r \$output_folder\n\n";

	print $ofh "time \$hadoop_home  jar \$streaming_home \\\n";
	#print $ofh "-archives 'hdfs://$args_dict->{'archieve_folder'}/$args_dict->{'job_arch'}"."#"."{'extracted_execarch'}' \\\n";

	if ($args_dict->{"upload_index"}) {
		print $ofh "-archives $exe_archive_path \\\n";
	}
	else {
		print $ofh "-archives $exe_archive_path \\\n";
	}


	print $ofh "-D mapreduce.tasktracker.map.tasks.maximum=1 \\\n";
	print $ofh "-D mapred.max.maps.per.node=1 \\\n";
	print $ofh "-D dfs.blocksize=$args_dict->{'block_size'} \\\n";


	print $ofh "-D mapreduce.map.memory.mb=3072 \\\n";
	print $ofh "-D mapreduce.map.java.opts=-Xmx2304m \\\n";
	print $ofh "-D mapred.child.java.opts=-Xmx2304m \\\n";
	print $ofh "-D mapreduce.reduce.memory.mb=3072 \\\n";
	print $ofh "-D mapreduce.reduce.java.opts=-Xmx2304m \\\n";

	#print $ofh "-D mapreduce.map.memory.mb=6144 \\\n";
	#print $ofh "-D mapreduce.map.java.opts=-Xmx4608m \\\n";
	#print $ofh "-D mapred.child.java.opts=-Xmx4608m \\\n";
	#print $ofh "-D mapreduce.reduce.memory.mb=9216 \\\n";
	#print $ofh "-D mapreduce.reduce.java.opts=-Xmx6912m \\\n";

	print $ofh "-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec \\\n";
	print $ofh "-D mapreduce.map.output.compress=true \\\n";
	print $ofh "-D mapreduce.output.fileoutputformat.compress=true \\\n";
	print $ofh "-D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.GzipCodec \\\n";
	print $ofh "-D mapreduce.output.fileoutputformat.compress.type=RECORD \\\n";

	print $ofh "-D $args_dict->{'job_priority'} \\\n";
	print $ofh "-D $args_dict->{'queue_name'} \\\n";
	print $ofh "-D mapreduce.job.reduces=0 \\\n";
	print $ofh "-D mapreduce.job.name=\"\$job\" \\\n"; #mapreduce.job.tags
	print $ofh "-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \\\n";
	print $ofh "-D stream.num.map.output.key.fields=4 \\\n";
	print $ofh "-D mapreduce.partition.keypartitioner.options=-k1,4 \\\n";
	print $ofh "-D mapreduce.partition.keycomparator.options=-k1,4 \\\n";
	print $ofh "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\\n";
	print $ofh "-input \"\$input_folder\" \\\n";
	print $ofh "-output \"\$output_folder\" \\\n";
	print $ofh "-mapper \"$args_dict->{'mapper_command'}\" \\\n";
	print $ofh "-file '$args_dict->{'mapper_script_path'}' \\\n";
	print $ofh "-file '$args_dict->{'utility_script_path'}'";
	close $ofh;

}

1;
