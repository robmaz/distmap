#!/usr/bin/perl -w
use strict;
use warnings;
package GenomeIndex;
use File::Copy;
use Cwd 'abs_path';
use Archive::Tar;
use Cwd;
use FindBin qw/$RealBin/;
use lib "$RealBin/bin";
use Utility;


##
# Author: Ram Vinay Pandey
#   Date: February 24, 2013
#
# This script index the reference fasta and creates an archive to upload into HDFS file system of Hadoop cluster.
#


sub new {
	my $class=shift;
	my $self = {};
	bless $self, $class;
	return $self;
}
sub start {
	
	my ($self,$args_dict) = @_;

	
	### to avoid the genome re-indexing
	#Utility::check_genome_index($args_dict);

	if ($args_dict->{"upload_index"}) {
		print STDERR "=======================================================================\n";
		print STDERR "Step1: Indexing the genome and uploading into HDFS\n";
		print STDERR "=======================================================================\n\n";
		
		################### Time start #########################
		my $start_time = time();
		my $time_stamp_start = Utility::get_time_stamp();
		print STDERR "\nStarted at:  $time_stamp_start\n";
		
		Utility::createdir("$args_dict->{'output_directory'}/$args_dict->{'ref_dir'}");
		
		$self->create_genome_index($args_dict);
		
		my $cmd1 = "tar -cvzf $args_dict->{'ref_arch'} $args_dict->{'ref_dir'}/";
		
		$self->create_archive($args_dict, $cmd1);
		
		################### Time end #########################
		my $end_time = time();
		my $executation_time = Utility::get_executation_time($start_time,$end_time);
		my $time_stamp_end = Utility::get_time_stamp();
		print STDERR "Finished at: $time_stamp_end\n";
		print STDERR "Duration: $executation_time\n\n";
	}
	
	
	
	
	my $mappers_exe_archive = $args_dict->{"mappers_exe_archive"};
	
	unless ($args_dict->{'mappers_exe_archive'} ne "" and exists$args_dict->{'mappers_exe_archive'}) {
		print STDERR "Copying executables\n";
		$self->copy_exec($args_dict);
	
		print STDERR "Creating Archive\n";
		
		my $cmd1 = "tar -cvzf $args_dict->{'exec_arch'} $args_dict->{'bin_dir'}/";
		$self->create_archive($args_dict, $cmd1);
	}
	
}



sub create_genome_index {
	my ($self,$args_dict) = @_;
	my $reference_file="";
	
	my $ref_dir="$args_dict->{'output_directory'}/$args_dict->{'ref_dir'}";
	$reference_file = "reference.fa";
	

	my $i=0;
	foreach my $mapper (@{$args_dict->{"mapper_path"}}) {
		my $mapper_path = $args_dict->{"mapper_path"}->[$i];
		
		if ($mapper =~ m/bwa/i) {
			$self->bwa_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		elsif ($mapper =~ m/(gsnap|gmap)/i) {
			$reference_file = "reference";
			$self->gsnap_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		elsif ($mapper =~ /(tophat|tophat2)/i) {
			$reference_file = "reference";
			$self->tophat_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir,$args_dict);
		}
		elsif ($mapper =~ /(bowtie|bowtie2)/i) {
			$reference_file = "reference";
			$self->bowtie_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir,$args_dict);
		}
		elsif ($mapper =~ /soap/i) {
			$self->soap_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		
		elsif ($mapper =~ /star/i) {
			$self->star_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		
		elsif ($mapper =~ /bismark/i) {
			$self->bismark_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir,$args_dict);
		}
		elsif ($mapper =~ /bsmap/i) {
			$self->bsmap_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		elsif ($mapper =~ /novoalign/i) {
			$self->novoalign_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		elsif ($mapper =~ /ngm/i) {
			$self->ngm_index($args_dict->{"reference_fasta"},$mapper_path,$ref_dir);
		}
		else {
			print STDERR "\t--mapper $args_dict->{'mapper'} not supported in DistMap\n";
			print STDERR "$args_dict->{'usage'}\n";
			#exit(1);
		}

		$i++;
	}
	
	#if ($args_dict->{"mapper"} =~ m/bwa/i) {
	#	$self->bwa_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#elsif ($args_dict->{"mapper"} =~ m/(gsnap|gmap)/i) {
	#	$reference_file = "reference";
	#	$self->gsnap_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#elsif ($args_dict->{"mapper"} =~ /(tophat|tophat2)/i) {
	#	$reference_file = "reference";
	#	$self->tophat_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir,$args_dict);
	#}
	#elsif ($args_dict->{"mapper"} =~ /(bowtie|bowtie2)/i) {
	#	$reference_file = "reference";
	#	$self->bowtie_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir,$args_dict);
	#}
	#elsif ($args_dict->{"mapper"} =~ /soap/i) {
	#	$self->soap_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#
	#elsif ($args_dict->{"mapper"} =~ /star/i) {
	#	$self->star_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#
	#elsif ($args_dict->{"mapper"} =~ /bismark/i) {
	#	$self->bismark_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir,$args_dict);
	#}
	#elsif ($args_dict->{"mapper"} =~ /bsmap/i) {
	#	$self->bsmap_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#elsif ($args_dict->{"mapper"} =~ /novoalign/i) {
	#	$self->novoalign_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#elsif ($args_dict->{"mapper"} =~ /ngm/i) {
	#	$self->ngm_index($args_dict->{"reference_fasta"},$args_dict->{"mapper_path"},$ref_dir);
	#}
	#else {
	#	print STDERR "\t--mapper $args_dict->{'mapper'} not supported in DistMap\n";
	#	print STDERR "$args_dict->{'usage'}\n";
	#	#exit(1);
	#}


	
	$args_dict->{"index_name"} = $reference_file;
	$args_dict->{"index_dir"} = $ref_dir;
	
}

sub create_archive {
	my ($self,$args_dict, $cmd1) = @_;
	my $script_current_directory = abs_path($0);
	my ( $name, $path, $extension ) = File::Basename::fileparse ( abs_path($0), '\..*' );
	
	my $current_directory = cwd();
	
	chdir("$args_dict->{'output_directory'}/") || die "Can not change to directory: $args_dict->{'output_directory'} $!\n";
		#system("tar -cvzf $args_dict->{'ref_arch'} $args_dict->{'ref_dir'}/");
		#system("tar -cvzf $args_dict->{'exec_arch'} $args_dict->{'bin_dir'}/");
		system($cmd1);
	chdir($current_directory) || die "Can not change to directory: $current_directory $!\n";
	
}


sub copy_exec {
	my ($self,$args_dict) = @_;
	#my $bin_dir_path = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}";
	my $bin_dir_path = "$args_dict->{'output_directory'}/$args_dict->{'bin_dir'}";
	my $i=0;
	foreach my $mapper (@{$args_dict->{"mapper_path"}}) {
		my $mapper_path = $args_dict->{"mapper_path"}->[$i];
		
		if ($mapper =~ /bwa/i) {
			copy($mapper_path, $bin_dir_path);
			system("chmod -R +x $bin_dir_path/");
		}
		else {
			my ( $name, $path, $extension ) = File::Basename::fileparse ( $mapper_path, '\..*' );
			$self->read_dir($path,$bin_dir_path);
		}
		
		$i++;
	}
	
	
	if (-e $args_dict->{"picard_mergesamfiles_jar"}) {
		copy($args_dict->{"picard_mergesamfiles_jar"}, "$bin_dir_path/");
	}
	
	if (-e $args_dict->{"picard_sortsam_jar"}) {
		copy($args_dict->{"picard_sortsam_jar"}, "$bin_dir_path/");
	}
	
	if (-e $args_dict->{"picard_jar"}) {
		copy($args_dict->{"picard_jar"}, "$bin_dir_path/");
	}
	
	
	if (-e $args_dict->{"trim_script_path"}) {
		copy($args_dict->{"trim_script_path"}, "$bin_dir_path/");
	}
	
	system("chmod -R +x $bin_dir_path/");

	
}


sub read_dir {
	my ($self,$indir,$outdir) = @_;
	opendir(DIR, $indir) || die("Cannot open directory");
	my @files= readdir(DIR);
	closedir(DIR);
	
	foreach my $file (@files) {
		copy("$indir"."$file",$outdir);
	}
}


sub bwa_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference.fa";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );
	my $bowtie_build = "";
	$bowtie_build = $path."bowtie-build";
	
	if (system("$mapper index $output_dir/$reference_file")!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	
}

sub gsnap_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference";
	#copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );
    
	my $gmap_build = "";
	$gmap_build = $path."gmap_build";

	if (system("$gmap_build -D $output_dir -d $reference_file $fasta_file")!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	
}


sub star_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference.fa";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	
	my $command = "$mapper --runMode genomeGenerate --genomeDir $output_dir --genomeFastaFiles $output_dir/$reference_file --runThreadN 4";
	
	if (system($command)!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	
	
}


sub bsmap_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference.fa";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	
}

sub bismark_index {
	my ($self,$fasta_file,$mapper,$output_dir,$args_dict) = @_;
	
	my $current_directory = cwd();
	
	Utility::deletedir($output_dir);
	Utility::createdir($output_dir);
	my $reference_file = "reference.fa";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );
    
	my $bismark_genome_preparation = "";
	my $bowtie_path="";
	$bismark_genome_preparation = $path."bismark_genome_preparation";
	$bowtie_path = $path."bowtie";
	
	my $bowtie_final_path="";
	$bowtie_final_path = $path;
	
	unless (-e $bowtie_path and -x $bowtie_path) {
		
		$bowtie_path = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/bowtie";
		
		unless (-e $bowtie_path and -x $bowtie_path) {
			$bowtie_path = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/bowtie2";
		}
		unless (-e $bowtie_path and -x $bowtie_path) {
			print "\n\tERROR: bowtie-build or bowtie2-build not found  in the directory \"$path\"\n\n";
			#exit(1);
		}
		
		my ( $name1, $path1, $extension1 ) = File::Basename::fileparse ( $bowtie_path, '\..*' );
		$bowtie_final_path = $path1;
	}
	
	
	
	unless (-e $bismark_genome_preparation and -x $bismark_genome_preparation) {
		print "\n\tERROR: \"$bismark_genome_preparation\" executables not found to create genome index \n\n";
		#exit(1);
	}
	$bowtie_final_path =~ s/\/$//g;
	$current_directory =~ s/\/$//g;
	$bowtie_final_path = "$current_directory/$bowtie_final_path";
	
	my $command = "$bismark_genome_preparation --path_to_bowtie $bowtie_final_path --verbose $output_dir/";
	
	
	if (system($command)!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	
}




sub bowtie_index {
	my ($self,$fasta_file,$mapper,$output_dir,$args_dict) = @_;
	my $reference_file = "reference.fa";
	my $reference_file1 = "reference";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	
	my ($name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );
	my $bowtie_build = "";
	my $bowtie2_build = "";
	my $bowtie_version = "";
	
	
	if ($name =~ /2$/) {
		$bowtie_build = $path."bowtie2-build";
	}
	else {
		$bowtie_build = $path."bowtie-build";
	}
	
	unless (-e $bowtie_build and -x $bowtie_build) {
		$bowtie_build = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/bowtie-build";

		unless (-e $bowtie_build and -x $bowtie_build) {
			$bowtie_build = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/bowtie-build2";
		}
		unless (-e $bowtie_build and -x $bowtie_build) {
			print "\n\tERROR: bowtie-build or bowtie2-build not found  in the directory \"$path\"\n\n";
			#exit(1);
		}
		
	}
		
	if (system("$bowtie_build $output_dir/$reference_file $output_dir/$reference_file1")!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}

}



sub tophat_index {
	my ($self,$fasta_file,$mapper,$output_dir,$args_dict) = @_;
	my $reference_file = "reference.fa";
	my $reference_file1 = "reference";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	my ($name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );

	unless (exists$args_dict->{'bowtie_build_version'} and -e $args_dict->{'bowtie_build_version'} and -x $args_dict->{'bowtie_build_version'} and $args_dict->{'bowtie_build_version'} ne "") {
		print "\n\tERROR: bowtie-build or bowtie2-build not found  in the directory \"$path\"\n\n";
		#exit(1);
	}
	
	my $command = "";
	$command = "$args_dict->{'bowtie_build_version'} $output_dir/$reference_file $output_dir/$reference_file1";
	
	if (system($command)!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	

}

sub soap_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference.fa";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}
	
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );
	my $bwt_builder = "";
	$bwt_builder = $path."2bwt-builder";
	system("$bwt_builder $output_dir/$reference_file");
	
	if (system("$bwt_builder $output_dir/$reference_file")!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	
}

sub novoalign_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference.nix";
	#copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	my ( $name, $path, $extension ) = File::Basename::fileparse ( $mapper, '\..*' );
    
	my $novoindex_path = "";
	$novoindex_path = $path."novoindex";
	#novoindex -k 14 -s 1 -t 9 ../reference/2R-2Mbp.nix ../reference/2R-2Mbp.fasta
	my $cmd="$novoindex_path  -k 14 -s 1 -t 2 $output_dir/$reference_file $fasta_file";
	system($cmd);
	#print "$cmd\n";
	
	if (system("$novoindex_path  -k 14 -s 1 -t 2 $output_dir/$reference_file $fasta_file")!=0) {
		print "\n\tERROR: mapper executable seems to be not compatible \"$mapper\"; give compatible version of mapper \n\n";
		#exit(1);
	}
	
}


sub ngm_index {
	my ($self,$fasta_file,$mapper,$output_dir) = @_;
	my $reference_file = "reference.fa";
	
	unless (-e "$output_dir/$reference_file") {
		copy($fasta_file,"$output_dir/$reference_file") or die "Copy failed: $!";
	}

}




1;
