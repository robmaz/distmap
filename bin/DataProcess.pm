#!/usr/bin/perl -w
use strict;
use warnings;
use File::Basename;

package DataProcess;
use File::Copy;
use Cwd 'abs_path';
use Archive::Tar;
use IO::Uncompress::AnyUncompress qw(anyuncompress $AnyUncompressError);
use IO::Uncompress::Gunzip;
use Cwd;
use FindBin qw/$RealBin/;
use lib "$RealBin/bin";
use Utility;
use XML::XPath;

##
# Author: Ram Vinay Pandey
#   Date: February 24, 2013
#
# This script processes FASTQ files and creates an archive for all executables to upload into HDFS file system of Hadoop cluster.
#

sub new {
    my $class = shift;
    my $self  = {};
    bless $self, $class;
    return $self;
}

sub start {
    my ( $self, $args_dict ) = @_;

    print STDERR
"=======================================================================\n";
    print STDERR
      "Step2: Converting FASTQ into tab seperated file, uploading in HDFS \n";
    print STDERR "	     and creating archieve to upload into HDFS system\n";
    print STDERR
"=======================================================================\n\n";

    ################### Time start #########################
    my $start_time       = time();
    my $time_stamp_start = Utility::get_time_stamp();
    print STDERR "\nStarted at:  $time_stamp_start\n";

    print STDERR "Writing fastq files into tab text file\n";
    $self->file_process($args_dict);

    ################### Time end #########################
    my $end_time       = time();
    my $execution_time = Utility::get_execution_time( $start_time, $end_time );
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $execution_time\n\n";

}

sub file_process {
    my ( $self, $args_dict ) = @_;

    my $files_pair_list   = [];
    my $files_single_list = [];
    ( $files_pair_list, $files_single_list ) =
      $self->check_files( $args_dict->{"input_files"}, $args_dict->{"usage"} );

    if ( scalar(@$files_pair_list) > 0 ) {
        Utility::createdir(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'fastq_dir_pe'}"
        );

        $self->fastq2tab_pe_java( $files_pair_list, $args_dict );

        my $read_type = $args_dict->{'fastq_dir_pe'};

        $args_dict->{"pe_file"} = scalar(@$files_pair_list);
    }

    if ( scalar(@$files_single_list) > 0 ) {
        Utility::createdir(
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'fastq_dir_pe'}"
        );

        $self->fastq2tab_se_java( $files_single_list, $args_dict );

        my $read_type = $args_dict->{'fastq_dir_se'};

        $args_dict->{"se_file"} = scalar(@$files_single_list);
    }
}

sub create_archive {
    my ( $self, $args_dict ) = @_;
    my $script_current_directory = abs_path($0);
    my ( $name, $path, $extension ) =
      File::Basename::fileparse( abs_path($0), '\..*' );

    my $current_directory = cwd();

    chdir("$args_dict->{'output_directory'}/$args_dict->{'random_id'}/")
      || die
"Can not change to directory: $args_dict->{'output_directory'}/$args_dict->{'random_id'} $!\n";
    system("tar -cvzf $args_dict->{'exec_arch'} $args_dict->{'bin_dir'}/");
    chdir($current_directory)
      || die "Can not change to directory: $current_directory $!\n";
}

sub compress_fastq_file {
    my ( $self, $args_dict, $read_type ) = @_;

    my $output_dir =
      "$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$read_type";

    my $script_current_directory = abs_path($0);
    my ( $name, $path, $extension ) =
      File::Basename::fileparse( abs_path($0), '\..*' );

    my $current_directory = cwd();

    chdir($output_dir) || die "Can not change to directory: $output_dir $!\n";
    system("bzip2 *");
    chdir($current_directory)
      || die "Can not change to directory: $current_directory $!\n";
}

sub check_files {
    my ( $self, $files, $usage ) = @_;

    my @files = ();
    @files = @{$files};

    my $files_pair_list   = [];
    my $files_single_list = [];

    foreach my $f (@files) {

        my $file_pair   = [];
        my $file_single = [];

        my @p = ();
        @p = split( ",", $f );
        next if scalar(@p) < 1;

        if ( scalar(@p) == 2 ) {
            if ( -e $p[0] and -s $p[0] ) {
                push( @$file_pair, $p[0] );
            }
            if ( -e $p[1] and -s $p[1] ) {
                push( @$file_pair, $p[1] );
            }
            push( @$file_pair, "pe" );
        }

        elsif ( scalar(@p) == 1 ) {
            if ( -e $p[0] and -s $p[0] ) {
                push( @$file_single, $p[0] );
            }
            push( @$file_single, "se" );
        }

        if ( scalar(@$file_pair) == 3 ) {
            push( @$files_pair_list, $file_pair );
        }
        if ( scalar(@$file_single) == 2 ) {
            push( @$files_single_list, $file_single );
        }

        $file_pair   = [];
        $file_single = [];
    }

    if ( scalar(@$files_pair_list) < 1 and scalar(@$files_single_list) < 1 ) {
        print STDERR "\t--input option not valid. Fastq files do not exists\n";
        print STDERR "$usage\n";
        exit(1);
    }

    return ( $files_pair_list, $files_single_list );
}

sub fastq2tab_pe {
    my ( $self, $file_list, $args_dict ) = @_;

    my $output_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'fastq_dir_pe'}";
    my $index = 1;
    foreach my $f (@$file_list) {
        my $output_file = "$output_dir/fastq_file" . $index;
        my $read1_fastq = $f->[0];
        my $read2_fastq = $f->[1];

        open( my $ofh, ">" . $output_file ) or die "could not open $_";

        my $fh1 = undef;
        my $fh2 = undef;

        if ( $read1_fastq =~ /\.gz$/i ) {
            open $fh1, "gzip -dc $read1_fastq |"
              or die
"Could not open file gzipped file $read1_fastq - do you have zlib $!";
        }
        else {
            open $fh1, "<", $read1_fastq
              or die "Could not open file handle, $!";
        }

        if ( $read2_fastq =~ /\.gz$/i ) {
            open $fh2, "gzip -dc $read2_fastq |"
              or die
"Could not open file gzipped file $read2_fastq - do you have zlib $!";
        }
        else {
            open $fh2, "<", $read2_fastq
              or die "Could not open file handle, $!";
        }

        my $line_f1;
        my $line_f2;
        my $counter = 0;

        while ( defined( $line_f1 = <$fh1> ) && defined( $line_f2 = <$fh2> ) ) {
            my $temp;
            $counter++;

            my $read_name = $line_f1;
            chomp($read_name);

            $read_name =~ s/(.*)(\/[0-9])/$1/;
            $temp = $read_name . "\t";

            $line_f1 = <$fh1>;
            my $seq_data = $line_f1;
            chomp($seq_data);
            $temp .= $seq_data . "\t";

            $line_f1 = <$fh1>;
            $line_f1 = <$fh1>;
            my $q_data = $line_f1;
            chomp($q_data);
            $temp .= $q_data . "\t";

            $line_f2  = <$fh2>;
            $seq_data = $line_f2;
            chomp($seq_data);
            $temp .= $seq_data . "\t";

            $line_f2 = <$fh2>;
            $line_f2 = <$fh2>;
            $q_data  = $line_f2;
            chomp($q_data);
            $temp .= $q_data . "\n";

            print $ofh $temp;
        }

        print STDERR "file$index: $counter Paired-end reads\n";
        $index++;
    }
}

sub fastq2tab_se {
    my ( $self, $file_list, $args_dict ) = @_;

    my $output_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'fastq_dir_se'}";
    my $index = 1;
    foreach my $f (@$file_list) {
        my $output_file = "$output_dir/fastq_file" . $index;
        my $read_fastq  = $f->[0];

        open( my $ofh, ">" . $output_file ) or die "could not open $_";

        my $fh = undef;
        if ( $read_fastq =~ /\.gz$/i ) {
            open $fh, "gzip -dc $read_fastq |"
              or die
"Could not open file gzipped file $read_fastq - do you have zlib $!";
        }
        else {
            open $fh, "<", $read_fastq or die "Could not open file handle, $!";
        }

        my $line_f;
        my $counter = 0;

        while ( defined( $line_f = <$fh> ) ) {

            my $temp;
            $counter++;

            my $read_name = $line_f;
            chomp($read_name);

            $read_name =~ s/(.*)(\/[0-9])/$1/;
            $temp = $read_name . "\t";

            $line_f = <$fh>;
            my $seq_data = $line_f;
            chomp($seq_data);
            $temp .= $seq_data . "\t";

            $line_f = <$fh>;
            $line_f = <$fh>;
            my $q_data = $line_f;
            chomp($q_data);
            $temp .= $q_data . "\n";

            print $ofh $temp;
        }

        print STDERR "file$index: $counter Single-end reads\n";
        $index++;
    }
}

sub fastq2tab_pe_java {
    my ( $self, $file_list, $args_dict ) = @_;
    use File::Basename;
    my $input_dir  = $args_dict->{'random_id'};
    my $input_dir1 = "/$args_dict->{'random_id'}_input/fastq_paired_end";

    my $hdfs_exe = $args_dict->{"hdfs_exe"};

    if ( system("$hdfs_exe dfs -test -d $input_dir1") == 0 ) {
        system("$hdfs_exe dfs -rm -r $input_dir1") == 0
          || warn
"Error in deleting the $input_dir1 folder on hdfs file system. $input_dir1 does not exists\n";
    }

    my $script_current_directory = abs_path($0);
    my $current_directory        = cwd();

    my $script_dirname = dirname($0);

    #my $jar_file_path = "$script_dirname/bin/JDistmap.jar";
    # ReadTools 1+ is too large to be distributed with distmap
    # my $jar_file_path = "$script_dirname/executables/ReadTools-1.1.0.jar";
    use File::Which;
    my $readtools = `which readtools`;
    unless ( -x $readtools ) die("ReadTools not found in path.");

      my $block_size = $args_dict->{"block_size"};

    $block_size =~ s/\D//g;
    my $block_size_byte = $block_size * 1024 * 1024;

    my $file_count = scalar(@$file_list);
    $file_count = $file_count + 1;
    my $output_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'fastq_dir_pe'}";
    my $index = 1;

   # my $xp    = qw(/configuration/property[string(name)="fs.defaultFS"]/value);
   # my $hdfs =
   #   XML::XPath->new( filename => "$ENV{HADOOP_CONF_DIR}/core-site.xml" )
   #   ->findvalue($xp);
    my $hdfs = `$hdfs_exe getconf -confKey "fs.defaultFS"`;

    foreach my $f (@$file_list) {
        my $output_file = "$output_dir/fastq_file" . $index;
        my $read1_fastq = $f->[0];
        my $read2_fastq = $f->[1];

#my $cmd = "java -Xmx4g -jar $jar_file_path UploadFastq HADOOP_HOME=$args_dict->{'hadoop_home'} I=$read1_fastq I=$read2_fastq O=file:///$input_dir BZ2=true BLOCK_SIZE=$block_size_byte MAPPER=BWA";
        my $cmd =
"JAVA_OPTS=-Xmx4g $readtools ReadsToDistmap --input $read1_fastq --input2 $read2_fastq --output $hdfs/$input_dir1/fastq_file1.bz2 --hdfsBlockSize $block_size_byte";
        print "$cmd\n";
        system($cmd);

        my $source_file      = "$input_dir1/fastq_file1.bz2";
        my $destination_file = "$input_dir1/fastq_file" . $file_count . ".bz2";

#$hdfs_exe dfs -mv /JDistmap1_input_input/fastq_paired_end/fastq_file1.bz2 /JDistmap1_input_input/fastq_paired_end/fastq_file2.bz2

        my $cmd1 =
          "$args_dict->{'hdfs_exe'} dfs -mv $source_file $destination_file";
        system($cmd1);

        $file_count--;
    }
}

sub fastq2tab_se_java {
    my ( $self, $file_list, $args_dict ) = @_;
    use File::Basename;
    my $input_dir  = $args_dict->{'random_id'};
    my $input_dir1 = "/$args_dict->{'random_id'}_input/fastq_single_end";

    my $hdfs_exe = $args_dict->{"hdfs_exe"};

    if ( system("$hdfs_exe dfs -test -d $input_dir1") == 0 ) {
        system("$hdfs_exe dfs -rm -r $input_dir1") == 0
          || warn
"Error in deleting the $input_dir1 folder on hdfs file system. $input_dir1 does not exists\n";
    }

    my $script_current_directory = abs_path($0);
    my $current_directory        = cwd();

    my $script_dirname = dirname($0);

    # my $jar_file_path  = "$script_dirname/bin/JDistmap.jar";
    # ReadTools 1+ is too large to be distributed with distmap
    # my $jar_file_path = "$script_dirname/executables/ReadTools-1.1.0.jar";
    use File::Which;
    my $readtools = `which readtools`;
    unless ( -x $readtools ) die("ReadTools not found in path.");

      my $block_size = $args_dict->{"block_size"};

    $block_size =~ s/\D//g;
    my $block_size_byte = $block_size * 1024 * 1024;

    my $file_count = scalar(@$file_list);
    $file_count = $file_count + 1;
    my $output_dir =
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'fastq_dir_se'}";
    my $index = 1;
    my $hdfs  = `$hdfs_exe getconf -confKey "fs.defaultFS"`;
    foreach my $f (@$file_list) {
        my $output_file = "$output_dir/fastq_file" . $index;
        my $read1_fastq = $f->[0];

# my $cmd = "java -Xmx4g -jar $jar_file_path UploadFastq HADOOP_HOME=$args_dict->{'hadoop_home'} I=$read1_fastq O=$input_dir BZ2=true BLOCK_SIZE=$block_size_byte MAPPER=BWA";
        my $cmd =
"JAVA_OPTS=-Xmx4g $readtools ReadsToDistmap --input $read1_fastq --output $hdfs/$input_dir/fastq_file1.bz2 --hdfsBlockSize $block_size_byte";
        system($cmd);

        my $source_file      = "$input_dir1/fastq_file1.bz2";
        my $destination_file = "$input_dir1/fastq_file" . $file_count . ".bz2";

        my $cmd1 =
          "$args_dict->{'hdfs_exe'} dfs -mv $source_file $destination_file";

        system($cmd1);

        $file_count--;
    }
}

sub copy_exec {
    my ( $self, $args_dict ) = @_;

    my $i = 0;
    foreach my $mapper ( @{ $args_dict->{"mapper"} } ) {
        my $mapper_path = $args_dict->{"mapper_path"}->[$i];

        if ( $mapper =~ /bwa/i ) {
            copy( $mapper_path,
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
            );
            system(
"chmod -R +x $args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
            );
        }
        else {
            my ( $name, $path, $extension ) =
              File::Basename::fileparse( $mapper_path, '\..*' );
            $self->read_dir( $path,
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}"
            );
        }

        $i++;
    }
    if ( -e $args_dict->{"picard_mergesamfiles_jar"} ) {
        copy( $args_dict->{"picard_mergesamfiles_jar"},
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
        );
    }
    if ( -e $args_dict->{"picard_sortsam_jar"} ) {
        copy( $args_dict->{"picard_sortsam_jar"},
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
        );
    }
    if ( -e $args_dict->{"picard_mark_duplicates_jar"} ) {
        copy( $args_dict->{"picard_mark_duplicates_jar"},
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
        );
    }
    if ( -e $args_dict->{"picard_jar"} ) {
        copy( $args_dict->{"picard_jar"},
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
        );
    }
    if ( -e $args_dict->{"trim_script_path"} ) {
        copy( $args_dict->{"trim_script_path"},
"$args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
        );
    }

    system(
"chmod -R +x $args_dict->{'output_directory'}/$args_dict->{'random_id'}/$args_dict->{'bin_dir'}/"
    );

}

sub read_dir {
    my ( $self, $indir, $outdir ) = @_;
    opendir( DIR, $indir ) || die("Cannot open directory");
    my @files = readdir(DIR);
    closedir(DIR);

    foreach my $file (@files) {
        copy( "$indir" . "$file", $outdir );
    }
}

1;
