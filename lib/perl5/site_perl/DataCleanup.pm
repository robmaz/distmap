#!/usr/bin/env perl
use strict;
use warnings;
package DataCleanup;
use FindBin qw/$RealBin/;
use lib "$RealBin/../lib/perl5/site_perl";
use Utility;



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


    #### Cleaning local temperory output files/folders
    $self->clean_local_files($args_dict);

    #### Cleaning hdfs input and output files/folders
    $self->clean_hdfs_files($args_dict);



    ################### Time end #########################
    my $end_time = time();
    my $executation_time = Utility::get_executation_time($start_time,$end_time);
    my $time_stamp_end = Utility::get_time_stamp();
    print STDERR "Finished at: $time_stamp_end\n";
    print STDERR "Duration: $executation_time\n\n";

}



sub clean_hdfs_files {
    my ($self,$args_dict) = @_;

    my $input =  "/$args_dict->{'random_id'}"."_input";
    my $output =  "/$args_dict->{'random_id'}"."_output";
    my $hadoop_exe = $args_dict->{'hadoop_exe'};
    my $hdfs_exe = $args_dict->{'hdfs_exe'};

    #print "$input\t$output\n";
    system("$hdfs_exe dfs -rm -r $input") == 0 || warn "Error in deleting the $input folder on hdfs file system. $input does not exists\n";
    system("$hdfs_exe dfs -rm -r $output") == 0 || warn "Error in deleting the $output folder on hdfs file system. $input does not exists\n";

}


sub clean_local_files {
	my ($self,$args_dict) = @_;

	my $dir1 = "$args_dict->{'output_directory'}/$args_dict->{'random_id'}";
	my $dir2 = "$args_dict->{'output_directory'}/ref";
	my $dir3 = "$args_dict->{'output_directory'}/bin";
	#print "$args_dict->{'output_directory'}/$args_dict->{'random_id'}\n";
	Utility::deletedir($dir1);
	Utility::deletedir($dir2);
	Utility::deletedir($dir3);

}

1;
