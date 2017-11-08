#!/usr/bin/perl

# Date: 31-01-2011
# tina

use strict;
use warnings;

use FindBin qw($RealBin);
use lib "$RealBin/Modules";

use Getopt::Long;
use Pod::Usage;
use POSIX;

##popGenTools packages:
use GffGtfParser qw(get_characteristics_of_genome_gff_pileup print_variance_for_feature);
 
my $help = 0;
my $test="";

###default settings:

my $MEASURE="all";
my $PILEUP_FILE;
my $GFF_FILE;
my $OUT_FILE;
my $QUAL_ENCODING = "illumina";
my $MIN_COUNT = 2;
my $MIN_QUAL = 20;
my $POOL_SIZE = 0;
my $MIN_COV = 4;
my $MAX_COV = 100000;
my $UNCORRECTED = 0;


GetOptions(
	"measure=s"=>\$MEASURE,
	"input=s"=>\$PILEUP_FILE,
	"gff=s"=>\$GFF_FILE,
	"output=s"=>\$OUT_FILE,
	"fastq-type=s"=>\$QUAL_ENCODING, 	
	"min-count=i"=>\$MIN_COUNT, 
	"min-qual=i"=>\$MIN_QUAL,
	"pool-size=i"=>\$POOL_SIZE,
	"min-coverage=i"=>\$MIN_COV,
	"max-coverage=i"=>\$MAX_COV,
	"dissable-corrections"=>\$UNCORRECTED,
	"test=s"=>\$test,
	"help"=>\$help
) or die "Invalid arguments, use --help.";

	pod2usage(-verbose=>2) if $help;
	pod2usage(-msg=>"Could not find pileup file",-verbose=>1) unless -e $PILEUP_FILE;
	pod2usage(-msg=>"Could not find gff/gtf file",-verbose=>1) unless -e $GFF_FILE;
	if ($POOL_SIZE == 0){pod2usage(-msg=>"Do not specified required option --pool-size  ",-verbose=>1)};
	
	my $outFileHandle;
	my $paramsFile;

	if (defined($OUT_FILE)){
		open $outFileHandle, ">", $OUT_FILE or die "Could not write an output to $OUT_FILE";
		$paramsFile = $OUT_FILE.".params";
	}else{
		$outFileHandle = *STDOUT;
		$paramsFile = "STDERR";	
	}
	
	my $startTime = POSIX::strftime("%m/%d/%Y %H:%M:%S\n", localtime);

	my $ptrGenomeCharacteristics = get_characteristics_of_genome_gff_pileup(
                              $GFF_FILE, $PILEUP_FILE,
                              $QUAL_ENCODING, $MIN_COUNT, $MIN_COV, $MAX_COV, $MIN_QUAL,
			                        $POOL_SIZE,$MEASURE, $UNCORRECTED);
	
	print_variance_for_feature($ptrGenomeCharacteristics, $outFileHandle, $MEASURE, $0, $paramsFile);
		
	my $endTime = POSIX::strftime("%m/%d/%Y %H:%M:%S\n", localtime);
	
	print_input_params($MEASURE, $PILEUP_FILE, $GFF_FILE, $OUT_FILE, $QUAL_ENCODING, $MIN_COUNT, $MIN_QUAL, $POOL_SIZE, $MIN_COV, $MAX_COV, $UNCORRECTED, $startTime, $endTime);


sub print_input_params{
	my ($MEASURE, $PILEUP_FILE, $GFF_FILE, $OUT_FILE, $QUAL_ENCODING, $MIN_COUNT, $MIN_QUAL, $POOL_SIZE, $MIN_COV, $MAX_COV, $UNCORRECTED, $startTime, $endTime)=@_;	
	
	my $paramsFileHandle;
	
	if (defined($OUT_FILE)){
		open $paramsFileHandle, ">", $OUT_FILE."params" or die "Could not create params output file ".$OUT_FILE."params"; 		
	}else{
		$paramsFileHandle = *STDERR or die "Could not write params to STDERR";	
	}
	
	print $paramsFileHandle "data generated by script $0,\n";	
	print $paramsFileHandle "start time: ", $startTime;
	print $paramsFileHandle "end time: ", $endTime;
	print $paramsFileHandle "calculated measure: ", $MEASURE, "\n";
	print $paramsFileHandle "using input pileup file: ", $PILEUP_FILE, "\n";
	print $paramsFileHandle "using input gff file: ", $GFF_FILE, "\n";
	if (defined $OUT_FILE){
		print $paramsFileHandle "output file: ", $OUT_FILE, "\n";
	}else{
		print $paramsFileHandle "output file: STDOUT\n";
	}
	print $paramsFileHandle "fastq-type: ", $QUAL_ENCODING, "\n";
	print $paramsFileHandle "min-count: ", $MIN_COUNT, "\n";
	print $paramsFileHandle "min-qual: ", $MIN_QUAL, "\n";
	print $paramsFileHandle "pool-size: ", $POOL_SIZE, "\n";						
	print $paramsFileHandle "min-coverage: ", $MIN_COV, "\n";
	print $paramsFileHandle "max-coverage: ", $MAX_COV, "\n";
	print $paramsFileHandle "dissable-corrections: ", $UNCORRECTED, "\n";
}



=head1 NAME

variance-for-feature.pl - Parses input files (GFF/GTF and pileup) of one population NGS data 
and reports for each feature (e.g. exon) whole-genome characteristics (e.g. sum of 
lengths of all exons in given data). For more details about features and characteristics
see section DETAILS.

=head1 SYNOPSIS

perl variance-for-feature.pl --gff-file file1 --pileup-file file2 --pool-size 100 > output

We require that data in the GFF/GTF file are all from the same source. We also require that for 
each chromosome in the GFF/GTF file is feature chromosome_arm provided in the file. See section DETAILS.

=head1 OPTIONS

The required options:

=over

=item B<--gff-file> - A path to the input gff/gtf file.

=item B<--pileup-file> - A path to the input pileup file.

=item B<--pool-size> - Size of your pool.

=back

The options that have default values alredy set and if the setting is fine for you there is no need to set them again:

=over 10 

=item B<--qual-encoding> - The encoding of quality characters.

Must either be 'sanger' or 'illumina'; 

default = illumina

=item B<--min-count> - The minimum count of nucleotides of the same type that are needed for SNP identification. 

default = 2

=item B<--min-cov> - The minimum coverage at a SNP site.

default = 4

=item B<--max-cov> - The maximum coverage at a SNP site. 

default = 100000

=item B<--min-qual> - The minimum quality at a SNP site.

default = 20

=back

The options that leads to unit testing or help page: 

=over 9

=item B<--test> - Run the unit tests for this script.

=over

=item  --test prove 

Runs prove unit tests, suggested. 

=item --test perl

Runs more detailed perl unit test.

=back

=item B<--help> - Display help for this script

#new stuff 
=item <--measure> one of possible choices "pi", "theta", "d", "all", if all is selected, then all 3 measures are calculated and printed at once


=back 

=head1 INPUT

=head2 GFF/GTF file format

File in GFF/GTF format contains an annotation of a genomic data.
Each of GFF/GTF files consists of at least eight columns that are separated by tabs. The columns contain following data:

1. column: chromosome name

2. column: source of a genomic data

3. column: feature

4. column: feature's start position

5. column: feature's end position

6. column: score

7. column: strand

8. column: frame

Other data in GFF/GTF file are not important for this script. For details see for example http://www.sanger.ac.uk/resources/software/gff/spec.html

=head2 pileup file format

See for example http://samtools.sourceforge.net/pileup.shtml

=head1 OUTPUT

Data in output file consist of comented header with (1) input parameters and (2) columns description. 
Below header, calculated variance measures are placed in tab separated columns, one line for each feature.

=head1 DETAILS

This script parses input files (GFF/GTF and pileup) of one population NGS data 
and reports for each feature (e.g. exon) whole-genome characteristics (e.g. a sum of 
lengths of all exons in the given data). 

We require that data in the GFF/GTF file are all from the same source. You can get such a data for example by using this perl oneliner:

perl -lane 'if ($F[1] eq "source_name"){print $_}' < input_gff_file > output_gff_file

We also require that for each chromosome in the GFF/GTF file is a feature chromosome_arm provided in the file.
If chromosome_arm features are missing in your data, you can fix it by adding line in the following format for each chromosome:

_chromosome_name_\t_data_source_\tchromosome_arm\t_start_\t_end_\t.\t.\t.\n

Replace _chromosome_name_, _data_source_, _start_ and _end_ by real data.

Lists of reported features and characteristics follows.

=head2 Reported features of genomic data

=over

=item intron

=item exon 

=item ncRNA

=item tRNA 

=item snoRNA

=item snRNA

=item rRNA

=item intergenic - those regions that are no one of the previous

=item CDS

=item five_prime_UTR

=item three_prime_UTR

=item enhancer

=item miRNA

=item regulatory_region

=item pseudogene

=item transposable_element

=item pre_miRNA

=back

These features are defined in GffGtfParser.pm hashes %featHash and %inverseFeatHash.

=head2 Reported characteristics:

=over

=item B<total length> - A sum of lengths of all regions of the feature,

=item B<covered length> - A sum of lengths of all covered (at least min-cov times) regions of the feature,

=item B<pi> - Average value of pi (?) over all covered positions of the feature, 
 
=item B<theta> - Average value of theta (?) over all covered positions of the feature,
          
=item B<D> - Average value of D (?) over all covered positions of the feature,

=back
