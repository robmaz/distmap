#!/usr/bin/env perl
package ReadToolsUtils;

use strict;
use warnings;

=head1 NAME

ReadToolsUtils - Module for integration with L<ReadTools|http://magicdgs.github.io/ReadTools/>

=head1 AUTHOR

Daniel Gomez Sanchez (L<magicDGS|https://github.com/magicDGS>)

=cut


=head2 VERSION

Minimum version supported (version >= ReadTools::Version).

=cut
use constant VERSION => "1.2.1";


## get the readtools runnable command
## params:
## -
=head2 readtools_runnable_cmd()

Get the ReadTools runnable command. If the provided input
is a jar file, it will return a "java -jar" formatted string.
Otherwise, it will use the wrapper syntax from brew

Input:
	- String: readtools string (jar file or wrapper script)

Ouput:
	- String: readtools runnable command.

=cut
sub get_readtools_runnable_cmd {
	chomp(my $readtools = $_[0]);
	if ( $readtools =~ /\.jar$/ ) {
		$readtools = "eval java \\\$JAVA_OPTS -jar $readtools";
	}
	return $readtools;
}

=head2 get_trimming_args()

Get trimming argumnents for ReadTools ReadsToDistmap program.

Input:
	- Numeric: quality threshold
	- Numeric: minimum length
	- Boolean: discard remaining Ns
	- Boolean: no trim quality
	- Boolean: no trim 5'

Output:
	- String: readtools arguments for trimming.

=cut
sub get_trimming_args {
	my ($qualThreshold, $minLength, $discardRemainingNs, $noTrimQuality, $no5ptrim) = @_;
	my $trim_args = " --readFilter ReadLengthReadFilter --minReadLength $minLength --maxReadLength 99999";
	# quality trimming
	if ($noTrimQuality) {
		# ignores the qualThreshold
	} else {
		# adds the trimming quality
		$trim_args .= " --trimmer MottQualityTrimmer --mottQualityThreshold $qualThreshold";
	}
	# add the discard of remaining Ns
	if ($discardRemainingNs) {
		$trim_args .= " --readFilter AmbiguousBaseReadFilter --ambigFilterFrac 0";
	}
	if ($no5ptrim) {
		$trim_args .= " --disable5pTrim";
	}
	return $trim_args;
}


# END OF THE PACKAGE
1;
