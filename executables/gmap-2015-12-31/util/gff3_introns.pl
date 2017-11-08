#! /usr/bin/perl

use warnings;

use IO::File;
use Getopt::Std;
undef $opt_C;			# If provided, will keep only introns with canonical splice sites.  Requires -d flag.
undef $opt_R;			# If provided, will report only introns with non-canonical splice sites to stdout.  Requires -d flag.
undef $opt_2;			# If provided, will print dinucleotides at splice sites.  Requires -d flag.
undef $opt_D;			# Genome directory
undef $opt_d;			# Genome index
undef $opt_Q;			# Quiet (nothing to stderr)
getopts("D:d:CR2Q");


if (defined($opt_d)) {
    if (!defined($opt_C) && !defined($opt_R) && !defined($opt_2)) {
	print STDERR "-d flag useful only with -C, -R, or -2 flags.  Ignoring -d flag\n";
	undef $opt_d;
    } else {
	if (0) {
	    $FP = new IO::File(">&STDOUT");
	} elsif (defined($opt_D)) {
	    $FP = new IO::File("| $ENV{DISTMAP_HOME}/executables/gmap-2015-12-31/bin/get-genome -D $opt_D -d $opt_d > get-genome.out");
	} else {
	    $FP = new IO::File("| $ENV{DISTMAP_HOME}/executables/gmap-2015-12-31/bin/get-genome -d $opt_d > get-genome.out");
	}

	$gene_name = "";
	$last_transcript_id = "";
	@exons = ();
	@CDS_regions = ();
	while (defined($line = <>)) {
	    if ($line =~ /^\#/) {
		# Skip
	    } elsif ($line !~ /\S/) {
		# Skip blank line
	    } else {
		$line =~ s/\r\n/\n/;
		push @lines,$line;
		chop $line;
		@fields = split /\t/,$line;

		if ($fields[2] eq "gene") {
		    if ($#exons > 0) {
			# Handle last mRNA of previous gene
			query_dinucleotides(\@exons,$chr,$strand,$FP);
			#print_exons(\@exons,$gene_name,$last_transcript_id,$chr,$strand,$FP);
		    } elsif ($#CDS_regions > 0) {
			# Handle last mRNA of previous gene
			query_dinucleotides(\@CDS_regions,$chr,$strand,$FP);
			#print_exons(\@CDS_regions,$gene_name,$last_transcript_id,$chr,$strand,$FP);
		    }

		    ($gene_name) = $fields[8] =~ /ID=([^;]+)/;
		    $chr = $fields[0];
		    @exons = ();
		    @CDS_regions = ();

		} elsif ($fields[2] eq "mRNA" || $fields[2] eq "transcript") {
		    if ($#exons > 0) {
			query_dinucleotides(\@exons,$chr,$strand,$FP);
			#print_exons(\@exons,$gene_name,$last_transcript_id,$chr,$strand,$FP);
		    } elsif ($#CDS_regions > 0) {
			query_dinucleotides(\@CDS_regions,$chr,$strand,$FP);
			#print_exons(\@CDS_regions,$gene_name,$last_transcript_id,$chr,$strand,$FP);
		    }

		    ($last_transcript_id) = $fields[8] =~ /ID=([^;]+)/;
		    $strand = $fields[6];

		    # For GFF3 files without a gene line
		    if (!defined($gene_name) || $gene_name !~ /\S/) {
			$gene_name = $last_transcript_id;
		    }
		    $chr = $fields[0];
		    @exons = ();
		    @CDS_regions = ();

		} elsif ($fields[2] eq "exon") {
		    push @exons,"$fields[3] $fields[4]";
		} elsif ($fields[2] eq "CDS") {
		    push @CDS_regions,"$fields[3] $fields[4]";
		}
	    }
	}
    }

    if ($#exons > 0) {
	query_dinucleotides(\@exons,$chr,$strand,$FP);
    } elsif ($#CDS_regions > 0) {
	query_dinucleotides(\@CDS_regions,$chr,$strand,$FP);
    }

    close($FP);

    $FP = new IO::File("get-genome.out") or die "Cannot open get-genome.out";

} else {
    if (defined($opt_C)) {
	print STDERR "-C flag requires you to specify -d flag.  Ignoring -C flag\n";
	undef $opt_C;
    }
    if (defined($opt_R)) {
	print STDERR "-R flag requires you to specify -d flag.  Ignoring -R flag\n";
	undef $opt_R;
    }
    if (defined($opt_2)) {
	print STDERR "-2 flag requires you to specify -d flag.  Ignoring -2 flag\n";
	undef $opt_2;
    }
}



$gene_name = "";
$last_transcript_id = "";
@exons = ();
@CDS_regions = ();
while (defined($line = get_line())) {
    if ($line =~ /^\#/) {
	# Skip
    } elsif ($line !~ /\S/) {
	# Skip blank line
    } else {
	$line =~ s/\r\n/\n/;
	chop $line;
	@fields = split /\t/,$line;

	if ($fields[2] eq "gene") {
	    if ($#exons > 0) {
		# Handle last mRNA of previous gene
		print_exons(\@exons,$gene_name,$last_transcript_id,$chr,$strand,$FP);
	    } elsif ($#CDS_regions > 0) {
		# Handle last mRNA of previous gene
		print_exons(\@CDS_regions,$gene_name,$last_transcript_id,$chr,$strand,$FP);
	    }

	    ($gene_name) = $fields[8] =~ /ID=([^;]+)/;
	    $chr = $fields[0];
	    @exons = ();
	    @CDS_regions = ();

	} elsif ($fields[2] eq "mRNA" || $fields[2] eq "transcript") {
	    if ($#exons > 0) {
		print_exons(\@exons,$gene_name,$last_transcript_id,$chr,$strand,$FP);
	    } elsif ($#CDS_regions > 0) {
		print_exons(\@CDS_regions,$gene_name,$last_transcript_id,$chr,$strand,$FP);
	    }

	    ($last_transcript_id) = $fields[8] =~ /ID=([^;]+)/;
	    $strand = $fields[6];

	    # For GFF3 files without a gene line
	    if (!defined($gene_name) || $gene_name !~ /\S/) {
		$gene_name = $last_transcript_id;
	    }
	    if (!defined($chr) || $chr !~ /\S/) {
		$chr = $fields[0];
	    }

	    @exons = ();
	    @CDS_regions = ();

	} elsif ($fields[2] eq "exon") {
	    push @exons,"$fields[3] $fields[4]";
	} elsif ($fields[2] eq "CDS") {
	    push @CDS_regions,"$fields[3] $fields[4]";
	}
    }
}

if ($#exons > 0) {
    print_exons(\@exons,$gene_name,$last_transcript_id,$chr,$strand,$FP);
} elsif ($#CDS_regions > 0) {
    print_exons(\@CDS_regions,$gene_name,$last_transcript_id,$chr,$strand,$FP);
}

if (defined($opt_d)) {
    close($FP);
}

exit;


sub get_line {
    my $line;

    if (!defined($opt_d)) {
	if (!defined($line = <>)) {
	    return;
	} else {
	    return $line;
	}
    } else {
	if ($#lines < 0) {
	    return;
	} else {
	    $line = shift @lines;
	    return $line;
	}
    }
}



sub get_dinucleotide {
    my ($query, $FP) = @_;
    my $dinucl;
    my $line;
    my $lastline;

    while (defined($line = <$FP>) && $line !~ /^\# Query: $query\s*$/) {
	if ($line =~ /^\# End\s*$/) {
	    print STDERR "line is $line\n";
	    die "Could not find query $query";
	}
    }

    while (defined($line = <$FP>) && $line !~ /^\# End\s*$/) {
	if ($line =~ /^\# Query: /) {
	    die "Could not find query $query";
	}
	$lastline = $line;
    }

    if (!defined($line)) {
	die "File ended while looking for query $query";
    }

    ($dinucl) = $lastline =~ /(\S\S)/;
    if (!defined($dinucl) || $dinucl !~ /\S/) {
	die "Could not find dinucl in lastline $line for query $query";
    }

    return $dinucl;
}


sub ascending_cmp {
    ($starta) = $a =~ /(\d+) \d+/;
    ($startb) = $b =~ /(\d+) \d+/;
    return $starta <=> $startb;
}

sub get_intron_bounds_plus {
    my ($exons) = @_;
    my @starts = ();
    my @ends = ();

    foreach $exon (sort ascending_cmp (@ {$exons})) {
	($start,$end) = $exon =~ /(\d+) (\d+)/;
	push @starts,$start;
	push @ends,$end;
    }

    shift @starts;
    pop @ends;

    return (\@starts,\@ends);
}

sub get_intron_bounds_minus {
    my ($exons) = @_;
    my @starts = ();
    my @ends = ();

    foreach $exon (reverse sort ascending_cmp (@ {$exons})) {
	($start,$end) = $exon =~ /(\d+) (\d+)/;
	push @starts,$start;
	push @ends,$end;
    }

    pop @starts;
    shift @ends;

    return (\@starts,\@ends);
}


sub query_dinucleotides {
    my ($exons, $chr, $strand, $FP) = @_;

    $nexons = $#{$exons} + 1;
    if ($strand eq "+") {
	($starts,$ends) = get_intron_bounds_plus($exons);
	for ($i = 0; $i < $nexons - 1; $i++) {
	    $query = sprintf("%s:%u..%u",$chr,$ends[$i]+1,$ends[$i]+2);
	    print $FP $query . "\n";

	    $query = sprintf("%s:%u..%u",$chr,$starts[$i]-2,$starts[$i]-1);
	    print $FP $query . "\n";
	}

    } elsif ($strand eq "-") {
	($starts,$ends) = get_intron_bounds_minus($exons);
	for ($i = 0; $i < $nexons - 1; $i++) {
	    $query = sprintf("%s:%u..%u",$chr,$starts[$i]-1,$starts[$i]-2);
	    print $FP $query . "\n";

	    $query = sprintf("%s:%u..%u",$chr,$ends[$i]+2,$ends[$i]+1);
	    print $FP $query . "\n";
	}
    }
    
    return;
}



sub donor_okay_p {
    my ($donor_dinucl, $acceptor_dinucl) = @_;

    if ($donor_dinucl eq "GT") {
	return 1;
    } elsif ($donor_dinucl eq "GC") {
	return 1;
    } elsif ($donor_dinucl eq "AT" && $acceptor_dinucl eq "AC") {
	return 1;
    } else {
	return 0;
    }
}

sub acceptor_okay_p {
    my ($donor_dinucl, $acceptor_dinucl) = @_;

    if ($acceptor_dinucl eq "AG") {
	return 1;
    } elsif ($donor_dinucl eq "AT" && $acceptor_dinucl eq "AC") {
	return 1;
    } else {
	return 0;
    }
}


sub print_exons {
    my ($exons, $gene_name, $transcript_id, $chr, $strand, $FP) = @_;

    $nexons = $#{$exons} + 1;
    if ($strand eq "+") {
	($starts,$ends) = get_intron_bounds_plus($exons);
	for ($i = 0; $i < $nexons - 1; $i++) {
	    if (($intron_length = $ {$starts}[$i] - $ {$ends}[$i] - 1) < 0) {
		printf STDERR "gene %s, transcript %s, intron %d with donor at %s:%u and acceptor at %s:%u implies a negative intron length of %d...skipping\n",$gene_name,$transcript_id,$i+1,$chr,$ {$ends}[$i],$chr,$ {$starts}[$i],$intron_length;
	    } elsif (!defined($opt_d)) {
		printf ">%s.%s.intron%d/%d %s:%u..%u\n",$gene_name,$transcript_id,$i+1,$nexons-1,$chr,$ {$ends}[$i],$ {$starts}[$i];
	    } else {
		$query = sprintf("%s:%u..%u",$chr,$ends[$i]+1,$ends[$i]+2);
		$donor_dinucl = get_dinucleotide($query,$FP);

		$query = sprintf("%s:%u..%u",$chr,$starts[$i]-2,$starts[$i]-1);
		$acceptor_dinucl = get_dinucleotide($query,$FP);

		if (defined($opt_C) && donor_okay_p($donor_dinucl,$acceptor_dinucl) == 0) {
		    if (!defined($opt_Q)) {
			printf STDERR "Skipping non-canonical donor $donor_dinucl, intron length %d for %s.%s.intron%d/%d on plus strand\n",
			$intron_length,$gene_name,$transcript_id,$i+1,$nexons-1;
		    }
		} elsif (defined($opt_C) && acceptor_okay_p($donor_dinucl,$acceptor_dinucl) == 0) {
		    if (!defined($opt_Q)) {
			printf STDERR "Skipping non-canonical acceptor $acceptor_dinucl, intron length %d for %s.%s.intron%d/%d on plus strand\n",
			$intron_length,$gene_name,$transcript_id,$i+1,$nexons-1;
		    }
		} elsif (defined($opt_R)) {
		    if (donor_okay_p($donor_dinucl,$acceptor_dinucl) == 0 || acceptor_okay_p($donor_dinucl,$acceptor_dinucl) == 0) {
			printf ">%s.%s.intron%d/%d %s:%u..%u",$gene_name,$transcript_id,$i+1,$nexons-1,$chr,$ {$ends}[$i],$ {$starts}[$i];
			print " $donor_dinucl-$acceptor_dinucl";
			print "\n";
		    }
		} else {
		    printf ">%s.%s.intron%d/%d %s:%u..%u",$gene_name,$transcript_id,$i+1,$nexons-1,$chr,$ {$ends}[$i],$ {$starts}[$i];
		    if (defined($opt_2)) {
			print " $donor_dinucl-$acceptor_dinucl";
		    }
		    print "\n";
		}
	    }
	}

    } elsif ($strand eq "-") {
	($starts,$ends) = get_intron_bounds_minus($exons);
	for ($i = 0; $i < $nexons - 1; $i++) {
	    if (($intron_length = $ {$starts}[$i] - $ {$ends}[$i] - 1) < 0) {
		printf STDERR "gene %s, transcript %s, intron %d with donor at %s:%u and acceptor at %s:%u implies a negative intron length of %d...skipping\n",$gene_name,$transcript_id,$i+1,$chr,$ {$starts}[$i],$chr,$ {$ends}[$i],$intron_length;
	    } elsif (!defined($opt_d)) {
		printf ">%s.%s.intron%d/%d %s:%u..%u\n",$gene_name,$transcript_id,$i+1,$nexons-1,$chr,$ {$starts}[$i],$ {$ends}[$i];
	    } else {
		$query = sprintf("%s:%u..%u",$chr,$starts[$i]-1,$starts[$i]-2);
		$donor_dinucl = get_dinucleotide($query,$FP);

		$query = sprintf("%s:%u..%u",$chr,$ends[$i]+2,$ends[$i]+1);
		$acceptor_dinucl = get_dinucleotide($query,$FP);

		if (defined($opt_C) && donor_okay_p($donor_dinucl,$acceptor_dinucl) == 0) {
		    if (!defined($opt_Q)) {
			printf STDERR "Skipping non-canonical donor $donor_dinucl, intron length %d for %s.%s.intron%d/%d on minus strand\n",
			$intron_length,$gene_name,$transcript_id,$i+1,$nexons-1;
		    }
		} elsif (defined($opt_C) && acceptor_okay_p($donor_dinucl,$acceptor_dinucl) == 0) {
		    if (!defined($opt_Q)) {
			printf STDERR "Skipping non-canonical acceptor $acceptor_dinucl, intron length %d for %s.%s.intron%d/%d on minus strand\n",
			$intron_length,$gene_name,$transcript_id,$i+1,$nexons-1;
		    }
		} elsif (defined($opt_R)) {
		    if (donor_okay_p($donor_dinucl,$acceptor_dinucl) == 0 || acceptor_okay_p($donor_dinucl,$acceptor_dinucl) == 0) {
			printf ">%s.%s.intron%d/%d %s:%u..%u",$gene_name,$transcript_id,$i+1,$nexons-1,$chr,$ {$starts}[$i],$ {$ends}[$i];
			print " $donor_dinucl-$acceptor_dinucl";
			print "\n";
		    }
		} else {
		    printf ">%s.%s.intron%d/%d %s:%u..%u",$gene_name,$transcript_id,$i+1,$nexons-1,$chr,$ {$starts}[$i],$ {$ends}[$i];
		    if (defined($opt_2)) {
			print " $donor_dinucl-$acceptor_dinucl";
		    }
		    print "\n";
		}
	    }
	}
    }
    
    return;
}

