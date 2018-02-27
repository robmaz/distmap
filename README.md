__This project is in an alpha developmental stage and it is subject to change without warning.__

# Distmap

DistMap is a wrapper around different mappers for distributed computation using Hadoop.

This repository contains a modified version for the DistMap pipeline derived from the original
implementation (see the [SourceForge DistMap project](https://sourceforge.net/projects/distmap/)).
This version shall provide an unified version that can be used on both Linux and MacOS servers
and it is no longer tied to a specific version of Hadoop (although 2.7.4 is the currently intended target).

This unified version will find a Hadoop configuration either via `HADOOP_CONF_DIR` or, possibly,
via a command line argument, and use the cluster configured there.


## Requirements

* [Perl 5](https://dev.perl.org/perl5/)
* [ReadTools](http://magicdgs.github.io/ReadTools/) >= 1.2.1
* [Picard Tools](http://broadinstitute.github.io/picard/) >= 1.124
* Mappers:
  - [bowtie](http://bowtie-bio.sourceforge.net/index.shtml)
  - [bowtie2](http://bowtie-bio.sourceforge.net/bowtie2/index.shtml)
  - [bwa](http://bio-bwa.sourceforge.net/)
  - [gmap](http://research-pub.gene.com/gmap/)
  - [novoalign](http://www.novocraft.com/products/novoalign/)


## Versioning

The _master_ branch of this repository contains versions >= 3.0.0, and releases might be found after
passing the alpha stage. Versioning from 3.0.0 will follow the Semantic Versioning conventions
([SemVer](https://semver.org/)).

We provide also the version of DistMap in the state as it is now (2.7.1), in two OS-specific branches
("macos" and "linux"). Version 3.0.0 shall provide an unified version that can be used in both
operating systems, and it is one of the major purposes of this repository.


## Citation

The original DistMap version is described in [Pandey & Schlötterer 2013](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0072614).
If you use the software in this repository, please cite as:

>> Pandey RV, Schlötterer C (2013) DistMap: A Toolkit for Distributed Short Read Mapping on a Hadoop Cluster. PLOS ONE 8(8): e72614. https://doi.org/10.1371/journal.pone.0072614
