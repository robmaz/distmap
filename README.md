# Distmap

DistMap is a wrapper around different mappers for distributed computation using Hadoop.

This repository contains a modified version for the DistMap pipeline derived from the original
implementation (see the [SourceForge DistMap project](https://sourceforge.net/projects/distmap/))
that can be used on both Linux and MacOS servers and it is no longer tied to a specific version of
Hadoop (although it was developed and tested on Hadoop 2.7.x versions).

This unified version finds a Hadoop configuration either via `HADOOP_CONF_DIR` or, possibly,
via a command line argument, and uses the cluster configured there.

From version 3 onward, distmap is no longer distributed with mapper binaries.
You need to provide binaries compatible with your cluster.

## Requirements

* [Perl 5](https://dev.perl.org/perl5/) including the File::Which module
* [ReadTools](http://magicdgs.github.io/ReadTools/) >= 1.3.1
* Mappers:
  - [bowtie2](http://bowtie-bio.sourceforge.net/bowtie2/index.shtml)
  - [bwa](http://bio-bwa.sourceforge.net/)
  - [gmap](http://research-pub.gene.com/gmap/)
  - [novoalign](http://www.novocraft.com/products/novoalign/)


## Versioning

The _master_ branch of this repository contains versions >= 3.0.0, and releases might be found after
passing the alpha stage. Versioning from 3.0.0 will follow the Semantic Versioning conventions
([SemVer](https://semver.org/)).

We provide also the old 2.7.1 version of DistMap in two OS-specific branches ("macos" and "linux").


## Citation

The original DistMap version is described in [Pandey & Schlötterer 2013](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0072614).
If you use the software in this repository, please cite as:

>> Pandey RV, Schlötterer C (2013) DistMap: A Toolkit for Distributed Short Read Mapping on a Hadoop Cluster. PLOS ONE 8(8): e72614. https://doi.org/10.1371/journal.pone.0072614
