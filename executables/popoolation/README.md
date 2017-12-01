PoPoolation scripts
===================

DistMap supports trimming on a Hadoop cluster using the PoPoolation pipeline described in
[Kofler et al (2011)](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0015925).

Scripts in this folder are the required components from the toolkit, obtained from the repository
of PoPoolation: https://sourceforge.net/p/popoolation/wiki/Main/

Concretely, it includes the `trim-fastq.pl` script.

__Note: the current implementation also supports trimming in the cluster using the
[ReadTools pre-release (version <= 0.3.0)](https://github.com/magicDGS/ReadTools),
which implementes the same functionality.
It is preferable to use that "trimming script", which is faster than the perl implementation__

---

# IMPORTANT NOTE

*Trimmming in the Hadoop cluster with the PoPoolation script is DEPRECATED*
*This files might be removed in the future*