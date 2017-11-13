# Distmap development

Development of the DistMap pipeline. Branches "macos" and "linux" contain
Ram's DistMap_v2.7.1 as found on vetgrid and vetlinux computers and are intended
to be there only for reference, while "master" shall provide a unified version that
can be used on both Linux and MacOS servers and is no longer tied to a specific
version of Hadoop (although 2.7.4 is the currently intended target). This unified
version finds a Hadoop configuration via HADOOP_HOME or a command line argument,
and use the cluster configured there. The command line argument takes precedence.
The Hadoop utilities used internally should respect HADOOP_CONF_DIR.

Current milestones are to

+ remove all hardcoded paths and replace them with environment variables (done)
+ use Daniel's readtools wherever applicable (done for fastq2tab uploads)
+ identify the tricks used to send jobs from vetgrids to the Linux cluster and
  sanitize this behavior
+ remove outdated unused binaries, archives with no apparent purpose, hard-coded
  Hadoop configuration info, unused scripts/modules etc. (partly done)
+ remove third-party software and rather find it in the PATH or the hdfs (ReadTools!)
+ add a Homebrew formula

so that this can become a normal easily installable package.
