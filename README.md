# Distmap development

Development of the DistMap pipeline. Branches "macos" and "linux" contain
Ram's DistMap_v2.7.1 as found on vetgrid and vetlinux computers and are intended
to be there only for reference, while "master" shall provide a unified version that
can be used on both Linux and MacOS servers and is no longer tied to a specific
version of Hadoop (although 2.7.4 is the currently intended target). This unified
version will find a Hadoop configuration either via HADOOP_CONF_DIR or, possibly,
via a command line argument, and use the cluster configured there.

Current milestones are to

+ remove all hardcoded paths and replace them with environment variables (mostly,
  but mabe not optimally, done)
+ use Daniel's readtools wherever applicable (partly done)
+ identify the tricks used to send jobs from vetgrids to the Linux cluster and
  sanitize this behavior
+ remove outdated unused binaries, archives with no apparent purpose, hard-coded
  Hadoop configuration info, unused scripts/modules etc.
+ remove third-party software and rather find it in the PATH or the hdfs
+ add a Homebrew formula

so that this can become a normal easily installable package.
