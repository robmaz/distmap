rm -rf libexec/linux/gmap-2015-12-31/src libexec/linux/gmap-2016-09-14/src libexec/macos/gmap-2015-12-31/src libexec/macos/gmap-2016-09-14/src
rm -rf libexec/linux/bwa-0.7.13 libexec/macos/bwa-0.7.13
rm -rf libexec/linux/samtools-1.3 libexec/macos/samtools-1.3
find libexec/ -name "*.c" -o -name "*.h" -o -name "*.o" -exec rm -f {} \;
