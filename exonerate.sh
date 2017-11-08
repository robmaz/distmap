exonerate --model protein2genome --bestn 1 --showtargetgff --maxintron 50000 -q dpse-prot-splitted/$f -t $genome > dpse-prot-splitted/$f.exo

/Volumes/cluster/DistMap_v1.0/exonerate/exonerate-software/exonerate-2.2.0/src/program/exonerate --model protein2genome --bestn 1 --showtargetgff --maxintron 50000 -q /Volumes/cluster/DistMap_v1.0/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta -t /Volumes/cluster/DistMap_v1.0/exonerate/input/dmir-all-chromosome-r1.0.fasta > output.exo


/Volumes/cluster/DistMap_v1.0/exonerate/exonerate-software/exonerate-2.2.0/src/program/exonerate --model protein2genome --bestn 1 --showtargetgff --maxintron 50000 -q /Volumes/cluster/DistMap_v1.0/exonerate/input/Adam-PA.fa -t /Volumes/cluster/DistMap_v1.0/exonerate/input/dmir-all-chromosome-r1.0.fasta > /Volumes/cluster/DistMap_v1.0/exonerate/input/Adam-PA.exo


/Volumes/cluster/DistMap_v1.1/distmap --reference-fasta /Volumes/cluster/DistMap_v1.0/exonerate/input/dmir-all-chromosome-r1.0.fasta --input /Volumes/cluster/DistMap_v1.0/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta --mapper exonerate --mapper-path /Volumes/cluster/DistMap_v1.1/executables/exonerate --mapper-args  "--model protein2genome --bestn 1 --showtargetgff --maxintron 50000" --output /Volumes/Temp/Ram/DistMap_Exonerate1 --queue-name pg1 --job-desc "Ram exonerate test"



### distmap
### Utility.pm
### DataProcess.pm

perl /Volumes/cluster/DistMap_v1.1/read-fasta.pl --input /Volumes/cluster/DistMap_v1.0/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta --output /Volumes/cluster/DistMap_v1.0/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok_NO


############ final command
/Volumes/cluster/DistMap_v1.1/distmap --reference-fasta /Volumes/cluster/DistMap_v1.0/exonerate/input/dmir-all-chromosome-r1.0.fasta --input /Volumes/cluster/DistMap_v1.0/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta --mapper exonerate --mapper-path /Volumes/cluster/DistMap_v1.1/executables/exonerate --mapper-args  "--model protein2genome  --showtargetgff --maxintron 50000 --bestn 1" --output /Volumes/Temp/Ram/DistMap_Exonerate2 --queue-name pg1 --job-desc "Ram exonerate test"

