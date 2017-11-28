exonerate --model protein2genome --bestn 1 --showtargetgff --maxintron 50000 -q dpse-prot-splitted/$f -t $genome > dpse-prot-splitted/$f.exo

${DISTMAP_HOME_10}/exonerate/exonerate-software/exonerate-2.2.0/src/program/exonerate --model protein2genome --bestn 1 --showtargetgff --maxintron 50000 -q ${DISTMAP_HOME_10}/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta -t ${DISTMAP_HOME_10}/exonerate/input/dmir-all-chromosome-r1.0.fasta > output.exo


${DISTMAP_HOME_10}/exonerate/exonerate-software/exonerate-2.2.0/src/program/exonerate --model protein2genome --bestn 1 --showtargetgff --maxintron 50000 -q ${DISTMAP_HOME_10}/exonerate/input/Adam-PA.fa -t ${DISTMAP_HOME_10}/exonerate/input/dmir-all-chromosome-r1.0.fasta > ${DISTMAP_HOME_10}/exonerate/input/Adam-PA.exo


${DISTMAP_HOME_11}/distmap --reference-fasta ${DISTMAP_HOME_10}/exonerate/input/dmir-all-chromosome-r1.0.fasta --input ${DISTMAP_HOME_10}/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta --mapper exonerate --mapper-path ${DISTMAP_HOME_11}/executables/exonerate --mapper-args  "--model protein2genome --bestn 1 --showtargetgff --maxintron 50000" --output /Volumes/Temp/Ram/DistMap_Exonerate1 --queue-name pg1 --job-desc "Ram exonerate test"



### distmap
### Utility.pm
### DataProcess.pm

perl ${DISTMAP_HOME_11}/read-fasta.pl --input ${DISTMAP_HOME_10}/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta --output ${DISTMAP_HOME_10}/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok_NO


############ final command
${DISTMAP_HOME_11}/distmap --reference-fasta ${DISTMAP_HOME_10}/exonerate/input/dmir-all-chromosome-r1.0.fasta --input ${DISTMAP_HOME_10}/exonerate/input/dpse-all-translation-r2.23-longest-geneheaderok.fasta --mapper exonerate --mapper-path ${DISTMAP_HOME_11}/executables/exonerate --mapper-args  "--model protein2genome  --showtargetgff --maxintron 50000 --bestn 1" --output /Volumes/Temp/Ram/DistMap_Exonerate2 --queue-name pg1 --job-desc "Ram exonerate test"

