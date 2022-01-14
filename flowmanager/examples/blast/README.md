<img align=right src=blast.png width=256>

BLAST Workflow Example
----------------------

This directory contains the materials needed to construct a blast workflow.
However, you will first need to install the blast software and a suitable
database before you can run the makeflow.


First, obtain a blast binary suitable for your architecture. (about 30MB)
```
wget https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.12.0/ncbi-blast-2.12.0+-x64-linux.tar.gz
tar xvzf ncbi-blast-2.12.0+-x64-linux.tar.gz
```

Next, copy the main executable into the working directory.
```
cp ncbi-blast-2.12.0+/bin/blastn .
```

Check to make sure that the makeflow file is correct

```
makeflow blast.mf
```


