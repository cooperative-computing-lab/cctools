### How to use
1. Generate Work Queue R Package by running
```bash
make
```

2. run the example r script by running
```bash
cd work_queue_example
Rscript work_queue_example.R a b c
```
make sure `work_queue_worker` is in the PATH, open another terminal and run
```bash
work_queue_worker localhost 9123
```
if all goes well, output files (i.e. a.gz, b.gz and c.gz) will be generated.

3. clean up
```bash
cd ..
make clean
```
