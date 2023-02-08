# Image and script to statically link parrot_run with cvmfs

To build the image with docker:

```sh
docker build --tag="cclnd/parrot-run-static-build" --no-cache=true --force-rm .
```

To compile `parrot_run`, mount the `cctools` directory at `/cctools` and execute the image, e.g.:

```sh
docker run --rm -v /path/to/cctools:/cctools docker.io/cclnd/parrot-run-static-build
```

The `parrot_run` executable will be generated at `/path/to/cctools/parrot_run`

## Notes

1. The `cctools` should be at a local directory (i.e., AFS won't work).
2. The linker will output some expected warnings, like:
```
warning: Using 'getservbyport_r' in statically linked applications requires at runtime the shared libraries from the glibc version used for linking
```
3. docker.io/cclnd/parrot-run-static-build:2023-02-03 should not be deleted or modified, as it will serve as a backup in case the image cannot be generated again.




