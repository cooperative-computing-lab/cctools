# TaskVine Example: ObsPy example

This workflow downloads some MiniSEED files from the ObsPy example repository,
processes the Z component, doing some very straightforward baseline correction (partial,
due to the fact we don't have some details about the sensor) and saves the graph as an image.

```
--8<-- "../../taskvine/src/examples/vine_example_obspy.py"
```
