set output "filename.ps"
set terminal postscript
set logscale x
set logscale y
plot "res" using 1:2 title 'Checkpoint data size' with lines, "res" using 1:3 title 'Log size between checkpoints' with lines
