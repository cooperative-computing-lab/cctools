#!/usr/bin/gnuplot


reset


set terminal png
set output 'perfdata-write.png'

set key left top
set yrange [0:.2]
set xtics rotate by 90 right
set border linewidth 1.5
set style line 1 \
    linetype 1 linewidth 2 \
    pointsize 1.5

#set logscale xy
set xlabel "Jobs"
set ylabel "MB written"
set title "Makeflow Performance With Increase in Jobs"

plot 'perfdata.dat' using 1:6 with points title "Initial Run" ls 7 lc rgb "blue", 'perfdata_recovery.dat' using 1:6 with points title "Recovery Time" ls 6 lc rgb "red"
