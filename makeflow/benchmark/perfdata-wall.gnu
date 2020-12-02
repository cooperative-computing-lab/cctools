#!/usr/bin/gnuplot


reset


set terminal png
set output 'perfdata-wall.png'

set yrange [0:8000]
set y2range [0:200] 
set y2tics
set key left top
set border linewidth 1.5
set xtics rotate by 90 right
set style line 1 \
    linetype 1 linewidth 2 \
    pointsize 1.5

#set logscale xy
set xlabel "Jobs"
set ylabel "Wall Time (s)"
set y2label "Wall Time (s)"
set title "Makeflow Performance With Increase in Jobs"

plot 'perfdata.dat' using 1:2 with points title "Initial Run" ls 7 lc rgb "blue" axes x1y1, 'perfdata_recovery.dat' using 1:2 with points title "Recovery Time" ls 6 lc rgb "red" axes x1y2
