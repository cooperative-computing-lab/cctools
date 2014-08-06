#!/bin/sh

for f in *.rst; do
    rst2s5 --theme-url /~pbui/common/stylesheets/verde-s5 $f $(basename $f .rst).html
done
