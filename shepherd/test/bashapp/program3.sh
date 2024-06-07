#!/bin/bash
START_TIME=$(date +%s)
startup_delay=5

while true; do
    echo "$(date +%s) - program is booting up"
    sleep 0.5

    if [[ $(date +%s) -gt $((START_TIME + startup_delay)) ]]; then
        echo "$(date +%s) - program is ready"
        break
    fi
done

READY_TIME=$(date +%s)
run_duration=5

while true; do
    echo "$(date +%s) - program is running"
    sleep 0.5

    if [[ $(date +%s) -gt $((READY_TIME + run_duration)) ]]; then
        echo "$(date +%s) - program is completed"
        break
    fi
done
