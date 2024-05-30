#!/bin/bash

# Check if the vehicle number is passed as an argument
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <instance_number>"
    exit 1
fi

instance_number=$1

# Constants setup
TARGET=${TARGET:-px4_sitl_default}
VEHICLE_MODEL=${VEHICLE_MODEL:-typhoon_h480}
FIRMWARE_PATH="/home/user/Firmware"
BUILD_PATH="${FIRMWARE_PATH}/build/${TARGET}"
CONFIG_PATH="${BUILD_PATH}/etc/init.d-posix"

# Load configuration for ports
source "$CONFIG_PATH/ports.config"
export GAZEBO_MASTER_URI="http://localhost:${drone_0_gazebo_master_port}"
echo "GAZEBO_MASTER_URI set to $GAZEBO_MASTER_URI"

function spawn_model() {
    local N=$1  # Instance Number
    local MODEL=$2
    local X=0.0
    local Y=$((3 * N))

    local simulator_port=$(eval echo \${drone_${N}_simulator_port})
    echo "Starting PX4 instance $N with simulator port $simulator_port"

    python3 "${FIRMWARE_PATH}/Tools/sitl_gazebo/scripts/jinja_gen.py" \
            "${FIRMWARE_PATH}/Tools/sitl_gazebo/models/${MODEL}/${MODEL}.sdf.jinja" \
            "${FIRMWARE_PATH}/Tools/sitl_gazebo" \
            --mavlink_tcp_port $simulator_port \
            --mavlink_udp_port $((14560 + N)) \
            --mavlink_id $((1 + N)) \
            --gst_udp_port $((5600 + N)) \
            --video_uri $((5600 + N)) \
            --mavlink_cam_udp_port $((14530 + N)) \
            --output-file "/tmp/${MODEL}_${N}.sdf"

    echo "Spawning ${MODEL}_${N} at X=${X}, Y=${Y}"
    gz model --spawn-file="/tmp/${MODEL}_${N}.sdf" --model-name="${MODEL}_${N}" -x $X -y $Y -z 0.83
}

# Spawn model
spawn_model $instance_number $VEHICLE_MODEL

