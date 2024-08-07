#!/bin/bash

# Check if the vehicle number is passed as an argument
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <instance_number>"
    exit 1
fi

instance_number=$1

#boilerplate
export QGC_IP="127.0.0.1"
export MAVROS_IP="127.0.0.1"
export POSE_RCVR_IP="129.74.155.16"
export POSE_RCVR_PORT="9500"
export HEADLESS="1"
export PX4_HOME_LAT=41.606695229356276
export PX4_HOME_LON=-86.35561433514731
export PX4_HOME_ALT=229.0
export START_PORT="20000"


# Load configuration
export WORLD=${WORLD:-empty}
export TARGET=${TARGET:-px4_sitl_default}
export VEHICLE_MODEL="typhoon_h480"
export PX4_SIM_MODEL=${VEHICLE_MODEL}

export FIRMWARE_PATH="/home/user/Firmware"
export BUILD_PATH="${FIRMWARE_PATH}/build/${TARGET}"
export CONFIG_PATH="${BUILD_PATH}/etc/init.d-posix"

source "$CONFIG_PATH/ports.config"

# Export Gazebo Master URI
export GAZEBO_MASTER_URI="http://localhost:${drone_0_gazebo_master_port}"
echo "GAZEBO_MASTER_URI set to $GAZEBO_MASTER_URI"

function start_px4_instance() {
    local N=$1  # Instance Number
    local MODEL=$2

    local working_dir="$BUILD_PATH/instance_$N"
    mkdir -p "$working_dir" && pushd "$working_dir" &>/dev/null

    echo "Starting PX4 instance $N in $(pwd)..."
    ../bin/px4 -i $N -d "$BUILD_PATH/etc" -w "sitl_${MODEL}_${N}" -s "etc/init.d-posix/rcS"

    popd &>/dev/null
}

# Start PX4 instance
start_px4_instance $instance_number $VEHICLE_MODEL

