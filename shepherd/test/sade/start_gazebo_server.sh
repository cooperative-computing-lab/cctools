#!/bin/bash

export QGC_IP="127.0.0.1"
export MAVROS_IP="127.0.0.1"
export POSE_RCVR_IP="129.74.155.16"
export POSE_RCVR_PORT="9500"
export HEADLESS="1"
export PX4_HOME_LAT=41.606695229356276
export PX4_HOME_LON=-86.35561433514731
export PX4_HOME_ALT=229.0
export START_PORT="20000"

export WORLD=${WORLD:-empty}
export TARGET=${TARGET:-px4_sitl_default}
export VEHICLE_MODEL="typhoon_h480"
export PX4_SIM_MODEL=${VEHICLE_MODEL}

export FIRMWARE_PATH="/home/user/Firmware"
export BUILD_PATH="${FIRMWARE_PATH}/build/${TARGET}"

# Set default values for environment variables if not already set
world=${WORLD:=empty}
target=${TARGET:=px4_sitl_default}
src_path=${FIRMWARE_PATH:=/home/user/Firmware}
config_path=${src_path}/build/${target}/etc/init.d-posix/ports.config

# Source the Gazebo setup script
source ${src_path}/Tools/setup_gazebo.bash ${src_path} ${BUILD_PATH}

# To use gazebo_ros ROS2 plugins
if [[ -n "$ROS_VERSION" ]] && [ "$ROS_VERSION" == "2" ]; then
    ros_args="-s libgazebo_ros_init.so -s libgazebo_ros_factory.so"
else
    ros_args=""
fi

# Read the ports.config file and set the GAZEBO_MASTER_URI environment variable
if [ -f "$config_path" ]; then
    source "$config_path"
    export GAZEBO_MASTER_URI="http://localhost:${drone_0_gazebo_master_port}"
    echo "GAZEBO_MASTER_URI set to $GAZEBO_MASTER_URI"
else
    echo "ports.config not found"
    exit 1
fi

# Start the Gazebo server
echo "Starting gazebo server ..."
#resource_monitor -O /tmp/log/gazebo_resource -- gzserver ${src_path}/Tools/sitl_gazebo/worlds/${world}.world --verbose $ros_args
gzserver ${src_path}/Tools/sitl_gazebo/worlds/${world}.world --verbose $ros_args
