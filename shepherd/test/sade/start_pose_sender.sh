#!/bin/bash

export POSE_RCVR_IP="129.74.155.16"
export POSE_RCVR_PORT="9500"

export TARGET=${TARGET:-px4_sitl_default}
export FIRMWARE_PATH="/home/user/Firmware"
export BUILD_PATH="${FIRMWARE_PATH}/build/${TARGET}"
export CONFIG_PATH="${BUILD_PATH}/etc/init.d-posix"

source "$CONFIG_PATH/ports.config"

# Export Gazebo Master URI
export GAZEBO_MASTER_URI="http://localhost:${drone_0_gazebo_master_port}"
echo "GAZEBO_MASTER_URI set to $GAZEBO_MASTER_URI"

send_drone_pose -p $POSE_RCVR_PORT -a $POSE_RCVR_IP

