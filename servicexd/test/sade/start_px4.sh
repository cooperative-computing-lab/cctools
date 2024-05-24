#!/bin/bash

function spawn_model() {
	MODEL=$1
	N=$2 #Instance Number
	X=$3
	Y=$4

	GID=$(((SGE_TASK_ID - 1)*2 + N))

	X=${X:=0.0}
	Y=$((GID*2))

	SUPPORTED_MODELS=("iris" "plane" "standard_vtol" "rover" "r1_rover" "typhoon_h480")
	if [[ " ${SUPPORTED_MODELS[*]} " != *"$MODEL"* ]];
	then
		echo "ERROR: Currently only vehicle model $MODEL is not supported!"
		echo "       Supported Models: [${SUPPORTED_MODELS[@]}]"
		trap "cleanup" SIGINT SIGTERM EXIT
		exit 1
	fi

	working_dir="$build_path/instance_$n"
	[ ! -d "$working_dir" ] && mkdir -p "$working_dir"

	pushd "$working_dir" &>/dev/null
	echo "starting instance $N in $(pwd)"

	../bin/px4 -i $N -d "$build_path/etc" -w sitl_${MODEL}_${N} -s etc/init.d-posix/rcS >/tmp/log/px4_out_$N.log 2>/tmp/log/px4_err_$N.log &

	eval "simulator_port=\${drone_${N}_simulator_port}"

	echo "Starting px4 instance $N with simulator port $simulator_port"

	echo "python3 ${src_path}/Tools/sitl_gazebo/scripts/jinja_gen.py \
		   	${src_path}/Tools/sitl_gazebo/models/${MODEL}/${MODEL}.sdf.jinja \
		   	${src_path}/Tools/sitl_gazebo \
			--mavlink_tcp_port $simulator_port \
			--mavlink_udp_port $((14560+${N})) \
			--mavlink_id $((1+${N})) \
			--gst_udp_port $((5600+${N})) \
			--video_uri $((5600+${N})) \
			--mavlink_cam_udp_port $((14530+${N})) \
			--output-file /tmp/${MODEL}_${N}.sdf"

	python3 ${src_path}/Tools/sitl_gazebo/scripts/jinja_gen.py \
		   	${src_path}/Tools/sitl_gazebo/models/${MODEL}/${MODEL}.sdf.jinja \
		   	${src_path}/Tools/sitl_gazebo \
			--mavlink_tcp_port $simulator_port \
			--mavlink_udp_port $((14560+${N})) \
			--mavlink_id $((1+${N})) \
			--gst_udp_port $((5600+${N})) \
			--video_uri $((5600+${N})) \
			--mavlink_cam_udp_port $((14530+${N})) \
			--output-file /tmp/${MODEL}_${N}.sdf

	echo "Spawning ${MODEL}_${N} at ${X} ${Y}"

	gz model --spawn-file=/tmp/${MODEL}_${N}.sdf --model-name=${MODEL}_${N}_gid_${GID} -x ${X} -y ${Y} -z 0.83

	sleep 0.1

	popd &>/dev/null
}

export WORLD=${WORLD:-empty}
export TARGET=${TARGET:-px4_sitl_default}
export VEHICLE_MODEL="typhoon_h480"
export PX4_SIM_MODEL=${VEHICLE_MODEL}

export FIRMWARE_PATH="/home/user/Firmware"
export BUILD_PATH="${FIRMWARE_PATH}/build/${TARGET}"
export CONFIG_PATH="${BUILD_PATH}/etc/init.d-posix"

n=0
src_path="${FIRMWARE_PATH}"
target=${TARGET}
build_path=${BUILD_PATH}

num_vehicles=${NUM_VEHICLES}
vehicle_model=${VEHICLE_MODEL}

source "$CONFIG_PATH/ports.config"

echo export GAZEBO_MASTER_URI="http://localhost:${drone_0_gazebo_master_port}"
export GAZEBO_MASTER_URI="http://localhost:${drone_0_gazebo_master_port}"


spawn_model $VEHICLE_MODEL $n
