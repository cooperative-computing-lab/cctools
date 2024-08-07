#!/bin/bash
# Reserve port
python3 /tmp/sade/reserve_ports.py /home/user/Firmware/build/px4_sitl_default/etc/init.d-posix/ports.config 8 4600 simulator,gcs_local,gcs_remote,offboard_local,onboard_payload_local,onboard_gimbal_local,typhoon_offboard_local,gazebo_master > /tmp/log/reserve_ports.log 2> /tmp/log/reserve_ports_error.log

# Check if the ports were reserved successfully
if grep -q "ports and their PIDs written to" /tmp/log/reserve_ports.log; then
  echo "Ports reserved successfully."
else
  echo "Failed to reserve ports." >&2
  exit 1
fi

# Set permissions
chmod +x /home/user/Firmware/build/px4_sitl_default/etc/init.d-posix/ports.config > /tmp/log/chmod_port_config.log 2> /tmp/log/chmod_port_config_error.log

# Copy ports config
cp /home/user/Firmware/build/px4_sitl_default/etc/init.d-posix/ports.config /tmp/log/ports.config > /tmp/log/copy_ports_config.log 2> /tmp/log/copy_ports_config_error.log


#!/bin/bash
# Start Gazebo server
/tmp/sade/start_gazebo_server.sh > /tmp/log/gazebo_server.log 2> /tmp/log/gazebo_server_error.log &
GAZEBO_PID=$!

# Wait for Gazebo to be ready
while ! grep -q "Connected to gazebo master" /tmp/log/gazebo_server.log; do
  sleep 0.1
done

echo "Gazebo server is ready."

#!/bin/bash
# Start PX4 instance 0
/tmp/sade/start_px4_instance.sh 0 > /tmp/log/px4_0.log 2> /tmp/log/px4_0_error.log &
PX4_0_PID=$!

# Wait for PX4 instance 0 to be ready
while ! grep -q "Startup script returned successfully" /tmp/log/px4_0.log; do
  sleep 0.1
done

echo "PX4 instance 0 is ready."

# Start PX4 instance 1
/tmp/sade/start_px4_instance.sh 1 > /tmp/log/px4_1.log 2> /tmp/log/px4_1_error.log &
PX4_1_PID=$!

# Wait for PX4 instance 1 to be ready
while ! grep -q "Startup script returned successfully" /tmp/log/px4_1.log; do
  sleep 0.1
done

echo "PX4 instance 1 is ready."

#!/bin/bash
# Spawn model for PX4 instance 0
/tmp/sade/spawn_model.sh 0 > /tmp/log/spawn_model_0.log 2> /tmp/log/spawn_model_0_error.log

# Spawn model for PX4 instance 1
/tmp/sade/spawn_model.sh 1 > /tmp/log/spawn_model_1.log 2> /tmp/log/spawn_model_1_error.log

#!/bin/bash
# Start pose sender after all PX4 instances are ready
/tmp/sade/start_pose_sender.sh > /tmp/log/pose_sender.log 2> /tmp/log/pose_sender_error.log &
POSE_SENDER_PID=$!

echo "Pose sender started."
