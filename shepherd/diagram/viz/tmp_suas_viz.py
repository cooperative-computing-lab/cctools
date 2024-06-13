import sys
import json
from graphviz import Digraph, Source


def generate_dot(config, state_transitions):
    dot = Digraph(comment='Workflow Visualization')

    colors = {
        'service': 'lightblue',
        'action': 'lightblue',
    }

    # Add subgraphs for each service with their state transitions
    for service, details in config['services'].items():
        service_type = details.get('type', 'action')  # Default to 'action' if type is not specified
        node_color = colors.get(service_type, 'lightgrey')  # Default color if no specific type is found

        with dot.subgraph(name=f'cluster_{service}') as sub:
            sub.attr(style='filled', color='lightgrey')
            sub.node_attr.update(style='filled', color=node_color)
            states = state_transitions.get(service, {})

            # Add nodes for each state
            for state, time in states.items():
                sub.node(f'{service}_{state}', f'{state}\n{time:.2f}s')

            # Add edges between states
            state_list = list(states.keys())
            for i in range(len(state_list) - 1):
                sub.edge(f'{service}_{state_list[i]}', f'{service}_{state_list[i + 1]}')

            sub.attr(label=service)

    # Add edges based on dependencies
    for service, details in config['services'].items():
        dependencies = details.get('dependency', {}).get('items', {})
        for dep, state in dependencies.items():
            dot.edge(f'{dep}_{state}', f'{service}_started', label=f'{dep} {state}')

    return dot.source


def render_dot(dot_source, output_filename='sade-suas'):
    src = Source(dot_source)
    src.format = 'png'
    src.render(output_filename, view=True)


if __name__ == '__main__':
    config = {
        "services": {
            "reserve_port": {
                "type": "action",
                "command": "python3 /tmp/scripts/reserve_ports.py /home/user/Firmware/build/px4_sitl_default/etc/init.d-posix/ports.config 2 20010 simulator,gcs_local,gcs_remote,offboard_local,onboard_payload_local,onboard_gimbal_local,typhoon_offboard_local,gazebo_master",
                "stdout_path": "/tmp/log/reserve_ports.log",
                "stderr_path": "/tmp/log/reserve_ports_error.log",
                "state": {
                    "log": {
                        "complete": "ports and their PIDs written to"
                    }
                }
            },
            "chmod_port_config": {
                "type": "action",
                "command": "chmod +x /home/user/Firmware/build/px4_sitl_default/etc/init.d-posix/ports.config",
                "stdout_path": "/tmp/log/chmod_port_config.log",
                "stderr_path": "/tmp/log/chmod_port_config_error.log",
                "dependency": {
                    "items": {
                        "reserve_port": "final"
                    }
                }
            },
            "copy_ports_config": {
                "command": "cp /home/user/Firmware/build/px4_sitl_default/etc/init.d-posix/ports.config /tmp/log/ports.config",
                "stdout_path": "/tmp/log/copy_ports_config.log",
                "stderr_path": "/tmp/log/copy_ports_config_error.log",
                "monitor_log": 'false',
                "dependency": {
                    "items": {
                        "chmod_port_config": "final"
                    }
                }
            },
            "gazebo_server": {
                "type": "service",
                "command": "/tmp/scripts/start_gazebo_server.sh",
                "stdout_path": "/tmp/log/gazebo_server.log",
                "stderr_path": "/tmp/log/gazebo_server_error.log",
                "state": {
                    "log": {
                        "ready": "Connected to gazebo master"
                    }
                },
                "dependency": {
                    "items": {
                        "chmod_port_config": "final"
                    }
                }
            },
            "px4_instance_0": {
                "type": "service",
                "command": "/tmp/scripts/start_px4_instance.sh 0",
                "stdout_path": "/tmp/log/px4_0.log",
                "stderr_path": "/tmp/log/px4_0_error.log",
                "state": {
                    "log": {
                        "waiting_for_simulator": "Waiting for simulator to accept connection",
                        "ready": "Startup script returned successfully"
                    }
                },
                "dependency": {
                    "items": {
                        "gazebo_server": "ready"
                    }
                }
            },
            "spawn_model_0": {
                "type": "action",
                "command": "/tmp/scripts/spawn_model.sh 0",
                "stdout_path": "/tmp/log/spawn_model_0.log",
                "stderr_path": "/tmp/log/spawn_model_0_error.log",
                "dependency": {
                    "items": {
                        "px4_instance_0": "waiting_for_simulator"
                    }
                }
            },
            "px4_instance_1": {
                "type": "service",
                "command": "/tmp/scripts/start_px4_instance.sh 1",
                "stdout_path": "/tmp/log/px4_1.log",
                "stderr_path": "/tmp/log/px4_1_error.log",
                "state": {
                    "log": {
                        "waiting_for_simulator": "Waiting for simulator to accept connection",
                        "ready": "Startup script returned successfully"
                    }
                },
                "dependency": {
                    "items": {
                        "gazebo_server": "ready"
                    }
                }
            },
            "spawn_model_1": {
                "type": "action",
                "command": "/tmp/scripts/spawn_model.sh 1",
                "stdout_path": "/tmp/log/spawn_model_1.log",
                "stderr_path": "/tmp/log/spawn_model_1_error.log",
                "dependency": {
                    "items": {
                        "px4_instance_1": "waiting_for_simulator"
                    }
                }
            },
            "pose_sender": {
                "type": "service",
                "command": "/tmp/scripts/start_pose_sender.sh",
                "stdout_path": "/tmp/log/pose_sender.log",
                "stderr_path": "/tmp/log/pose_sender_error.log",
                "dependency": {
                    "items": {
                        "px4_instance_0": "ready",
                        "px4_instance_1": "ready"
                    }
                }
            }
        },
        "output": {
            "state_times": "/tmp/log/state_times.json"
        },
        "stop_signal": "/tmp/log/stop.txt"
    }

    state_transitions = {
        "reserve_port": {
            "initialized": 0.020361661911010742,
            "started": 0.02051997184753418,
            "complete": 0.04095315933227539,
            "success": 0.12297582626342773,
            "final": 0.12353825569152832
        },
        "chmod_port_config": {
            "initialized": 0.02258157730102539,
            "started": 0.12559270858764648,
            "success": 0.22781109809875488,
            "final": 0.2281782627105713
        },
        "copy_ports_config": {
            "initialized": 0.02147674560546875,
            "started": 0.22913479804992676,
            "success": 0.33168983459472656,
            "final": 0.3327939510345459
        },
        "gazebo_server": {
            "initialized": 0.021079063415527344,
            "started": 0.2294480800628662,
            "ready": 0.9342648983001709,
            "stopped": 57.39571189880371,
            "final": 57.395782470703125
        },
        "px4_instance_0": {
            "initialized": 0.022138595581054688,
            "started": 0.9354474544525146,
            "waiting_for_simulator": 1.046609878540039,
            "ready": 17.815176725387573,
            "stopped": 57.403639793395996,
            "final": 57.40375828742981
        },
        "spawn_model_0": {
            "initialized": 0.023059844970703125,
            "started": 1.0479681491851807,
            "success": 17.476674556732178,
            "final": 17.476802110671997
        },
        "px4_instance_1": {
            "initialized": 0.023921966552734375,
            "started": 0.9361538887023926,
            "waiting_for_simulator": 1.0474460124969482,
            "ready": 18.012163639068604,
            "stopped": 57.40846657752991,
            "final": 57.408539056777954
        },
        "spawn_model_1": {
            "initialized": 0.023538589477539062,
            "started": 1.048147201538086,
            "success": 17.475992918014526,
            "final": 17.476157426834106
        },
        "pose_sender": {
            "initialized": 0.024396896362304688,
            "started": 18.012657403945923,
            "stopped": 57.35790205001831,
            "final": 57.357980489730835
        }
    }

    dot = generate_dot(config, state_transitions)
    print(dot)
    render_dot(dot)
