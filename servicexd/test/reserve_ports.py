import subprocess
import socket
import os
import sys

def find_unused_port(start_port):
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("", start_port))
                return start_port
            except OSError:
                start_port += 1

def bind_port_with_nc(port):
    try:
        process = subprocess.Popen(
            ["nc", "-l", "-p", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid)
        return process.pid
    except Exception as e:
        print(f"Error binding port {port}: {e}")
        return None

def find_and_reserve_ports(count, output_file, repeat):
    reserved_ports = []
    start_port = 4560

    for r in range(repeat):
        for _ in range(count):
            port = find_unused_port(start_port)
            pid = 0
            reserved_ports.append((port, pid, r))
            start_port = port + 1

    with open(output_file, 'w') as f:
        for i, (port, pid, index) in enumerate(reserved_ports):
            port_index = i % count + 1
            f.write(
                f"port_{port_index}_{index} = {port}\npid_port_{port_index}_{index} = {pid}\n")

def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print(
            "Usage: python reserve_ports.py <count> <output_file> [<repeat>]")
        sys.exit(1)

    count = int(sys.argv[1])
    output_file = sys.argv[2]
    repeat = int(sys.argv[3]) if len(sys.argv) == 4 else 1

    find_and_reserve_ports(count, output_file, repeat)
    print(
        f"Reserved {count * repeat} ports and their PIDs written to {output_file}")

if __name__ == "__main__":
    main()
