import subprocess
import socket
import os
import sys


def find_and_reserve_ports(prefixes, output_file, repeat, start_port):
    reserved_ports = []
    next_port = start_port

    for r in range(repeat):
        for prefix_index, prefix in enumerate(prefixes):
            port = find_unused_port(next_port)

            pid = 1  # Placeholder PID
            # pid = bind_port_with_nc(port)
            # todo: check if bind is successful before adding to reserved port

            reserved_ports.append((prefix, port, pid, r))
            next_port = port + 1

    with open(output_file, 'w') as f:
        for i, (prefix, port, pid, index) in enumerate(reserved_ports):
            # print(f"drone_{index}_{prefix}_port={port}\ndrone_{index}_{prefix}_pid={pid}")
            f.write(f"drone_{index}_{prefix}_port={port}\ndrone_{index}_{prefix}_pid={pid}\n")


def find_unused_port(start_port):
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as usock:
                try:
                    sock.bind(("", start_port))
                    usock.bind(("", start_port))
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


def main():
    if len(sys.argv) < 5:
        print("Usage: python reserve_ports.py <output_file> <repeat> <start_port> <prefix>")
        sys.exit(1)

    output_file = sys.argv[1]
    repeat = int(sys.argv[2])
    start_port = int(sys.argv[3])
    prefixes = sys.argv[4].split(',')

    find_and_reserve_ports(prefixes, output_file, repeat, start_port)

    print(f"Reserved {len(prefixes) * repeat} ports and their PIDs written to {output_file}")


if __name__ == "__main__":
    main()