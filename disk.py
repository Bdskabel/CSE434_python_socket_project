import socket, json, argparse, time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("disk_name")
    ap.add_argument("manager_ip")
    ap.add_argument("manager_port", type=int)
    ap.add_argument("my_m_port", type=int)   # this process' UDP port
    ap.add_argument("my_c_port", type=int)   # reserved for future peer traffic
    args = ap.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.my_m_port))

    # Send register-disk to manager
    msg = {
        "cmd": "register-disk",
        "args": {"disk_name": args.disk_name, "ip": "127.0.0.1",
                 "m_port": args.my_m_port, "c_port": args.my_c_port}
    }
    sock.sendto(json.dumps(msg).encode(), (args.manager_ip, args.manager_port))

    # Wait for reply
    data, _ = sock.recvfrom(12000)
    print(json.loads(data.decode("utf-8")))

    print("Disk registered. Ctrl+C to exit.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

