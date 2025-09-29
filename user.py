import socket, json, argparse, time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("user_name")
    ap.add_argument("manager_ip")
    ap.add_argument("manager_port", type=int)
    ap.add_argument("my_m_port", type=int)
    ap.add_argument("my_c_port", type=int)
    args = ap.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.my_m_port))
    msg = {
        "cmd": "register-user",
        "args": {"user_name": args.user_name, "ip": "127.0.0.1",
                 "m_port": args.my_m_port, "c_port": args.my_c_port}
    }
    sock.sendto(json.dumps(msg).encode(), (args.manager_ip, args.manager_port))

    data, _ = sock.recvfrom(12000)
    print(json.loads(data.decode("utf-8")))

    print("User registered. Ctrl+C to exit.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

