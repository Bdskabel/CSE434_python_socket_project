import socket, json, argparse

def send(sock, mgr, msg):
    sock.sendto(json.dumps(msg).encode(), mgr)
    data, _ = sock.recvfrom(12000)
    return json.loads(data.decode("utf-8"))

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
    mgr = (args.manager_ip, args.manager_port)

    r = send(sock, mgr, {
        "cmd": "register-user",
        "args": {"user_name": args.user_name, "ip": "127.0.0.1",
                 "m_port": args.my_m_port, "c_port": args.my_c_port}
    })
    print("register-user ->", r)

    print("Type commands: ls | quit")
    while True:
        try:
            line = input("> ").strip().lower()
        except EOFError:
            break
        if line in ("quit", "exit"):
            break
        elif line == "ls":
            r = send(sock, mgr, {"cmd": "ls", "args": {}})
            print(json.dumps(r, indent=2))
        elif line == "":
            continue
        else:
            print("unknown command")

if __name__ == "__main__":
    main()
