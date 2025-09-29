from __future__ import annotations
import sys, argparse
from common import bind_udp, recv_json, send_json, SUCCESS, FAILURE, log

# First manager file: supports register-user and register-disk only.
class DiskInfo:
    def __init__(self, name, ip, m_port, c_port):
        self.name = name
        self.ip = ip
        self.m_port = m_port
        self.c_port = c_port
        self.state = "Free"

class UserInfo:
    def __init__(self, name, ip, m_port, c_port):
        self.name = name
        self.ip = ip
        self.m_port = m_port
        self.c_port = c_port

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("manager_port", type=int, help="UDP port to listen on")
    args = ap.parse_args()

    sock = bind_udp(args.manager_port)
    users = {}
    disks = {}

    log(f"Step 1 manager listening on UDP {args.manager_port}")
    while True:
        msg, addr = recv_json(sock)
        cmd = msg.get("cmd")
        corr = msg.get("corr")
        if cmd == "register-user":
            a = msg.get("args", {})
            name = a.get("user_name")
            if not name or name in users:
                send_json(sock, addr, {"corr": corr, "status": FAILURE, "error": "duplicate or bad user_name"})
            else:
                users[name] = UserInfo(name, a.get("ip"), a.get("m_port"), a.get("c_port"))
                send_json(sock, addr, {"corr": corr, "status": SUCCESS})
        elif cmd == "register-disk":
            a = msg.get("args", {})
            name = a.get("disk_name")
            if not name or name in disks:
                send_json(sock, addr, {"corr": corr, "status": FAILURE, "error": "duplicate or bad disk_name"})
            else:
                disks[name] = DiskInfo(name, a.get("ip"), a.get("m_port"), a.get("c_port"))
                send_json(sock, addr, {"corr": corr, "status": SUCCESS})
        else:
            send_json(sock, addr, {"corr": corr, "status": FAILURE, "error": "unsupported in step 1"})

if __name__ == "__main__":
    main()

