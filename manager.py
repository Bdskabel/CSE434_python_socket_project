# Step 1 manager: supports register-user and register-disk
import socket, json, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("manager_port", type=int)
    args = ap.parse_args()

    # Create & bind UDP socket like homework 2 (to listen)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.manager_port)) 

    # Both of the users and disks will need to send over name, IP, m_port, and c_port
    users = {}  
    disks = {}   

    # Ensure this runs
    print(f"Manager listening on UDP {args.manager_port}")
    while True:
        data, addr = sock.recvfrom(12000)  # 12 KB, might need to up this?
        msg = json.loads(data.decode("utf-8"))
        cmd = msg.get("cmd","")
        # Handle the register-user command
        if cmd == "register-user":
            a = msg.get("args", {})
            name = a.get("user_name")
            if not name or name in users:
                resp = {"status":"FAILURE", "error":"duplicate or bad user_name"}
            else:
                users[name] = {"ip":a.get("ip"), "m_port":a.get("m_port"), "c_port":a.get("c_port")}
                resp = {"status":"SUCCESS"}
        # Handle the register-disk command
        elif cmd == "register-disk":
            a = msg.get("args", {})
            name = a.get("disk_name")
            if not name or name in disks:
                resp = {"status":"FAILURE", "error":"duplicate or bad disk_name"}
            else:
                disks[name] = {"ip":a.get("ip"), "m_port":a.get("m_port"), "c_port":a.get("c_port"), "state":"Free"}
                resp = {"status":"SUCCESS"}
        else:
            resp = {"status":"FAILURE", "error":"unsupported in step 1"}

        sock.sendto(json.dumps(resp).encode(), addr)

if __name__ == "__main__":
    main()
