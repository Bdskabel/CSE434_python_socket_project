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
        # configure-dss
        elif cmd == "configure-dss":
            a = msg.get("args", {})
            dss_name = a.get("dss_name")

            try:
                n = int(a.get("n"))
                b = int(a.get("striping_unit"))
            except Exception:
                n = -1
                b = -1


            if not dss_name or dss_name in dsses:
                resp = {"status":"FAILURE", "error":"bad or duplicate dss_name"}
            elif n < 3:
                resp = {"status":"FAILURE", "error":"n must be >= 3"}
            elif not (power_of_two(b) and 128 <= b <= 1024*1024):
                resp = {"status":"FAILURE", "error":"striping_unit must be a power of two in [128, 1048576]"}
            else:
                # Choose n Free disks (sorted by name)
                free = sorted([name for name,info in disks.items() if info.get("state") == "Free"])
                if len(free) < n:
                    resp = {"status":"FAILURE", "error":"fewer than n disks with state Free"}
                else:
                    chosen = free[:n]
                    for dn in chosen:
                        disks[dn]["state"] = f"InDSS:{dss_name}"
                    dsses[dss_name] = {"n": n, "striping_unit": b, "disks": chosen}
                    resp = {
                        "status": "SUCCESS",
                        "dss": {"dss_name": dss_name, "n": n, "striping_unit": b, "disks": chosen}
                    }

        else:
            resp = {"status":"FAILURE", "error":"unsupported in step 1"}

        sock.sendto(json.dumps(resp).encode(), addr)

if __name__ == "__main__":
    main()
