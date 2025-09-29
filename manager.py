# Step 1 manager: supports register-user and register-disk
import socket, json, argparse
import random

def power_of_two(x: int) -> bool:
    return x > 0 and (x & (x - 1)) == 0

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
    dsses = {} 
    busy = {"op": None, "dss": None, "user": None} 

    # Ensure this runs
    print(f"Manager listening on UDP {args.manager_port}")
    while True:
        data, addr = sock.recvfrom(12000)  # 12 KB, might need to up this?
        msg = json.loads(data.decode("utf-8"))
        cmd = msg.get("cmd","")

        if busy["op"] is not None:
            allowed = "copy-complete" if busy["op"] == "copy" else "decommission-complete"
            if cmd != allowed:
                resp = {"status": "FAILURE", "error": f"busy: {busy['op']} in progress"}
                sock.sendto(json.dumps(resp).encode(), addr)
                continue
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
                    chosen = random.sample(free, n)
                    for dn in chosen:
                        disks[dn]["state"] = f"InDSS:{dss_name}"
                    dsses[dss_name] = {"n": n, "striping_unit": b, "disks": chosen, "files": {}}
                    resp = {
                        "status": "SUCCESS",
                        "dss": {"dss_name": dss_name, "n": n, "striping_unit": b, "disks": chosen}
                    }

        elif cmd == "ls":
            if not dsses:
                resp = {"status": "FAILURE", "error": "no DSS configured"}
            else:
                listing = {
                    "users": sorted(users.keys()),
                    "disks": [{"name": n, "state": disks[n]["state"]} for n in sorted(disks.keys())],
                    "dsses": [
                        {
                            "dss_name": dn,
                            "n": dsses[dn]["n"],
                            "striping_unit": dsses[dn]["striping_unit"],
                            "disks": dsses[dn]["disks"],
                            "files": dsses[dn]["files"],
                        }
                        for dn in sorted(dsses.keys())
                    ],
                    "free_disks": [n for n in sorted(disks.keys()) if disks[n]["state"] == "Free"],
                }
                resp = {"status": "SUCCESS", "listing": listing}
        elif cmd == "copy-prepare":
            a = msg.get("args", {})
            dss_name = a.get("dss_name")
            file_name = a.get("file_name")
            owner = a.get("owner")
            
        
            dss = dsses.get(dss_name)
            if not dss:
                resp = {"status": "FAILURE", "error": "no such dss"}
            else:
                disk_eps = []
                for dn in dss["disks"]:
                    info = disks.get(dn)
                    disk_eps.append({"disk_name": dn, "ip": info["ip"], "c_port": info["c_port"]})
                resp = {
                    "status": "SUCCESS",
                    "dss": {
                        "dss_name": dss_name,
                        "n": dss["n"],
                        "striping_unit": dss["striping_unit"],
                        "disks": disk_eps
                    }
                }
        
        elif cmd == "copy-complete":
            a = msg.get("args", {})
            dss_name  = a.get("dss_name")
            file_name = a.get("file_name")
            owner     = a.get("owner")
            size      = a.get("size")
        
            dss = dsses.get(dss_name)
            if not dss:
                resp = {"status": "FAILURE", "error": "no such dss"}
            else:
                try:
                    size = int(size)
                except Exception:
                    size = -1
                if size < 0:
                    resp = {"status": "FAILURE", "error": "invalid size"}
                else:
                    dss["files"][file_name] = {"owner": owner, "size": size}
                    resp = {"status": "SUCCESS"}
        elif cmd == "read-prepare":
            a = msg.get("args", {})
            dss_name  = a.get("dss_name")
            file_name = a.get("file_name")
        
            dss = dsses.get(dss_name)
            if not dss:
                resp = {"status": "FAILURE", "error": "no such dss"}
            else:
                meta = dss["files"].get(file_name)
                if not meta:
                    resp = {"status": "FAILURE", "error": "file not found"}
                else:
                    disk_eps = []
                    for dn in dss["disks"]:
                        info = disks.get(dn)
                        disk_eps.append({"disk_name": dn, "ip": info["ip"], "c_port": info["c_port"]})
                    resp = {
                        "status": "SUCCESS",
                        "dss": {
                            "dss_name": dss_name,
                            "n": dss["n"],
                            "striping_unit": dss["striping_unit"],
                            "disks": disk_eps
                        },
                        "file": {"name": file_name, "size": meta["size"], "owner": meta["owner"]}
                    }
        
        elif cmd == "read-complete":
            resp = {"status": "SUCCESS"}
        elif cmd == "deregister-user":
            a = msg.get("args", {})
            name = a.get("user_name")
            if name not in users:
                resp = {"status": "FAILURE", "error": "no such user"}
            else:
                del users[name]
                resp = {"status": "SUCCESS"}
        elif cmd == "deregister-disk":
            a = msg.get("args", {})
            name = a.get("disk_name")
            info = disks.get(name)
            if not info:
                resp = {"status": "FAILURE", "error": "no such disk"}
            elif info["state"] != "Free":
                resp = {"status": "FAILURE", "error": "disk is InDSS; cannot deregister"}
            else:
                del disks[name]
                resp = {"status": "SUCCESS"}
        elif cmd == "decommission-dss":
            a = msg.get("args", {})
            dss_name = a.get("dss_name")
            dss = dsses.get(dss_name)
            if not dss:
                resp = {"status": "FAILURE", "error": "no such dss"}
            else:
                busy.update({"op": "decommission", "dss": dss_name, "user": a.get("user_name")})
                disk_eps = []
                for dn in dss["disks"]:
                    info = disks.get(dn)
                    disk_eps.append({"disk_name": dn, "ip": info["ip"], "c_port": info["c_port"]})
                resp = {
                    "status": "SUCCESS",
                    "dss": {
                        "dss_name": dss_name,
                        "n": dss["n"],
                        "striping_unit": dss["striping_unit"],
                        "disks": disk_eps
                    }
                }
        
        elif cmd == "decommission-complete":
            a = msg.get("args", {})
            dss_name = a.get("dss_name")
            if busy["op"] != "decommission" or busy["dss"] != dss_name:
                resp = {"status": "FAILURE", "error": "no decommission in progress"}
            else:
                dss = dsses.get(dss_name)
                if not dss:
                    resp = {"status": "FAILURE", "error": "no such dss"}
                else:
                    for dn in dss["disks"]:
                        if dn in disks:
                            disks[dn]["state"] = "Free"
                    del dsses[dss_name]
                    resp = {"status": "SUCCESS"}
                busy.update({"op": None, "dss": None, "user": None})
        else:
            resp = {"status":"FAILURE", "error":"unsupported"}

        sock.sendto(json.dumps(resp).encode(), addr)

if __name__ == "__main__":
    main()
