import socket, json, argparse, time
import threading, base64

def guess_my_ip(to_ip: str, to_port: int) -> str:
    """Derive outward-facing local IP by opening a UDP 'connect' to manager."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((to_ip, to_port))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

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

    my_ip = guess_my_ip(args.manager_ip, args.manager_port)
    
    # Send register-disk to manager
    msg = {
        "cmd": "register-disk",
        "args": {"disk_name": args.disk_name, "ip": my_ip,
                 "m_port": args.my_m_port, "c_port": args.my_c_port}
    }
    print({"trace": "send", "to": (args.manager_ip, args.manager_port), "msg": msg})
    sock.sendto(json.dumps(msg).encode(), (args.manager_ip, args.manager_port))

    # Wait for reply
    data, _ = sock.recvfrom(12000)
    resp = json.loads(data.decode("utf-8"))
    print({"trace": "recv", "from": "manager", "resp": resp})

    c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    c_sock.bind(("0.0.0.0", args.my_c_port))

    # in the memory make sure to store: (file_name, stripe_idx, disk_index) in bytes
    store = {}
    mode = {"state": "normal"}

    def content_loop():
        while True:
            data2, addr2 = c_sock.recvfrom(65535)
            try:
                msg2 = json.loads(data2.decode("utf-8"))
            except Exception:
                c_sock.sendto(json.dumps({"status": "FAILURE", "error": "bad json"}).encode(), addr2)
                continue

            if msg2.get("cmd") == "write-block":
                a2 = msg2.get("args", {})
                file_name  = a2.get("file_name")
                stripe_idx = a2.get("stripe_idx")
                disk_index = a2.get("disk_index")
                block_b64  = a2.get("block_b64")

                ok = True
                try:
                    stripe_idx = int(stripe_idx)
                    disk_index = int(disk_index)
                except Exception:
                    ok = False

                if not (ok and file_name and isinstance(block_b64, str)):
                    resp2 = {"status": "FAILURE", "error": "missing/invalid fields"}
                else:
                    try:
                        block = base64.b64decode(block_b64.encode("ascii"))
                        store[(file_name, stripe_idx, disk_index)] = block
                        resp2 = {"status": "SUCCESS"}
                    except Exception as e:
                        resp2 = {"status": "FAILURE", "error": f"decode error: {e}"}

                c_sock.sendto(json.dumps(resp2).encode(), addr2)
            elif msg2.get("cmd") == "read-block":
                a2 = msg2.get("args", {})
                file_name  = a2.get("file_name")
                stripe_idx = a2.get("stripe_idx")
                disk_index = a2.get("disk_index")

                if mode["state"] == "fail":
                    c_sock.sendto(json.dumps({"status": "FAILURE", "error": "simulated failure"}).encode(), addr2)
                    continue

                ok = True
                try:
                    stripe_idx = int(stripe_idx)
                    disk_index = int(disk_index)
                except Exception:
                    ok = False

                key = (file_name, stripe_idx, disk_index)
                if not ok or not file_name or key not in store:
                    resp2 = {"status": "FAILURE", "error": "not found"}
                else:
                    block = store[key]
                    resp2 = {"status": "SUCCESS", "block_b64": base64.b64encode(block).decode("ascii")}
                c_sock.sendto(json.dumps(resp2).encode(), addr2)

            elif msg2.get("cmd") == "fail":
                store.clear()
                mode["state"] = "fail"
                resp2 = {"status": "SUCCESS", "event": "fail-complete"}
                c_sock.sendto(json.dumps(resp2).encode(), addr2)
            elif msg2.get("cmd") == "wipe":
                store.clear()
                resp2 = {"status": "SUCCESS"}
                c_sock.sendto(json.dumps(resp2).encode(), addr2)
            elif msg2.get("cmd") == "set-mode":
                a2 = msg2.get("args", {})
                state = (a2 or {}).get("state")
                if state in ("normal", "fail"):
                    mode["state"] = state
                    resp2 = {"status": "SUCCESS", "mode": mode["state"]}
                else:
                    resp2 = {"status": "FAILURE", "error": "state must be 'normal' or 'fail'"}
                c_sock.sendto(json.dumps(resp2).encode(), addr2)
            else:
                c_sock.sendto(json.dumps({"status": "FAILURE", "error": "unsupported"}).encode(), addr2)

    threading.Thread(target=content_loop, daemon=True).start()

    print("Disk registered. Make sure to press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

