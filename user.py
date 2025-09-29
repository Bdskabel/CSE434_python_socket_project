import socket, json, argparse
import os, base64

def send(sock, mgr, msg):
    sock.sendto(json.dumps(msg).encode(), mgr)
    data, _ = sock.recvfrom(12000)
    return json.loads(data.decode("utf-8"))

def parity_disk(n: int, stripe_idx: int) -> int:
    return n - (((stripe_idx % n) + 1))

def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")

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

    print("Type commands: ls | configure <dss_name> <n> <striping_unit> | quit")
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
        elif line.startswith("copy "):
            parts = line.split(maxsplit=2)
            if len(parts) != 3:
                print("usage: copy <dss_name> <local_file_path>")
                continue

            dss_name, local_path = parts[1], parts[2]
            if not os.path.isfile(local_path):
                print("file not found:", local_path)
                continue

            file_name = os.path.basename(local_path)
            with open(local_path, "rb") as f:
                file_bytes = f.read()
            owner = args.user_name

            prep = send(sock, mgr, {
                "cmd": "copy-prepare",
                "args": {"dss_name": dss_name, "file_name": file_name, "owner": owner}
            })
            if prep.get("status") != "SUCCESS":
                print("copy-prepare failed:", prep)
                continue

            d = prep["dss"]
            n = int(d["n"])
            b = int(d["striping_unit"])
            disks = d["disks"]  

            blocks_per_stripe = n - 1
            total = len(file_bytes)
            total_stripes = (total + (blocks_per_stripe * b) - 1) // (blocks_per_stripe * b)

            offset = 0
            for stripe_idx in range(total_stripes):
                p = parity_disk(n, stripe_idx)
                data_chunks = []

                for _ in range(blocks_per_stripe):
                    real = min(b, max(0, total - offset))
                    chunk = file_bytes[offset:offset + real]
                    offset += real
                    if len(chunk) < b:
                        chunk = chunk + bytes(b - len(chunk))
                    data_chunks.append(chunk)

                parity = bytearray(b)
                for ch in data_chunks:
                    for i in range(b):
                        parity[i] ^= ch[i]
                parity = bytes(parity)

                data_iter = iter(data_chunks)
                for disk_index in range(n):
                    if disk_index == p:
                        block = parity
                        is_parity = True
                    else:
                        block = next(data_iter)
                        is_parity = False

                    target = (disks[disk_index]["ip"], int(disks[disk_index]["c_port"]))
                    msg = {
                        "cmd": "write-block",
                        "args": {
                            "dss_name": dss_name,
                            "file_name": file_name,
                            "stripe_idx": stripe_idx,
                            "disk_index": disk_index,
                            "is_parity": is_parity,
                            "block_b64": b64e(block),
                        }
                    }
                    ack = send(sock, target, msg)
                    if ack.get("status") != "SUCCESS":
                        print("write-block failed:", ack)

            done = send(sock, mgr, {
                "cmd": "copy-complete",
                "args": {"dss_name": dss_name, "file_name": file_name,
                         "owner": owner, "size": len(file_bytes)}
            })
            print("copy-complete ->", done)
            
        elif line.startswith("configure"):
            parts = line.split()
            if len(parts) != 4:
                print("usage: configure <dss_name> <n> <striping_unit>")
                continue
            dss_name, n_str, b_str = parts[1], parts[2], parts[3]
            try:
                n = int(n_str); b = int(b_str)
            except ValueError:
                print("n and striping_unit must be integers")
                continue
            r = send(sock, mgr, {
                "cmd": "configure-dss",
                "args": {"dss_name": dss_name, "n": n, "striping_unit": b}
            })
            print("configure-dss ->", r)
        elif line == "":
            continue
        else:
            print("unknown command")

if __name__ == "__main__":
    main()
