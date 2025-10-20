import socket, json, argparse
import os, base64
import hashlib
import threading

def blocks_per_stripe(n: int) -> int:
    return n - 1 

def total_stripes_for_size(file_size: int, n: int, b: int) -> int:
    denom = blocks_per_stripe(n) * b
    return (file_size + denom - 1) // denom

def pad_to(bsize: int, data: bytes) -> bytes:
    if len(data) >= bsize:
        return data[:bsize]
    return data + bytes(bsize - len(data))

def xor_bytes(chunks: list[bytes], b: int) -> bytes:
    out = bytearray(b)
    for ch in chunks:
        for i in range(b):
            out[i] ^= ch[i]
    return bytes(out)


def guess_my_ip(to_ip: str, to_port: int) -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((to_ip, to_port))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def read_block_parallel(sock, target, file_name, stripe_idx, disk_index, out_list):
    """Read one block; store bytes (or None) at out_list[disk_index]."""
    r = send_to_with_timeout(sock, target, {
        "cmd": "read-block",
        "args": {"file_name": file_name, "stripe_idx": stripe_idx, "disk_index": disk_index}
    }, timeout=1.0)
    if r.get("status") == "SUCCESS":
        try:
            out_list[disk_index] = b64d(r["block_b64"])
        except Exception:
            out_list[disk_index] = None
    else:
        out_list[disk_index] = None

def write_block_parallel(sock, target, payload, results, idx):
    """Write one block; set results[idx] = True/False."""
    r = send_to_with_timeout(sock, target, payload, timeout=1.0)
    results[idx] = (r.get("status") == "SUCCESS")



def send(sock, mgr, msg):
    print({"trace": "send", "to": mgr, "msg": msg})
    sock.sendto(json.dumps(msg).encode(), mgr)
    data, _ = sock.recvfrom(12000)
    resp = json.loads(data.decode("utf-8"))
    print({"trace": "recv", "from": "manager", "resp": resp})
    return resp

def parity_disk(n: int, stripe_idx: int) -> int:
    return n - (((stripe_idx % n) + 1))

def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")

def send_to_with_timeout(sock, target, msg, timeout=1.0):
    sock.settimeout(timeout)
    try:
        sock.sendto(json.dumps(msg).encode(), target)
        data, _ = sock.recvfrom(65535)
        return json.loads(data.decode("utf-8"))
    except socket.timeout:
        return {"status": "FAILURE", "error": "timeout"}
    finally:
        sock.settimeout(None)

def b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))

def data_disk_order(n: int, stripe_idx: int):
    p = parity_disk(n, stripe_idx)
    return [i for i in range(n) if i != p]

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

    my_ip = guess_my_ip(args.manager_ip, args.manager_port)

    r = send(sock, mgr, {
        "cmd": "register-user",
        "args": {"user_name": args.user_name, "ip": my_ip,
                 "m_port": args.my_m_port, "c_port": args.my_c_port}
    })
    print("register-user ->", r)

    print("Type commands: ls | configure <dss_name> <n> <striping_unit> | copy <dss_name> <local_file_path> | read <dss_name> <file_name> <output_path> | decommission <dss_name> | deregister | quit")

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
        elif line.startswith("read "):
            parts = line.split(maxsplit=3)
            if len(parts) != 4:
                print("usage: read <dss_name> <file_name> <output_path>")
                continue
    
            dss_name, file_name, out_path = parts[1], parts[2], parts[3]
    
            prep = send(sock, mgr, {
                "cmd": "read-prepare",
                "args": {"dss_name": dss_name, "file_name": file_name}
            })
            if prep.get("status") != "SUCCESS":
                print("read-prepare failed:", prep)
                continue
    
            n = int(prep["dss"]["n"])
            b = int(prep["dss"]["striping_unit"])
            disks = prep["dss"]["disks"]
            file_size = int(prep["file"]["size"])
    
            total_stripes = total_stripes_for_size(file_size, n, b)

            data_blocks = []
            for stripe_idx in range(total_stripes):
                p = parity_disk(n, stripe_idx)
    
                got = [None] * n
                threads = []
                for disk_index in range(n):
                    target = (disks[disk_index]["ip"], int(disks[disk_index]["c_port"]))
                    t = threading.Thread(
                        target=read_block_parallel,
                        args=(sock, target, file_name, stripe_idx, disk_index, got)
                    )
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
    
                missing = [i for i in range(n) if got[i] is None]
    
                if len(missing) == 0:
                    pass
                elif len(missing) == 1:
                    miss = missing[0]
                    x = bytearray(b)
                    if miss == p:
                        for i in range(n):
                            if i == p: continue
                            blk = got[i]
                            for j in range(b):
                                x[j] ^= blk[j]
                        got[p] = bytes(x)
                    else:
                        for i in range(n):
                            if i == miss: continue
                            blk = got[i]
                            for j in range(b):
                                x[j] ^= blk[j]
                        got[miss] = bytes(x)
                else:
                    print(f"read failed at stripe {stripe_idx}: more than one block missing")
                    data_blocks = []
                    break
    
                for i in range(n):
                    if i == p:
                        continue
                    data_blocks.append(got[i])
    
            if not data_blocks:
                print("read aborted")
                continue
    
            buf = b"".join(data_blocks)[:file_size]
            try:
                with open(out_path, "wb") as f:
                    f.write(buf)
                print(f"read -> wrote {len(buf)} bytes to {out_path}")
            except Exception as e:
                print("write failed:", e)
    
            done = send(sock, mgr, {"cmd": "read-complete", "args": {}})
            if done.get("status") != "SUCCESS":
                print("read-complete ack:", done)
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

            total = len(file_bytes)
            total_stripes = total_stripes_for_size(total, n, b)

            offset = 0
            for stripe_idx in range(total_stripes):
                p = parity_disk(n, stripe_idx)
                data_chunks = []

                for _ in range(blocks_per_stripe(n)):
                    chunk = file_bytes[offset:offset + b]
                    offset += len(chunk)
                    data_chunks.append(pad_to(b, chunk))

                parity = xor_bytes(data_chunks, b)

                p = parity_disk(n, stripe_idx)
                results = [False] * n
                threads = []
                data_iter = iter(data_chunks)
                for disk_index in range(n):
                    if disk_index == p:
                        block = parity
                        is_parity = True
                    else:
                        block = next(data_iter)
                        is_parity = False

                    target = (disks[disk_index]["ip"], int(disks[disk_index]["c_port"]))
                    payload = {
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
                    t = threading.Thread(
                        target=write_block_parallel,
                        args=(sock, target, payload, results, disk_index)
                    )
                    t.start()
                    threads.append(t)
                
                for t in threads:
                    t.join()
                
                if not all(results):
                    print(f"warning: some write-block failed on stripe {stripe_idx}")

            done = send(sock, mgr, {
                "cmd": "copy-complete",
                "args": {"dss_name": dss_name, "file_name": file_name,
                         "owner": owner, "size": len(file_bytes)}
            })
            print("copy-complete ->", done)
            
        elif line.startswith("decommission "):
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                print("usage: decommission <dss_name>")
                continue
            dss_name = parts[1]
        
            # Phase 1: ask manager for DSS params and enter critical section
            prep = send(sock, mgr, {"cmd": "decommission-dss", "args": {"dss_name": dss_name, "user_name": args.user_name}})
            if prep.get("status") != "SUCCESS":
                print("decommission-dss failed:", prep)
                continue
        
            d = prep["dss"]
            disks = d["disks"]  # [{disk_name, ip, c_port}, ...]
        
            # Instruct each disk to wipe its contents
            ok = True
            for ep in disks:
                target = (ep["ip"], int(ep["c_port"]))
                r = send_to_with_timeout(sock, target, {"cmd": "wipe", "args": {}}, timeout=1.0)
                if r.get("status") != "SUCCESS":
                    ok = False
                    print("wipe failed on", ep["disk_name"], r)
        
            # Phase 2: tell manager we are done
            done = send(sock, mgr, {"cmd": "decommission-complete", "args": {"dss_name": dss_name}})
            print("decommission-complete ->", done)
        elif line == "deregister":
            r = send(sock, mgr, {"cmd": "deregister-user", "args": {"user_name": args.user_name}})
            print("deregister-user ->", r)
            if r.get("status") == "SUCCESS":
                break
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
