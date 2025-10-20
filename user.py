import socket, json, argparse
import os, base64
import hashlib
import threading
import random

def fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n // 1024} KB"
    return f"{n // (1024 * 1024)} MB"


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

    print("Type commands: ls | configure <dss_name> <n> <striping_unit> | copy <dss_name> <local_file_path> | read <dss_name> <file_name> <output_path> [p] | disk-failure <dss_name> | decommission <dss_name> | deregister | quit")

    while True:
        try:
            line = input("> ").strip()
            cmd = line.lower()
        except EOFError:
            break
        if cmd in ("quit", "exit"):
            break
        elif cmd == "ls":
            r = send(sock, mgr, {"cmd": "ls", "args": {}})
            if r.get("status") != "SUCCESS":
                print(f"ls failed: {r.get('error', 'unknown error')}")
                continue
        
            listing = r.get("listing", {})
            users_list = listing.get("users", [])
            disks_list = listing.get("disks", [])
            dsses_list = listing.get("dsses", [])
            free_disks = listing.get("free_disks", [])
        
            print("Users:", ", ".join(users_list) if users_list else "(none)")
        
            print("Disks:")
            if disks_list:
                for d in disks_list:
                    print(f"  - {d.get('name','?')} [{d.get('state','?')}]")
            else:
                print("  (none)")
        
            if not dsses_list:
                print("No DSS configured.")
            else:
                for dss in dsses_list:
                    dss_name = dss.get("dss_name", "?")
                    n = dss.get("n", 0)
                    su = dss.get("striping_unit", 0)
                    disk_names = dss.get("disks", [])
                    files = dss.get("files", {})
        
                    print(f"{dss_name}: Disk array with n={n} ({', '.join(disk_names)}) with striping-unit {fmt_bytes(su)}.")
        
                    if files:
                        for fname, meta in files.items():
                            size = meta.get("size", 0)
                            owner = meta.get("owner", "?")
                            print(f"  {fname} {size:,} B {owner}")
                    else:
                        print("  (no files)")
        
            if free_disks:
                print("Free disks:", ", ".join(free_disks))
        elif cmd.startswith("read "):
            parts = line.split()
            if len(parts) < 4 or len(parts) > 5:
                print("usage: read <dss_name> <file_name> <output_path> [p]")
                continue
        
            dss_name, file_name, out_path = parts[1], parts[2], parts[3]
            try:
                p_error = int(parts[4]) if len(parts) == 5 else 0
            except ValueError:
                print("p must be an integer 0..100")
                continue
            p_error = max(0, min(100, p_error))
    
            prep = send(sock, mgr, {
                "cmd": "read-prepare",
                "args": {"dss_name": dss_name, "file_name": file_name, "user_name": args.user_name}
            })
            if prep.get("status") != "SUCCESS":
                err = prep.get("error", "unknown error")
                if err == "NOT_OWNER":
                    print(f"read denied: you are not the owner of '{file_name}' on DSS '{dss_name}'.")
                else:
                    print(f"read-prepare failed: {err}")
                continue

    
            n = int(prep["dss"]["n"])
            b = int(prep["dss"]["striping_unit"])
            disks = prep["dss"]["disks"]
            file_size = int(prep["file"]["size"])
    
            total_stripes = total_stripes_for_size(file_size, n, b)

            data_blocks = []
            MAX_RETRIES = 5  
            
            for stripe_idx in range(total_stripes):
                pidx = parity_disk(n, stripe_idx)
            
                for attempt in range(MAX_RETRIES):
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
            
                    if p_error > 0 and random.randrange(100) < p_error:
                        flip_idx = random.randrange(n)
                        if got[flip_idx] is not None:
                            bb = bytearray(got[flip_idx])
                            if len(bb) > 0:
                                j = random.randrange(len(bb))
                                bb[j] ^= (1 << random.randrange(8))
                                got[flip_idx] = bytes(bb)
            
                    missing = [i for i in range(n) if got[i] is None]
                    if len(missing) > 1:
                        if attempt == MAX_RETRIES - 1:
                            print(f"read failed at stripe {stripe_idx}: multiple blocks missing after retries")
                            data_blocks = []
                            break
                        continue
            
                    if len(missing) == 1:
                        miss = missing[0]
                        others = [got[i] for i in range(n) if i != miss]
                        if any(x is None for x in others):
                            if attempt == MAX_RETRIES - 1:
                                print(f"read failed at stripe {stripe_idx}: not enough blocks to reconstruct")
                                data_blocks = []
                                break
                            continue
                        got[miss] = xor_bytes(others, b)
            
                    if any(x is None for i, x in enumerate(got) if i != pidx):
                        if attempt == MAX_RETRIES - 1:
                            print(f"read failed at stripe {stripe_idx}: missing data for parity verification")
                            data_blocks = []
                            break
                        continue
            
                    calc_parity = xor_bytes([got[i] for i in range(n) if i != pidx], b)
                    if calc_parity != got[pidx]:
                        if attempt == MAX_RETRIES - 1:
                            print(f"read failed parity at stripe {stripe_idx} after {MAX_RETRIES} attempts")
                            data_blocks = []
                            break
                        continue
            
                    for i in range(n):
                        if i == pidx:
                            continue
                        data_blocks.append(got[i])
                    break  
            
                if not data_blocks or len(data_blocks) < (stripe_idx + 1) * (n - 1):
                    break

    
            if not data_blocks:
                print("read aborted")
                _ = send(sock, mgr, {"cmd": "read-complete", "args": {"dss_name": dss_name}})
                continue
    
            buf = b"".join(data_blocks)[:file_size]
            try:
                with open(out_path, "wb") as f:
                    f.write(buf)
                print(f"read -> wrote {len(buf)} bytes to {out_path}")
                sha_read = hashlib.sha256(buf).hexdigest()
                sha_expected = prep.get("file", {}).get("sha256")
                if sha_expected:
                    print("SHA256 match:" if sha_read == sha_expected else "SHA256 MISMATCH!", sha_read)

            except Exception as e:
                print("write failed:", e)
    
            done = send(sock, mgr, {"cmd": "read-complete", "args": {"dss_name": dss_name}})
            if done.get("status") != "SUCCESS":
                print("read-complete ack:", done)
        elif cmd.startswith("copy "):
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

            sha_src = hashlib.sha256(file_bytes).hexdigest()
            done = send(sock, mgr, {
                "cmd": "copy-complete",
                "args": {"dss_name": dss_name, "file_name": file_name,
                         "owner": owner, "size": len(file_bytes), "sha256": sha_src}
            })

            print("copy-complete ->", done)
            
        elif cmd.startswith("disk-failure "):
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                print("usage: disk-failure <dss_name>")
                continue
            dss_name = parts[1]
        
            prep = send(sock, mgr, {"cmd": "disk-failure", "args": {"dss_name": dss_name, "user_name": args.user_name}})
            if prep.get("status") != "SUCCESS":
                print(f"disk-failure denied: {prep.get('error', 'unknown error')}")
                continue
        
            d = prep["dss"]
            n = int(d["n"])
            b = int(d["striping_unit"])
            disks = d["disks"]          
            files = prep.get("files", {}) 
        
            failed_idx = random.randrange(n)
            failed_ep = disks[failed_idx]
            failed_target = (failed_ep["ip"], int(failed_ep["c_port"]))
        
            fr = send_to_with_timeout(sock, failed_target, {"cmd": "fail", "args": {}}, timeout=1.0)
            if fr.get("status") != "SUCCESS":
                print("disk did not confirm failure:", fr)
                _ = send(sock, mgr, {"cmd": "recovery-complete", "args": {"dss_name": dss_name}})
                continue
        
            print(f"Failed disk index {failed_idx} ({failed_ep['disk_name']}). Starting reconstruction...")
        
            for fname, meta in files.items():
                fsize = int(meta.get("size", 0))
                total_stripes = total_stripes_for_size(fsize, n, b)
        
                for stripe_idx in range(total_stripes):
                    got = [None] * n
                    threads = []
                    for k in range(n):
                        if k == failed_idx:
                            continue
                        target = (disks[k]["ip"], int(disks[k]["c_port"]))
                        t = threading.Thread(
                            target=read_block_parallel,
                            args=(sock, target, fname, stripe_idx, k, got)
                        )
                        t.start()
                        threads.append(t)
                    for t in threads:
                        t.join()
        
                    missing_other = [i for i in range(n) if i != failed_idx and got[i] is None]
                    if missing_other:
                        print(f"reconstruct failed at stripe {stripe_idx} for file {fname}: missing from {missing_other}")
                        _ = send(sock, mgr, {"cmd": "recovery-complete", "args": {"dss_name": dss_name}})
                        break
        
                    others = [got[i] for i in range(n) if i != failed_idx]
                    rebuilt = xor_bytes(others, b)
                    is_parity = (failed_idx == parity_disk(n, stripe_idx))
        
                    payload = {
                        "cmd": "write-block",
                        "args": {
                            "dss_name": dss_name,
                            "file_name": fname,
                            "stripe_idx": stripe_idx,
                            "disk_index": failed_idx,
                            "is_parity": is_parity,
                            "block_b64": b64e(rebuilt),
                        }
                    }
                    wr = send_to_with_timeout(sock, failed_target, payload, timeout=1.0)
                    if wr.get("status") != "SUCCESS":
                        print(f"write failed during reconstruction at stripe {stripe_idx} for file {fname}: {wr}")
                        _ = send(sock, mgr, {"cmd": "recovery-complete", "args": {"dss_name": dss_name}})
                        break
                else:
                    continue
                break
        
            _ = send_to_with_timeout(sock, failed_target, {"cmd": "set-mode", "args": {"state": "normal"}}, timeout=1.0)
        
            done = send(sock, mgr, {"cmd": "recovery-complete", "args": {"dss_name": dss_name}})
            print("recovery-complete ->", done)

        elif cmd.startswith("decommission "):
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                print("usage: decommission <dss_name>")
                continue
            dss_name = parts[1]
        
            prep = send(sock, mgr, {"cmd": "decommission-dss", "args": {"dss_name": dss_name, "user_name": args.user_name}})
            if prep.get("status") != "SUCCESS":
                print("decommission-dss failed:", prep)
                continue
        
            d = prep["dss"]
            disks = d["disks"] 
        
            ok = True
            for ep in disks:
                target = (ep["ip"], int(ep["c_port"]))
                r = send_to_with_timeout(sock, target, {"cmd": "wipe", "args": {}}, timeout=1.0)
                if r.get("status") != "SUCCESS":
                    ok = False
                    print("wipe failed on", ep["disk_name"], r)
        
            done = send(sock, mgr, {"cmd": "decommission-complete", "args": {"dss_name": dss_name}})
            print("decommission-complete ->", done)
        elif cmd == "deregister":
            r = send(sock, mgr, {"cmd": "deregister-user", "args": {"user_name": args.user_name}})
            print("deregister-user ->", r)
            if r.get("status") == "SUCCESS":
                break
        elif cmd.startswith("configure"):
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
        elif not line:
            continue
        else:
            print("unknown command")

if __name__ == "__main__":
    main()
