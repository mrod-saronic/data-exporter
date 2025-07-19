import argparse
import subprocess
import json
import pandas as pd
import getpass
import paramiko
import re
import socket
import os
from typing import List

def expand_range(item: str) -> List[str]:
    match = re.match(r"([a-zA-Z]+)(\d+)-\1(\d+)", item)
    if not match:
        return [item]
    prefix, start, end = match.groups()
    return [f"{prefix}{i}" for i in range(int(start), int(end) + 1)]

def parse_vehicles(raw_input: str) -> List[str]:
    vehicles = []
    for item in raw_input.split(","):
        if "-" in item:
            vehicles.extend(expand_range(item))
        else:
            vehicles.append(item)
    return vehicles

def is_reachable(host: str) -> bool:
    try:
        subprocess.check_output(["ping", "-c", "3", "-W", "2", host], stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def fetch_engine_hours(host: str, password: str) -> str:
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username="saronic", password=password, timeout=10)

        if host.startswith("cr"):
            stdin, stdout, stderr = client.exec_command("timeout 3 submsg pentad")
            output = stdout.read().decode()
            for line in output.splitlines():
                if "EngineHours" in line:
                    try:
                        data = json.loads(line)
                        hours = data["msg"]["EngineHours"]["engine_total_hours_of_operation"]
                        return str(hours)
                    except Exception as e:
                        return f"[WARN] JSON parsing failed on {host}"
        else:
            return "[ERROR] Unsupported vehicle type"

    except (socket.timeout, paramiko.ssh_exception.SSHException) as e:
        return f"[ERROR] SSH connection failed: {e}"
    finally:
        client.close()

    return "[WARN] No engine hours found"

def main():
    parser = argparse.ArgumentParser(description="Engine hours exporter")
    parser.add_argument("vehicles", help="List like cr1,cr2 or cr1-cr3")
    args = parser.parse_args()

    raw_vehicles = parse_vehicles(args.vehicles)
    reachable = []
    unreachable = []
    for v in raw_vehicles:
        if is_reachable(v):
            reachable.append(v)
        else:
            print(f"[WARN] {v} is not reachable.")
            unreachable.append(v)

    if not reachable:
        print("[ERROR] No reachable assets")
        return
    if unreachable:
        print(f"[INFO] Unreachable assets: {', '.join(unreachable)}")

    password = getpass.getpass(prompt="[INFO] Enter SSH password: ")

    results = []
    for vehicle in reachable:
        print(f"[INFO] Querying {vehicle}...")
        hours = fetch_engine_hours(vehicle, password)
        print(f"{vehicle} -> {hours}")
        results.append({"vehicle": vehicle, "engine_hours": hours})

    df = pd.DataFrame(results)
    output_file = os.path.join(os.path.expanduser("~"), "Downloads", "engine_hours.xlsx")
    df.to_excel(output_file, index=False)
    print(f"[INFO] Exported to {output_file}")

if __name__ == "__main__":
    main()