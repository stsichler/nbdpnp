#!/usr/bin/env python3
"""Shared helpers for the nbdpnpd and nbdpnpc daemons."""

from __future__ import annotations

import configparser
import json
import logging
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def setup_logging(level: str = "INFO") -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def read_config(path: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    with open(path, "r", encoding="utf-8") as fh:
        parser.read_file(fh)
    return parser


def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on", "y"}:
        return True
    if value in {"0", "false", "no", "off", "n"}:
        return False
    return default


def send_json_line(fp, payload: dict) -> None:
    data = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    fp.write(data)
    fp.flush()


def recv_json_line(fp) -> Optional[dict]:
    if fp is None:
        return None
    line = fp.readline()
    if not line:
        return None
    return json.loads(line.decode("utf-8"))


def run_cmd(
    cmd: list[str],
    check: bool = True,
    capture_output: bool = True,
    text: bool = True,
    timeout: Optional[int] = 30,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture_output,
        text=text,
        timeout=timeout,
    )


def wait_for_nbd_device_ready(device: str, timeout: float = 10.0, poll_interval: float = 0.2) -> None:
    device = path_for_device(device)
    sys_name = os.path.basename(device)
    sys_block = f"/sys/block/{sys_name}"
    pid_path = f"{sys_block}/pid"

    # Give udev a chance to finish processing the device registration.
    if shutil.which("udevadm"):
        try:
            run_cmd(
                ["udevadm", "settle", "--timeout", str(max(1, int(round(timeout))))],
                check=True,
                capture_output=True,
                text=True,
                timeout=max(2, int(round(timeout)) + 2),
            )
        except Exception:
            # Settling is best-effort only.
            pass

    deadline = time.monotonic() + max(0.0, timeout)
    last_error = None
    while time.monotonic() < deadline:
        try:
            if not os.path.exists(device):
                last_error = f"{device} does not exist yet"
            elif not os.path.isdir(sys_block):
                last_error = f"{sys_block} does not exist yet"
            elif not os.path.exists(pid_path):
                last_error = f"{pid_path} does not exist yet"
            else:
                with open(pid_path, "r", encoding="utf-8") as fh:
                    pid = fh.read().strip()
                if pid and pid != "0":
                    # A non-zero PID indicates that the kernel has finished
                    # binding the NBD device and it is ready for userspace.
                    return
                last_error = f"{pid_path} is not populated yet"
        except OSError as exc:
            last_error = str(exc)

        time.sleep(max(0.05, poll_interval))

    raise TimeoutError(f"Timed out waiting for {device} to become ready: {last_error}")


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def is_mounted(path: str) -> bool:
    path = os.path.realpath(path)
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 5 and os.path.realpath(parts[4]) == path:
                    return True
    except FileNotFoundError:
        pass
    return False


def mount_bind(source: str, target: str) -> None:
    ensure_dir(target)
    run_cmd(["mount", "--bind", source, target], check=True)


def umount_path(target: str) -> None:
    run_cmd(["umount", target], check=True)


def path_for_device(device: str) -> str:
    if device.startswith("/dev/"):
        return device
    return f"/dev/{device}"


def sys_name_for_device(device: str) -> str:
    return os.path.basename(path_for_device(device))


def now_ts() -> float:
    return time.time()


@dataclass
class ExportState:
    name: str
    device: str
    present: bool = False


@dataclass
class SubscriptionState:
    server_name: str
    host: str
    port: int
    export_name: str
    preferred_device: Optional[str] = None
    device: Optional[str] = None
    mountpoint: Optional[str] = None
    auto_mount: bool = True
    retry_interval: float = 10.0
    device_ready_timeout: float = 10.0
    connected: bool = False
    mounted: bool = False
    actual_mountpoint: Optional[str] = None
    bind_mountpoint: Optional[str] = None
    last_error: Optional[str] = None


def parse_section_name(name: str) -> tuple[str, str]:
    m = re.match(r'^(?P<kind>[A-Za-z0-9_-]+)\s+"(?P<name>.*)"$', name.strip())
    if not m:
        raise ValueError(f"Invalid section name: {name!r}")
    return m.group("kind"), m.group("name")


def normalize_device_node(device: str) -> str:
    device = path_for_device(device)
    return os.path.realpath(device) if os.path.exists(device) else device
