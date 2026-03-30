#!/usr/bin/env python3
from __future__ import annotations

import argparse
import configparser
import glob
import json
import logging
import os
import re
import select
import shutil
import signal
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

LOG = logging.getLogger("nbdpnpc")


# Moved from nbdpnp_common.py
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


def ensure_nbd_module_loaded() -> None:
    if os.path.exists("/sys/module/nbd"):
        LOG.debug("NBD kernel module already loaded")
        return

    if not shutil.which("modprobe"):
        LOG.warning("modprobe not found; cannot load nbd kernel module")
        return

    try:
        run_cmd(["modprobe", "nbd"], check=True, capture_output=True, text=True, timeout=30)
        LOG.info("Loaded nbd kernel module")
    except Exception as exc:
        LOG.warning("Failed to load nbd kernel module: %s", exc)


def now_ts() -> float:
    return time.time()


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


def _device_index(device: str) -> int:
    name = os.path.basename(path_for_device(device))
    if not name.startswith("nbd"):
        return 10**9
    try:
        return int(name[3:])
    except ValueError:
        return 10**9


class NbdDeviceAllocator:
    """Allocate local NBD devices dynamically from the free device pool."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._allocated: set[str] = set()

    def _candidate_devices(self) -> list[str]:
        candidates = []
        seen = set()

        # Prefer exact NBD block devices known to sysfs, then fall back to
        # matching /dev/nbdN device nodes.
        for path in glob.glob("/sys/block/nbd[0-9]*"):
            name = os.path.basename(path)
            if name not in seen and name.startswith("nbd") and name[3:].isdigit():
                seen.add(name)
                candidates.append(f"/dev/{name}")

        for path in glob.glob("/dev/nbd[0-9]*"):
            name = os.path.basename(path)
            if name not in seen and name.startswith("nbd") and name[3:].isdigit():
                seen.add(name)
                candidates.append(f"/dev/{name}")

        candidates.sort(key=_device_index)
        return candidates

    @staticmethod
    def _read_sysfs_text(path: str) -> Optional[str]:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            return None

    @classmethod
    def _is_free_device(cls, device: str) -> bool:
        device = path_for_device(device)
        sys_name = os.path.basename(device)
        sys_path = f"/sys/block/{sys_name}"
        if not os.path.exists(sys_path):
            return False

        pid_path = f"{sys_path}/pid"
        pid_value = cls._read_sysfs_text(pid_path)
        if pid_value is not None:
            return pid_value in {"", "0"}

        # Some kernels or environments do not expose pid for NBD devices.
        # In that case use other sysfs indicators so that an absent pid file
        # does not incorrectly make every device appear busy.
        size_value = cls._read_sysfs_text(f"{sys_path}/size")
        if size_value is not None:
            try:
                if int(size_value) != 0:
                    return False
            except ValueError:
                return False

        holders_path = f"{sys_path}/holders"
        try:
            if any(os.scandir(holders_path)):
                return False
        except FileNotFoundError:
            pass
        except OSError:
            return False

        return True

    def reserve(self, preferred: Optional[str] = None) -> str:
        with self._lock:
            if preferred:
                preferred = path_for_device(preferred)
                if preferred not in self._allocated and self._is_free_device(preferred):
                    self._allocated.add(preferred)
                    return preferred

            for device in self._candidate_devices():
                if device in self._allocated:
                    continue
                if self._is_free_device(device):
                    self._allocated.add(device)
                    return device

        raise RuntimeError("No free NBD device is available")

    def release(self, device: Optional[str]) -> None:
        if not device:
            return
        device = path_for_device(device)
        with self._lock:
            self._allocated.discard(device)


ALLOCATOR = NbdDeviceAllocator()


def cleanup_subscription(sub: SubscriptionState) -> None:
    try:
        unmount_device(sub.device, sub.actual_mountpoint, sub.bind_mountpoint)
    except Exception as exc:
        LOG.debug("Cleanup failed for %s: %s", sub.export_name, exc)
    finally:
        ALLOCATOR.release(sub.device)
        sub.device = None
        sub.connected = False
        sub.mounted = False
        sub.actual_mountpoint = None
        sub.bind_mountpoint = None


def nbd_connect(host: str, port: int, device: str, export: str) -> None:
    device = path_for_device(device)
    try:
        LOG.info("Connecting %s to %s:%s export %s", device, host, port, export)
        result = run_cmd(
            ["nbd-client", "-N", export, host, str(port), device],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.stdout.strip():
            LOG.debug("nbd-client output: %s", result.stdout.strip())
    except Exception as exc:
        raise RuntimeError(f"Failed to connect {device} to {host}:{port}/{export}: {exc}") from exc


def nbd_disconnect(device: Optional[str]) -> None:
    if not device:
        return
    device = path_for_device(device)
    try:
        LOG.info("Disconnecting %s", device)
        run_cmd(["nbd-client", "-d", device], check=True, capture_output=True, text=True, timeout=30)
    except Exception as exc:
        LOG.debug("nbd-client disconnect failed for %s: %s", device, exc)


def parse_udisks_mountpoint(output: str) -> Optional[str]:
    # Typical output:
    # Mounted /dev/nbd0 at /run/media/root/DISC.
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("Mounted ") and " at " in line:
            tail = line.split(" at ", 1)[1].rstrip(".")
            return tail
    return None


def mount_device(device: str, mountpoint: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    device = path_for_device(device)
    if mountpoint:
        ensure_dir(mountpoint)

    result = run_cmd(
        ["udisksctl", "mount", "-b", device, "--no-user-interaction"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    actual = parse_udisks_mountpoint(result.stdout)
    if actual is None:
        raise RuntimeError(f"Could not determine mount path from udisksctl output: {result.stdout!r}")

    if mountpoint and os.path.realpath(actual) != os.path.realpath(mountpoint):
        if is_mounted(mountpoint):
            try:
                umount_path(mountpoint)
            except Exception:
                pass
        mount_bind(actual, mountpoint)
        return actual, mountpoint

    return actual, None


def unmount_device(device: Optional[str], actual_mountpoint: Optional[str], bind_mountpoint: Optional[str]) -> None:
    if bind_mountpoint and is_mounted(bind_mountpoint):
        try:
            LOG.info("Unmounting bind mount %s", bind_mountpoint)
            umount_path(bind_mountpoint)
        except Exception as exc:
            LOG.debug("Failed to unmount bind mount %s: %s", bind_mountpoint, exc)

    if actual_mountpoint and is_mounted(actual_mountpoint):
        try:
            LOG.info("Unmounting %s", actual_mountpoint)
            umount_path(actual_mountpoint)
        except Exception as exc:
            LOG.debug("Failed to unmount %s: %s", actual_mountpoint, exc)

    nbd_disconnect(device)


PING_INTERVAL = 5.0
PING_TIMEOUT = 15.0

class ServerByeError(ConnectionError):
    """Raised when the server sent a graceful bye and closed the connection."""


class RemoteServerWorker:
    def __init__(self, name: str, host: str, port: int, retry_interval: float, subscriptions: List[SubscriptionState], stop_event: threading.Event) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.retry_interval = retry_interval
        self.subscriptions = {sub.export_name: sub for sub in subscriptions}
        self.stop_event = stop_event
        self.sock: Optional[socket.socket] = None
        self.fp = None
        self._last_pong = 0.0
        self._last_ping = 0.0

    def close_socket(self) -> None:
        try:
            if self.fp:
                try:
                    send_json_line(self.fp, {"type": "bye", "timestamp": now_ts()})
                except Exception:
                    pass
                self.fp.close()
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.fp = None
        self.sock = None

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=20)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.fp = self.sock.makefile("rwb")
        send_json_line(
            self.fp,
            {
                "type": "hello",
                "client_id": socket.gethostname(),
                "subscriptions": list(self.subscriptions.keys()),
                "timestamp": now_ts(),
            },
        )
        self.sock.settimeout(10.0)
        welcome = recv_json_line(self.fp)
        if not welcome or welcome.get("type") != "welcome":
            raise RuntimeError(f"Unexpected handshake response from {self.host}:{self.port}")
        initial_exports = welcome.get("exports", [])
        if not isinstance(initial_exports, list):
            raise RuntimeError("Invalid welcome payload")
        LOG.info("Connected to server %s (%s:%s)", self.name, self.host, self.port)
        for item in initial_exports:
            if not isinstance(item, dict):
                continue
            export = item.get("export")
            present = bool(item.get("present", False))
            if export in self.subscriptions:
                sub = self.subscriptions[export]
                item_port = item.get("port")
                if isinstance(item_port, int) and item_port > 0:
                    sub.port = item_port
                self.apply_state(sub, present)

        self.sock.settimeout(None)
        now = now_ts()
        self._last_pong = now
        self._last_ping = now

    def _cleanup_subscription(self, sub: SubscriptionState) -> None:
        cleanup_subscription(sub)

    def ensure_present(self, sub: SubscriptionState) -> None:
        if sub.device is None:
            sub.device = ALLOCATOR.reserve(sub.preferred_device)
            LOG.info("Allocated %s for %s/%s", sub.device, self.name, sub.export_name)

        try:
            if not sub.connected:
                nbd_connect(sub.host, sub.port, sub.device, sub.export_name)
                sub.connected = True

            if sub.auto_mount:
                if sub.mounted and (sub.actual_mountpoint or sub.bind_mountpoint):
                    return
                wait_for_nbd_device_ready(sub.device, timeout=sub.device_ready_timeout)
                actual, bind = mount_device(sub.device, sub.mountpoint)
                sub.actual_mountpoint = actual
                sub.bind_mountpoint = bind
                sub.mounted = True
            else:
                sub.mounted = False
        except Exception:
            self._cleanup_subscription(sub)
            raise

    def ensure_absent(self, sub: SubscriptionState) -> None:
        if sub.mounted or sub.connected or sub.device is not None:
            self._cleanup_subscription(sub)

    def apply_state(self, sub: SubscriptionState, present: bool) -> None:
        if present:
            self.ensure_present(sub)
        else:
            self.ensure_absent(sub)

    def handle_event(self, msg: dict) -> None:
        typ = msg.get("type")
        if typ == "change":
            export = msg.get("export")
            if export not in self.subscriptions:
                return
            present = bool(msg.get("present", False))
            sub = self.subscriptions[export]
            msg_port = msg.get("port")
            if isinstance(msg_port, int) and msg_port > 0:
                sub.port = msg_port
            LOG.info("Received change for %s/%s: present=%s, port=%s", self.name, export, present, sub.port)
            if present:
                self.ensure_present(sub)
            else:
                self.ensure_absent(sub)
        elif typ == "pong":
            self._last_pong = now_ts()
            LOG.debug("Received pong from server %s", self.name)
        elif typ == "bye":
            LOG.info("Received bye from server %s", self.name)
            for sub in self.subscriptions.values():
                try:
                    self.ensure_absent(sub)
                except Exception as exc:
                    LOG.debug("Cleanup failed for %s on bye: %s", sub.export_name, exc)
            raise ServerByeError("Server requested disconnect via bye")

    def run(self) -> None:
        was_connected = False
        while not self.stop_event.is_set():
            try:
                self.connect()
                was_connected = True
                for sub in self.subscriptions.values():
                    sub.connected = True
                while not self.stop_event.is_set():
                    assert self.sock is not None
                    now = now_ts()
                    if (now - self._last_ping) >= PING_INTERVAL:
                        send_json_line(self.fp, {"type": "ping", "timestamp": now})
                        self._last_ping = now
                    if self._last_pong and (now - self._last_pong) >= PING_TIMEOUT:
                        raise ConnectionError("Server unresponsive (ping timeout)")
                    r, _, _ = select.select([self.sock], [], [], 1.0)
                    if not r:
                        continue

                    msg = recv_json_line(self.fp)
                    if msg is None:
                        raise ConnectionError("Server closed the connection")
                    self.handle_event(msg)
            except Exception as exc:
                if was_connected and not self.stop_event.is_set() and not isinstance(exc, ServerByeError):
                    LOG.warning("Server %s connection lost: %s", self.name, exc)
                for sub in self.subscriptions.values():
                    try:
                        self.ensure_absent(sub)
                    except Exception as inner_exc:
                        LOG.debug("Cleanup failed for %s: %s", sub.export_name, inner_exc)
                self.close_socket()
                was_connected = False
                if not self.stop_event.is_set():
                    time.sleep(self.retry_interval)
            finally:
                for sub in self.subscriptions.values():
                    sub.connected = False
                self.close_socket()


class NbdPnpcClient:
    def __init__(self, config_path: str) -> None:
        self.config_path = config_path
        self.config = read_config(config_path)
        self.stop_event = threading.Event()
        self.servers: Dict[str, dict] = {}
        self.subscriptions_by_server: Dict[str, List[SubscriptionState]] = {}
        self.threads: List[threading.Thread] = []
        self.workers: List[RemoteServerWorker] = []
        self._load_config()

    def _load_config(self) -> None:
        for section in self.config.sections():
            kind, name = parse_section_name(section)
            if kind == "server":
                self.servers[name] = {
                    "host": self.config.get(section, "host", fallback=""),
                    "port": self.config.getint(section, "port", fallback=10800),
                    "retry_interval": self.config.getfloat(section, "retry_interval", fallback=10.0),
                    "device_ready_timeout": self.config.getfloat(section, "device_ready_timeout", fallback=10.0),
                }
            elif kind == "subscription":
                server_name = self.config.get(section, "server", fallback="").strip()
                export_name = self.config.get(section, "export", fallback="").strip()
                preferred_device = self.config.get(section, "device", fallback="").strip() or None
                mountpoint = self.config.get(section, "mountpoint", fallback="").strip() or None
                auto_mount = parse_bool(self.config.get(section, "auto_mount", fallback="yes"), True)
                if not server_name or not export_name:
                    raise ValueError(f"Missing server/export in section {section!r}")
                server = self.servers.get(server_name)
                if server is None:
                    raise ValueError(f'Subscription {section!r} references unknown server {server_name!r}')
                if preferred_device:
                    LOG.warning(
                        "Section %s sets device=%s, but devices are allocated dynamically; the value is ignored as a binding hint.",
                        section,
                        preferred_device,
                    )
                sub = SubscriptionState(
                    server_name=server_name,
                    host=server["host"],
                    port=server["port"],
                    export_name=export_name,
                    preferred_device=preferred_device,
                    device=None,
                    mountpoint=mountpoint,
                    auto_mount=auto_mount,
                    retry_interval=server["retry_interval"],
                    device_ready_timeout=server["device_ready_timeout"],
                )
                self.subscriptions_by_server.setdefault(server_name, []).append(sub)

        if not self.servers:
            raise ValueError("No server sections configured")
        for name, subs in self.subscriptions_by_server.items():
            if not subs:
                raise ValueError(f"Server {name!r} has no subscriptions")

    def run(self) -> int:
        for server_name, subs in self.subscriptions_by_server.items():
            server = self.servers[server_name]
            worker = RemoteServerWorker(
                name=server_name,
                host=server["host"],
                port=server["port"],
                retry_interval=server["retry_interval"],
                subscriptions=subs,
                stop_event=self.stop_event,
            )
            self.workers.append(worker)
            thread = threading.Thread(target=worker.run, name=f"nbdpnpc-{server_name}", daemon=True)
            thread.start()
            self.threads.append(thread)

        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        finally:
            self.stop_event.set()
            for worker in self.workers:
                worker.close_socket()
            for thread in self.threads:
                thread.join(timeout=5.0)
            for subs in self.subscriptions_by_server.values():
                for sub in subs:
                    cleanup_subscription(sub)
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="NBD plug-and-play client")
    parser.add_argument("-c", "--config", required=True, help="Path to the configuration file")
    parser.add_argument("-l", "--log-level", default=None, help="Log level")
    args = parser.parse_args()

    cfg = read_config(args.config)
    log_level = args.log_level or cfg.get("global", "log_level", fallback="INFO")
    setup_logging(log_level)

    ensure_nbd_module_loaded()

    client = NbdPnpcClient(args.config)

    def _stop(_signum, _frame):
        LOG.info("Stop requested")
        client.stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    return client.run()


if __name__ == "__main__":
    raise SystemExit(main())
