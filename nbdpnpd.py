#!/usr/bin/env python3
from __future__ import annotations

import argparse
import configparser
import json
import logging
import os
import pyudev
import re
import select
import signal
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

LOG = logging.getLogger("nbdpnpd")


# Moved from nbdpnp_common.py
def setup_logging(level: str = "INFO") -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def read_config(path: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(default_section="global", interpolation=None)
    with open(path, "r", encoding="utf-8") as fh:
        parser.read_file(fh)
    return parser


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


#PING_INTERVAL = 5.0
PING_TIMEOUT = 15.0


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
        subprocess.run(
            ["modprobe", "nbd"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        LOG.info("Loaded nbd kernel module")
    except Exception as exc:
        LOG.warning("Failed to load nbd kernel module: %s", exc)


def now_ts() -> float:
    return time.time()


def parse_section_name(name: str) -> tuple[str, str]:
    m = re.match(r'^(?P<kind>[A-Za-z0-9_-]+)\s+"(?P<name>.*)"$', name.strip())
    if not m:
        raise ValueError(f"Invalid section name: {name!r}")
    return m.group("kind"), m.group("name")


@dataclass
class ExportState:
    name: str
    device: str
    port: int = 10809
    present: bool = False


def media_present(device: str) -> bool:
    device = path_for_device(device)
    if not os.path.exists(device):
        return False

    # Prefer udev properties because they do not trigger any media access.
    try:
        out = subprocess.run(
            ["udevadm", "info", "--query=property", "--name", device],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if out.returncode == 0:
            props = {}
            for line in out.stdout.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    props[k.strip()] = v.strip()
            for key in ("ID_CDROM_MEDIA", "ID_CDROM_MEDIA_PRESENT", "ID_CDROM_MEDIA_STATE"):
                val = props.get(key)
                if val is not None:
                    return val.lower() in {"1", "yes", "true", "present", "loaded"}
    except Exception:
        pass

    # As a non-invasive fallback, inspect sysfs only.
    base = os.path.basename(device)
    sysfs_candidates = [
        f"/sys/class/block/{base}/ro",
        f"/sys/class/block/{base}/removable",
        f"/sys/block/{base}/ro",
        f"/sys/block/{base}/removable",
    ]
    for candidate in sysfs_candidates:
        try:
            if os.path.exists(candidate):
                with open(candidate, "r", encoding="utf-8") as fh:
                    value = fh.read().strip()
                # For optical devices, a mounted disc is typically readable through udev props.
                # If sysfs only reports a removable device without media state, keep the previous state.
                if candidate.endswith("/removable"):
                    continue
                if value in {"0", "1"}:
                    return False
        except Exception:
            continue

    return False


class ClientByeError(Exception):
    """Raised when the client closes the connection gracefully via bye."""


class ClientConnection:
    def __init__(self, sock: socket.socket, server: "NbdPnpdServer") -> None:
        self.sock = sock
        self.server = server
        self.fp = sock.makefile("rwb")
        self.subscriptions: set[str] = set()
        self.alive = True
        self.last_ping = now_ts()
        self.lock = threading.Lock()

    def close(self) -> None:
        if not self.alive:
            return
        try:
            # Inform the client that the server is closing the connection.
            self.send({"type": "bye", "server_time": now_ts()})
        except Exception:
            pass
        self.alive = False
        try:
            self.fp.close()
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass

    def send(self, payload: dict) -> None:
        with self.lock:
            if not self.alive:
                return
            send_json_line(self.fp, payload)

    def handle(self) -> None:
        try:
            self.sock.settimeout(10.0)
            hello = recv_json_line(self.fp)
            if not hello or hello.get("type") != "hello":
                self.send({"type": "error", "error": "Expected hello message"})
                return

            subscriptions = hello.get("subscriptions") or []
            if not isinstance(subscriptions, list):
                self.send({"type": "error", "error": "subscriptions must be a list"})
                return

            validated = []
            for name in subscriptions:
                if isinstance(name, str) and name in self.server.exports:
                    validated.append(name)
            if not validated:
                self.send({"type": "error", "error": "No valid subscriptions"})
                return

            self.subscriptions = set(validated)
            self.server.register_client(self)

            initial = [
                {
                    "export": name,
                    "device": self.server.exports[name].device,
                    "port": self.server.exports[name].port,
                    "present": self.server.exports[name].present,
                    "timestamp": now_ts(),
                }
                for name in validated
            ]
            self.send(
                {
                    "type": "welcome",
                    "server_time": now_ts(),
                    "exports": initial,
                }
            )

            self.sock.settimeout(None)
            while self.alive and not self.server.stop_event.is_set():
                now = now_ts()
                if (now - self.last_ping) > PING_TIMEOUT:
                    client_addr = None
                    try:
                        client_addr = self.sock.getpeername()
                        if isinstance(client_addr, tuple) and len(client_addr) >= 2:
                            client_addr = f"{client_addr[0]}:{client_addr[1]}"
                    except Exception:
                        pass
                    LOG.warning("Client %s ping timeout, closing", client_addr or "unknown")
                    break

                r, _, _ = select.select([self.sock], [], [], 1.0)
                if not r:
                    continue
                msg = recv_json_line(self.fp)
                if msg is None:
                    break

                typ = msg.get("type")
                if typ == "ping":
                    self.last_ping = now
                    self.send({"type": "pong", "server_time": now_ts()})
                elif typ == "bye":
                    peer = None
                    try:
                        peer = self.sock.getpeername()
                    except Exception:
                        pass
                    raise ClientByeError("Client requested disconnect via bye")
        except ClientByeError:
            pass
        except Exception as exc:
            client_addr = None
            try:
                client_addr = self.sock.getpeername()
                if isinstance(client_addr, tuple) and len(client_addr) >= 2:
                    client_addr = f"{client_addr[0]}:{client_addr[1]}"
            except Exception:
                pass
            LOG.warning("Client %s connection lost: %s", client_addr or "unknown", exc)
        finally:
            client_addr = None
            try:
                client_addr = self.sock.getpeername()
                if isinstance(client_addr, tuple) and len(client_addr) >= 2:
                    client_addr = f"{client_addr[0]}:{client_addr[1]}"
            except Exception:
                pass
            LOG.info("Client %s connection closed", client_addr or "unknown")
            self.server.unregister_client(self)
            self.close()


class NbdPnpdServer:
    def __init__(self, config_path: str) -> None:
        self.config_path = config_path
        self.config = read_config(config_path)
        self.stop_event = threading.Event()
        self.listen_host = self.config.get("global", "listen_host", fallback="0.0.0.0")
        self.listen_port = self.config.getint("global", "listen_port", fallback=10800)
        self.exports: Dict[str, ExportState] = {}
        self.clients: set[ClientConnection] = set()
        self.clients_lock = threading.Lock()
        self.listen_sock: Optional[socket.socket] = None
        self.context = pyudev.Context() if pyudev is not None else None
        self._load_exports()

    def _load_exports(self) -> None:
        for section in self.config.sections():
            kind, name = parse_section_name(section)
            if kind != "export":
                continue
            device = self.config.get(section, "device", fallback="").strip()
            if not device:
                raise ValueError(f"Missing device in section {section!r}")
            device = path_for_device(device)
            port = self.config.getint(section, "port", fallback=10809)
            present = media_present(device)
            self.exports[name] = ExportState(name=name, device=device, port=port, present=present)
            LOG.info("Configured export %s -> %s (port=%s present=%s)", name, device, port, present)

    def register_client(self, client: ClientConnection) -> None:
        with self.clients_lock:
            self.clients.add(client)

    def unregister_client(self, client: ClientConnection) -> None:
        with self.clients_lock:
            self.clients.discard(client)

    def broadcast_change(self, export: ExportState) -> None:
        payload = {
            "type": "change",
            "export": export.name,
            "device": export.device,
            "port": export.port,
            "present": export.present,
            "timestamp": now_ts(),
        }
        with self.clients_lock:
            clients = list(self.clients)
        for client in clients:
            if export.name in client.subscriptions and client.alive:
                try:
                    client.send(payload)
                except Exception:
                    client.close()

    def set_state(self, export_name: str, present: bool) -> None:
        exp = self.exports[export_name]
        if exp.present == present:
            return
        exp.present = present
        LOG.info("Export %s (%s) changed: present=%s", exp.name, exp.device, present)
        self.broadcast_change(exp)

    def rescan_all(self) -> None:
        for name, exp in self.exports.items():
            current = media_present(exp.device)
            self.set_state(name, current)

    def update_device(self, device: str) -> None:
        device = path_for_device(device)
        for exp in self.exports.values():
            if os.path.realpath(path_for_device(exp.device)) == os.path.realpath(device):
                self.set_state(exp.name, media_present(exp.device))

    def monitor_thread(self) -> None:
        observer = None
        # Seed the initial state once, then rely purely on udev events to avoid
        # any periodic media probing that could disturb optical trays.
        self.rescan_all()
        monitor = pyudev.Monitor.from_netlink(self.context)
        monitor.filter_by(subsystem="block")
        observer = pyudev.MonitorObserver(monitor, callback=self._udev_callback, name="nbdpnpd-udev")
        observer.start()
        while not self.stop_event.is_set():
            time.sleep(0.5)

        try:
            observer.stop()
        except Exception:
            pass

    def _udev_callback(self, device) -> None:  # type: ignore[override]
        try:
            device_node = getattr(device, "device_node", None)
            if device_node:
                self.update_device(device_node)
        except Exception as exc:
            LOG.debug("udev callback failed: %s", exc)

    def accept_loop(self) -> None:
        assert self.listen_sock is not None
        self.listen_sock.settimeout(1.0)
        LOG.info("Listening on %s:%s", self.listen_host, self.listen_port)
        while not self.stop_event.is_set():
            try:
                conn, addr = self.listen_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            LOG.info("Client connected from %s:%s", addr[0], addr[1])
            client = ClientConnection(conn, self)
            thread = threading.Thread(target=client.handle, name=f"nbdpnpd-client-{addr[0]}:{addr[1]}", daemon=True)
            thread.start()

    def shutdown(self) -> None:
        # Send bye to all connected clients and close them.
        with self.clients_lock:
            clients = list(self.clients)
        for client in clients:
            client.close()

    def run(self) -> int:
        self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_sock.bind((self.listen_host, self.listen_port))
        self.listen_sock.listen(64)

        t = threading.Thread(target=self.monitor_thread, name="nbdpnpd-monitor", daemon=True)
        t.start()

        try:
            self.accept_loop()
        finally:
            self.stop_event.set()
            self.shutdown()
            try:
                self.listen_sock.close()
            except Exception:
                pass
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="NBD plug-and-play daemon")
    parser.add_argument("-c", "--config", required=True, help="Path to the configuration file")
    parser.add_argument("-l", "--log-level", default=None, help="Log level")
    args = parser.parse_args()

    cfg = read_config(args.config)
    log_level = args.log_level or cfg.get("global", "log_level", fallback="INFO")
    setup_logging(log_level)

    ensure_nbd_module_loaded()

    server = NbdPnpdServer(args.config)

    def _stop(_signum, _frame):
        LOG.info("Stop requested")
        server.stop_event.set()
        try:
            if server.listen_sock:
                server.listen_sock.close()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    return server.run()


if __name__ == "__main__":
    raise SystemExit(main())
