# nbdpnp

*NOTE: Parts of this project were generated with the help of AI tools.*

This project adds plug-and-play functionality to nbd-server, making it particularly suitable for sharing optical drives with removable media, such as CD/DVD or Blu-ray drives, with one or more client computers. 
This is achieved through two Python 3 services:

- `nbdpnpd`: watches configured optical drives on the server and broadcasts insertion/removal changes over TCP.
- `nbdpnpc`: connects to one or more servers, attaches local NBD devices with `nbd-client`, and mounts them with `udisksctl`.

## Files

- `nbdpnpd.py`
- `nbdpnpc.py`
- `nbdpnp_common.py`
- `nbdpnpd.conf.example`
- `nbdpnpc.conf.example`
- `nbdpnpd.service`
- `nbdpnpc.service`

## Runtime dependencies

- Python 3
- `pyudev` (on the server side)
- `nbd-client`
- `udisksctl` from UDisks2
- `mount`, `umount`
- `udevadm`

## Notes

- Currently there is no authentication or encryption support, so this is only useful for LANs.
- The protocol is newline-delimited JSON over TCP.
- The client periodically reconnects when a server is offline.
- The client searches the next free `/dev/nbdX` dynamically for each active export.
- The client uses a bind mount when a custom `mountpoint` is configured.
