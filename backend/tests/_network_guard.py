"""The suite is offline by construction (Phase 266).

Installed by ``conftest.py`` for every run that is not ``--run-live``: a
connection to anything but the loopback interface (or a Unix socket), and a
DNS lookup for any name but ``localhost``, raises :class:`NetworkBlocked`
and is recorded. The ``_network_guard_check`` fixture in ``conftest.py``
fails the test during which the attempt happened.

Why both raise *and* record: code under test catches network errors — that
is its job — so an exception alone would be swallowed and turn a real
request into a quietly degraded result. The record is what makes the test
fail. ``NetworkBlocked`` subclasses ``OSError`` so the code still takes the
path it would take on a real outage, which keeps the failure message about
the request rather than about a crash it provoked.

Before this, any ``with TestClient(app)`` ran the lifespan warm-up and
downloaded the ClimateTRACE CSVs and the GLEIF GEM↔LEI mapping; one
securities test took 29 s and CI failed whenever storage.googleapis.com or
mapping.gleif.org had a bad morning. The warm-up is now off under test
(``OPENCHECK_WARM_CACHES_ON_START=0``), and this guard is what proves
nothing else reaches out.

Patching happens on ``socket.socket`` (the Python subclass every caller —
httpx, asyncio's selector loop, urllib — instantiates) and on the
module-level ``getaddrinfo`` / ``create_connection``. respx and
pytest-httpx intercept at the httpx transport, above the socket, so mocked
requests never reach it.
"""

from __future__ import annotations

import ipaddress
import os
import socket
import threading
import traceback

# A proxy on loopback (a developer's, or a sandbox's) would carry every
# request past the guard as a "local" connection, so the proxy variables are
# removed for the run. httpx, urllib and requests all read them at call time.
_PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")

_LOCAL_NAMES = frozenset({"localhost", "localhost.localdomain", "ip6-localhost", "testserver"})

_lock = threading.Lock()
_attempts: list[str] = []
_installed = False
_originals: dict[str, object] = {}


class NetworkBlocked(OSError):
    """A test tried to reach the network."""


def _is_local_host(host: object) -> bool:
    if host is None:
        return True
    if isinstance(host, bytes):
        host = host.decode("ascii", "replace")
    if not isinstance(host, str):
        return False
    name = host.strip("[]").lower()
    if name in _LOCAL_NAMES or name == "":
        return True
    try:
        return ipaddress.ip_address(name.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _where() -> str:
    """The innermost frames outside the socket layer — enough to name the caller."""
    frames = [
        f
        for f in traceback.extract_stack()[:-3]
        if "/socket.py" not in f.filename
        and "/asyncio/" not in f.filename
        and "_network_guard" not in f.filename
    ]
    return " <- ".join(f"{f.filename.rsplit('/', 2)[-1]}:{f.lineno}" for f in frames[-3:][::-1])


def _block(what: str) -> None:
    message = f"{what} (from {_where()})"
    with _lock:
        _attempts.append(message)
    raise NetworkBlocked(
        f"Network access is disabled in the test suite: {what}. Mock it with "
        "respx, or mark the test @pytest.mark.live."
    )


def _address_host(family: int, address: object) -> object:
    if family == getattr(socket, "AF_UNIX", object()):
        return "localhost"
    if isinstance(address, tuple) and address:
        return address[0]
    return address


def install() -> None:
    """Patch the socket layer. Idempotent."""
    global _installed
    if _installed:
        return
    _installed = True

    orig_connect = socket.socket.connect
    orig_connect_ex = socket.socket.connect_ex
    orig_getaddrinfo = socket.getaddrinfo
    orig_create_connection = socket.create_connection
    _originals.update(
        connect=orig_connect,
        connect_ex=orig_connect_ex,
        getaddrinfo=orig_getaddrinfo,
        create_connection=orig_create_connection,
    )

    def connect(self, address):  # type: ignore[no-untyped-def]
        host = _address_host(self.family, address)
        if not _is_local_host(host):
            _block(f"connect to {address!r}")
        return orig_connect(self, address)

    def connect_ex(self, address):  # type: ignore[no-untyped-def]
        host = _address_host(self.family, address)
        if not _is_local_host(host):
            _block(f"connect to {address!r}")
        return orig_connect_ex(self, address)

    def getaddrinfo(host, *args, **kwargs):  # type: ignore[no-untyped-def]
        if not _is_local_host(host):
            _block(f"DNS lookup for {host!r}")
        return orig_getaddrinfo(host, *args, **kwargs)

    def create_connection(address, *args, **kwargs):  # type: ignore[no-untyped-def]
        if not _is_local_host(address[0] if isinstance(address, tuple) else address):
            _block(f"connection to {address!r}")
        return orig_create_connection(address, *args, **kwargs)

    _originals["proxy_env"] = {k: os.environ.pop(k) for k in _PROXY_VARS if k in os.environ}

    socket.socket.connect = connect  # type: ignore[method-assign]
    socket.socket.connect_ex = connect_ex  # type: ignore[method-assign]
    socket.getaddrinfo = getaddrinfo
    socket.create_connection = create_connection


def uninstall() -> None:
    """Restore the socket layer (used by the guard's own test)."""
    global _installed
    if not _installed:
        return
    socket.socket.connect = _originals["connect"]  # type: ignore[method-assign]
    socket.socket.connect_ex = _originals["connect_ex"]  # type: ignore[method-assign]
    socket.getaddrinfo = _originals["getaddrinfo"]  # type: ignore[assignment]
    socket.create_connection = _originals["create_connection"]  # type: ignore[assignment]
    os.environ.update(_originals.get("proxy_env") or {})  # type: ignore[arg-type]
    _installed = False


def installed() -> bool:
    return _installed


def drain() -> list[str]:
    """Return and clear the attempts recorded since the last drain."""
    with _lock:
        out = list(_attempts)
        _attempts.clear()
    return out
