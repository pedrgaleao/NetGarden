import datetime
import socket
import threading

from bson import BSON

from NetGarden.CORE.Packet import Packet


class Proxy:
    BUFFER_SIZE = 8192
    MAX_FRAME_SIZE = 5_000_000
    CONNECT_TIMEOUT = 10
    ACCEPT_TIMEOUT = 1.0

    def __init__(
        self,
        listen_host,
        listen_port,
        server_host,
        server_port,
        on_packet,
        on_log,
        on_close=None,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.server_host = server_host
        self.server_port = server_port
        self.on_packet = on_packet
        self.on_log = on_log
        self.on_close = on_close

        self._thread = None
        self._stop_flag = threading.Event()
        self._sockets_lock = threading.Lock()
        self._sockets = set()

    def log(self, msg):
        if self.on_log:
            try:
                self.on_log(str(msg))
            except Exception:
                pass

    def start(self):
        if self._thread and self._thread.is_alive():
            return

        self._stop_flag.clear()

        self._thread = threading.Thread(
            target=self._run,
            name="NetGarden-Proxy",
            daemon=True,
        )

        self._thread.start()

    def stop(self):
        self._stop_flag.set()
        self._close_all_sockets()

    def _now_ts(self):
        return datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _register_socket(self, sock):
        if sock is not None:
            with self._sockets_lock:
                self._sockets.add(sock)

    def _unregister_socket(self, sock):
        if sock is not None:
            with self._sockets_lock:
                self._sockets.discard(sock)

    def _close_socket(self, sock):
        if sock is None:
            return

        self._unregister_socket(sock)

        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        try:
            sock.close()
        except Exception:
            pass

    def _close_all_sockets(self):
        with self._sockets_lock:
            sockets = list(self._sockets)
            self._sockets.clear()

        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass

            try:
                sock.close()
            except Exception:
                pass

    def _run(self):
        listener = None

        try:
            listener = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            listener.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1,
            )

            listener.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1,
            )

            listener.bind(
                (
                    self.listen_host,
                    self.listen_port,
                )
            )

            listener.listen(1)
            listener.settimeout(self.ACCEPT_TIMEOUT)

            self._register_socket(listener)

            self.log(
                f"[NetGarden] Listening on "
                f"{self.listen_host}:{self.listen_port}"
            )

            while not self._stop_flag.is_set():
                client_sock = None
                server_sock = None
                client_thread = None
                server_thread = None

                try:
                    try:
                        client_sock, addr = listener.accept()
                    except socket.timeout:
                        continue

                    if self._stop_flag.is_set():
                        self._close_socket(client_sock)
                        break

                    self._register_socket(client_sock)

                    client_sock.setsockopt(
                        socket.SOL_SOCKET,
                        socket.SO_KEEPALIVE,
                        1,
                    )

                    self.log(
                        f"[NetGarden] Client connected: {addr}"
                    )

                    server_sock = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                    )

                    server_sock.settimeout(
                        self.CONNECT_TIMEOUT
                    )

                    server_sock.setsockopt(
                        socket.SOL_SOCKET,
                        socket.SO_KEEPALIVE,
                        1,
                    )

                    self._register_socket(server_sock)

                    server_sock.connect(
                        (
                            self.server_host,
                            self.server_port,
                        )
                    )

                    server_sock.settimeout(None)
                    client_sock.settimeout(None)

                    self.log(
                        f"[NetGarden] Connected to server "
                        f"{self.server_host}:{self.server_port}"
                    )

                    client_thread = threading.Thread(
                        target=self._pipe,
                        args=(
                            client_sock,
                            server_sock,
                            "client",
                        ),
                        name="NetGarden-ClientPipe",
                        daemon=True,
                    )

                    server_thread = threading.Thread(
                        target=self._pipe,
                        args=(
                            server_sock,
                            client_sock,
                            "server",
                        ),
                        name="NetGarden-ServerPipe",
                        daemon=True,
                    )

                    client_thread.start()
                    server_thread.start()

                    while (
                        not self._stop_flag.is_set()
                        and client_thread.is_alive()
                        and server_thread.is_alive()
                    ):
                        client_thread.join(0.2)
                        server_thread.join(0.2)

                except Exception as e:
                    if not self._stop_flag.is_set():
                        self.log(
                            f"[ERROR] Connection error: {e}"
                        )

                finally:
                    self._close_socket(client_sock)
                    self._close_socket(server_sock)

                    if not self._stop_flag.is_set():
                        self.log(
                            "[NetGarden] Connection closed"
                        )

        except Exception as e:
            if not self._stop_flag.is_set():
                self.log(
                    f"[ERROR] Proxy run error: {e}"
                )

        finally:
            self._close_socket(listener)

            try:
                if self.on_close:
                    self.on_close()
            except Exception:
                pass

    def _pipe(self, source, destination, direction):
        buffer = bytearray()

        try:
            while not self._stop_flag.is_set():
                chunk = source.recv(self.BUFFER_SIZE)

                if not chunk:
                    self.log(
                        f"[NetGarden] Pipe closed by peer: "
                        f"{direction}"
                    )
                    break

                buffer.extend(chunk)

                while len(buffer) >= 4:
                    length = int.from_bytes(
                        buffer[0:4],
                        "little",
                        signed=False,
                    )

                    if (
                        length < 4
                        or length > self.MAX_FRAME_SIZE
                    ):
                        self.log(
                            f"[ERROR] Invalid length={length} "
                            f"({direction})"
                        )

                        buffer.clear()
                        break

                    if len(buffer) < length:
                        break

                    frame = bytes(buffer[:length])
                    del buffer[:length]

                    try:
                        destination.sendall(frame)
                    except (
                        BrokenPipeError,
                        ConnectionResetError,
                        ConnectionAbortedError,
                        OSError,
                    ) as e:
                        self.log(
                            f"[NetGarden] Destination closed "
                            f"({direction}): {e}"
                        )
                        return

                    self._process_packet(
                        frame,
                        direction,
                    )

        except (
            ConnectionResetError,
            BrokenPipeError,
            ConnectionAbortedError,
            OSError,
        ) as e:
            if not self._stop_flag.is_set():
                self.log(
                    f"[ERROR] Pipe connection error "
                    f"({direction}): {e}"
                )

        except Exception as e:
            if not self._stop_flag.is_set():
                self.log(
                    f"[ERROR] Pipe error "
                    f"({direction}): {e}"
                )

        finally:
            self._close_socket(source)
            self._close_socket(destination)

    def _process_packet(self, frame, direction):
        bson_data = frame[4:]
        parsed = None
        packet_id = "?"

        try:
            parsed = BSON(bson_data).decode()

            if (
                isinstance(parsed, dict)
                and "ID" in parsed
            ):
                packet_id = str(
                    parsed.get("ID")
                )

        except Exception as e:
            self.log(
                f"[WARN] BSON decode failed "
                f"({direction}): {e}"
            )

        packet = Packet(
            direction=direction,
            raw_frame=frame,
            parsed=parsed,
            packet_id=packet_id,
            timestamp=self._now_ts(),
        )

        if self.on_packet:
            try:
                self.on_packet(packet)
            except Exception as e:
                self.log(
                    f"[ERROR] Packet callback failed: {e}"
                )