import asyncio
import secrets
import socket
import struct
import spake2
import websockets
import logging
import json

class NetworkClient:
    STUN_MAGIC_COOKIE = 0x2112A442
    MAX_PUNCH_ATTEMPTS = 20
    UDP_BUFFER_SIZE = 2048
    PUNCH_WAIT_TIMEOUT_SECONDS = 0.4

    def __init__(self, server_uri, password):
        self.server_uri = server_uri
        self.password = password
        self.websocket = None
        self.peer_endpoint = None
        self.udp_socket = None

    async def connect_and_pair(self, action, room_id):
        #action is either "host" or "join"
        self.websocket = await websockets.connect(self.server_uri)

        # send our intent to the server
        init_msg = json.dumps({"action": action, "room_id": room_id})
        await self.websocket.send(init_msg)
        logging.info(f"Sent {action} request for room {room_id}")

        # if joining, we might get an error if room doesn't exist
        # wait for the peer_conencted message from the server
        message = await self.websocket.recv()
        data = json.loads(message)

        if data.get("error"):
            logging.error(f"failed to pair: {data['error']}")
            await self.websocket.close()
            return False
        
        if data.get("action") == "peer_connected":
            logging.info("Successfully paired with peer!")
            return True
        
        return False

    @staticmethod
    def _build_stun_binding_request(transaction_id):
        return struct.pack(
            "!HHI12s",
            0x0001,  # Binding Request
            0,
            NetworkClient.STUN_MAGIC_COOKIE,
            transaction_id,
        )

    @staticmethod
    def _parse_stun_xor_mapped_address(response, transaction_id):
        if len(response) < 20:
            return None

        msg_type, msg_length, magic_cookie = struct.unpack("!HHI", response[:8])
        if msg_type != 0x0101 or magic_cookie != NetworkClient.STUN_MAGIC_COOKIE:
            return None

        expected_length = 20 + msg_length
        if len(response) < expected_length:
            return None

        offset = 20
        while offset + 4 <= expected_length:
            attr_type, attr_len = struct.unpack("!HH", response[offset : offset + 4])
            attr_start = offset + 4
            attr_end = attr_start + attr_len
            if attr_end > len(response):
                return None

            value = response[attr_start:attr_end]
            if attr_type == 0x0020 and attr_len >= 8:  # XOR-MAPPED-ADDRESS
                family = value[1]
                xor_port = struct.unpack("!H", value[2:4])[0]
                port = xor_port ^ (NetworkClient.STUN_MAGIC_COOKIE >> 16)

                if family == 0x01 and attr_len >= 8:  # IPv4
                    cookie_bytes = struct.pack("!I", NetworkClient.STUN_MAGIC_COOKIE)
                    xored_ip = value[4:8]
                    ip_bytes = bytes(b ^ cookie_bytes[i] for i, b in enumerate(xored_ip))
                    return socket.inet_ntoa(ip_bytes), port

                if family == 0x02 and attr_len >= 20:  # IPv6
                    key = struct.pack("!I", NetworkClient.STUN_MAGIC_COOKIE) + transaction_id
                    xored_ip = value[4:20]
                    ip_bytes = bytes(b ^ key[i] for i, b in enumerate(xored_ip))
                    return socket.inet_ntop(socket.AF_INET6, ip_bytes), port

            offset = attr_end
            if attr_len % 4:
                offset += 4 - (attr_len % 4)

        return None

    async def _discover_public_udp_endpoint(self, udp_socket, stun_server=("stun.l.google.com", 19302)):
        loop = asyncio.get_running_loop()
        transaction_id = secrets.token_bytes(12)
        request = self._build_stun_binding_request(transaction_id)

        try:
            await loop.sock_sendto(udp_socket, request, stun_server)
            response, _ = await asyncio.wait_for(loop.sock_recvfrom(udp_socket, 2048), timeout=3)
        except Exception as error:
            logging.warning(f"STUN lookup failed: {error}")
            return None

        mapped_address = self._parse_stun_xor_mapped_address(response, transaction_id)
        if mapped_address:
            logging.info(f"Discovered public endpoint via STUN: {mapped_address[0]}:{mapped_address[1]}")
        else:
            logging.warning("STUN response received but no usable mapped address found")
        return mapped_address

    @staticmethod
    def _get_local_ip():
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("8.8.8.8", 80))
            return probe.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            probe.close()

    async def _exchange_p2p_candidates(self, local_candidates):
        await self.websocket.send(
            json.dumps(
                {
                    "action": "p2p_candidates",
                    "candidates": local_candidates,
                }
            )
        )

        while True:
            peer_message_raw = await self.websocket.recv()
            peer_data = json.loads(peer_message_raw)
            if peer_data.get("action") == "p2p_candidates":
                return peer_data.get("candidates", [])

    async def key_exchange(self, is_host):
        logging.info(f"starting key exchange as (is_host={is_host})")
        # 1. initialize SPAKE2
        # the password must be bytes. we must use the room_id/code as the password to ensure both sides derive the same key
        password_bytes = self.password.encode('utf-8')

        if is_host:
            shared_key_instance = spake2.SPAKE2_A(password_bytes)
        else:
            shared_key_instance = spake2.SPAKE2_B(password_bytes)

        # 2. get our public message ot send to the peer
        my_msg = shared_key_instance.start()
        logging.info(f"Generated SPAKE2 message: {my_msg.hex()[:16]}...")

        #send it via the websocket 
        try:
            await self.websocket.send(json.dumps({
                "action": "key_exchange",
                "payload": my_msg.hex() # send as hex string
            }))
            logging.info("Sent SPAKE2 payload to server")
        except Exception as e:
            logging.error(f"Failed to send SPAKE2 payload: {e}")
            return None

        # wait for the peer's message
        logging.info("Waiting for peer's SPAKE2 payload...")
        try:
            peer_message_raw = await self.websocket.recv()
            logging.info("Received peer's SPAKE2 payload")
        except Exception as e:
            logging.error(f"Failed to receive SPAKE2 payload from peer: {e}")
            raise e # let it crash so we can see the trace if it happens here

        peer_data = json.loads(peer_message_raw)

        if peer_data.get("action") != "key_exchange":
            logging.error("Expected key_exchange")
            return None
        
        peer_msg_bytes = bytes.fromhex(peer_data["payload"])

        # 3. process the peer's message to derive the shared key
        logging.info("Deriving final shared key...")
        try:
            self.session_key = shared_key_instance.finish(peer_msg_bytes)
            logging.info("Key exchange successful, derived session key ready for secure transfer")
            return self.session_key
        except spake2.KeyGenError:
            logging.error("Key exchange failed, possibly due to incorrect password or tampered messages")
            return None

    async def establish_p2p_connection(self):
        loop = asyncio.get_running_loop()
        local_ip = self._get_local_ip()
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind((local_ip, 0))
        udp_socket.setblocking(False)
        self.udp_socket = udp_socket

        local_port = udp_socket.getsockname()[1]
        public_endpoint = await self._discover_public_udp_endpoint(udp_socket)

        local_candidates = [{"ip": local_ip, "port": local_port, "type": "local"}]
        if public_endpoint:
            local_candidates.append(
                {
                    "ip": public_endpoint[0],
                    "port": public_endpoint[1],
                    "type": "stun",
                }
            )

        peer_candidates = await self._exchange_p2p_candidates(local_candidates)
        peer_addresses = []
        for candidate in peer_candidates:
            ip = candidate.get("ip")
            port = candidate.get("port")
            if ip and isinstance(port, int):
                peer_addresses.append((ip, port))

        if not peer_addresses:
            logging.error("No valid peer candidates were received")
            return None

        punch_payload = b"PYTRANSFER_PUNCH"
        for _ in range(self.MAX_PUNCH_ATTEMPTS):
            for peer_address in peer_addresses:
                try:
                    await loop.sock_sendto(udp_socket, punch_payload, peer_address)
                except OSError:
                    continue

            try:
                packet, addr = await asyncio.wait_for(
                    loop.sock_recvfrom(udp_socket, self.UDP_BUFFER_SIZE),
                    timeout=self.PUNCH_WAIT_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                continue

            if packet in {b"PYTRANSFER_PUNCH", b"PYTRANSFER_ACK"}:
                await loop.sock_sendto(udp_socket, b"PYTRANSFER_ACK", addr)
                self.peer_endpoint = addr
                logging.info(f"Established direct UDP peer endpoint: {addr[0]}:{addr[1]}")
                return addr

        logging.error("Failed to establish direct peer-to-peer UDP path")
        return None

    def close_p2p_socket(self):
        if self.udp_socket is not None:
            self.udp_socket.close()
            self.udp_socket = None

    async def exchange_ips(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            #doesnt have to be reachable, just gets the local IP routing correctly
            s.connect(("10.255.255.255", 1))
            my_ip = s.getsockname()[0]
        except Exception:
            my_ip = "127.0.0.1"
        finally:
            s.close()
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        logging.info(f"Determined local IP as {my_ip}")
        #send it to the peer via the relay server
        try:
            await self.websocket.send(json.dumps({
                "action": "ip_exchange",
                "ip": my_ip,
                "local_ip": local_ip
            }))

        # wait for peers IP

            peer_message_raw = await self.websocket.recv()
            peer_data = json.loads(peer_message_raw)

            if peer_data.get("action") == "ip_exchange":
                peer_ip = peer_data.get("ip")
                logging.info(f"sucsessfully received peer IP: {peer_ip}")
                peer_local_ip = peer_data.get("local_ip")
                logging.info(f"sucsessfully received peer local IP: {peer_local_ip}")
                return peer_ip
                
        
            logging.error("Failed to exchange IPs")
            return None
        except websockets.exceptions.ConnectionClosed:
            logging.error("Connection closed during IP exchange")
            return None

    async def excange_ips(self):
        return await self.exchange_ips()
