import os 
import asyncio
import aiostun
import spake2
import websockets
import logging
import json
import socket

class NetworkClient:
    def __init__(self, server_uri, password):
        self.server_uri = server_uri
        self.password = password
        self.websocket = None 

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
    
    async def key_exchange(self, is_host):
        logging.info(f"starting key exchange as (is_host={is_host})")
        # 1. initalze SPAKE2
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

    async def exchange_ips(self):
        #p Create a robust way to find the local IP
        local_ip = "127.0.0.1"
        try:
            # 1. Try the Internet route first 
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            candidate_ip = s.getsockname()[0]
            s.close

            # if It give us an APIPA address, ignore and search manually
            if candidate_ip.startswith("169.254."):
                hostname = socket.gethostname()
                # get all IPv4 addresses resolved for this hostname
                addrs = socket.getaddrinfo(hostname, None, socket.AF_INET)

                # filter for valid lan IPS (192.168.x.x, 10.x.x.x, 172.16.x.x)
                for addr in addrs:
                    ip = addr[4][0]
                    if ip.startswith("192.168.") or ip.startswith("10.") or (ip.startswith("172.") and 16 <= int(ip.split('.')[1]) <= 31):
                        local_ip = ip
                        break
            else:
                local_ip = candidate_ip
        except Exception as e:
            logging.error("failed to auto-detect IP")
        
        logging.info(f"Determined local IP as {local_ip}")

        # get public IP and NAT port using STUN
        # we can use the same STUN server for both clients since they just need to know
        # their own public IP/port, not the peers
        public_ip = None
        public_port = None
        try:
            async with aiostun.Client(host="stun.l.google.com", port=19302) as stun_client:
                mapped_addr = await stun_client.get_mapped_address()
                # mapped_addr is usually a dict or tuple
                public_ip = mapped_addr['ip']
                public_port = mapped_addr['port']
                logging.info(f"STUN discovery sucsessful: Public IP {public_ip}:{public_port}")
        except Exception as e:
            logging.error(f"STUN Discovery failed: {e}")
            
        #send it to the peer via the relay server
        try:
            await self.websocket.send(json.dumps({
                "action": "ip_exchange",
                "local_ip": local_ip,
                "public_ip": public_ip,
                "public_port": public_port
            }))

        # wait for peers IP

            peer_message_raw = await self.websocket.recv()
            peer_data = json.loads(peer_message_raw)

            if peer_data.get("action") == "ip_exchange":
                peer_ip = peer_data.get("ip")
                logging.info(f"sucsessfully received peer IP: {peer_ip}")
                peer_local_ip = peer_data.get("local_ip")
                logging.info(f"sucsessfully received peer local IP: {peer_local_ip}")
                peer_public_ip = peer_data.get("public_ip")
                peer_public_port = peer_data.get("public_port")
                logging.info(f"sucsessfully received peer public IP: {peer_public_ip}:{peer_public_port}")

                # return both so the hole punch can try both
                return {
                    "local": (peer_local_ip, peer_public_port if peer_public_port else 0),
                    "public": (peer_public_ip, peer_public_port)
                }
                
        
            logging.error("Failed to exchange IPs")
            return None
        except websockets.exceptions.ConnectionClosed:
            logging.error("Connection closed during IP exchange")
            return None
    
    async def upgrade_to_p2p(self, peer_endpoints):
        # Attepts UDP hole punching using the exchanged IP addr
        #returns the active p2p socket and adress if successful, or None if it fails.

        # create a UDP socket for p2p 
        self.p2p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # we need this to be non-blocking for asyncio
        self.p2p_socket.setblocking(False)

        # bind any local port (or the STUN port)
        self.p2p_socket.bind(("0.0.0.0", 0))
        my_port = self.p2p_socket.getsockname()[1]
        logging.info(f"local UDP socket bound to port {my_port}")

        peer_local = tuple(peer_endpoints["local"])
        peer_public = tuple(peer_endpoints["public"])

        punch_message = b"HOLE_PUNCH_PING"

        #setup asyncio furutre to resolve when we receive a valid p2p ping
        loop = asyncio.get_running_loop()
        p2p_connected = loop.create_future()

        active_peer_addr = None

        def datagram_received():
            nonlocal active_peer_addr
            try:
                data, addr, = self.p2p_socket.recvfrom(1024)
                if data == punch_message:
                    logging.info(f"Recived P2P ping directly from {addr}")
                    if not p2p_connected.done():
                        active_peer_addr = addr
                        p2p_connected.set_result(True)
                elif data == b"HOLE_PUNCH_ACK":
                    logging.info(f"Recived P2P ACK directly from {addr}, connection established")
                    if not p2p_connected.done():
                        active_peer_addr = addr
                        p2p_connected.set_result(True)
            except BlockingIOError:
                pass
            except Exception as e:
                logging.error(f"Error reading from P2P socket: {e}")
        
        # register the socker to the asyncio event loop to listen for incoming packets
        loop.add_reader(self.p2p_socket.fileno(), datagram_received)

        logging.info(f"starting UDP hole punching... ({peer_public})")

        # try sending pings to both local and public addr for 5 seconds
        for _ in range(10): # 10 attempts 0.5s appart
            if p2p_connected.done():
                break

            try:
                #try local IP
                if peer_local[0] and peer_local[1]:
                    self.p2p_socket.sendto(punch_message, peer_local)
                    logging.info(f"Sent P2P ping to local address: {peer_local},{punch_message}")

                #try public ip
                if peer_public[0] and peer_public[1]:
                    self.p2p_socket.sendto(punch_message, peer_public)
                    logging.info(f"Sent P2P ping to public address: {peer_public},{punch_message}")
            
            except Exception as e:
                logging.error(f"Hole punch and send error: {e}")

            await asyncio.sleep(0.5)
        
        # stop listening for reader events temporarily
        loop.remove_reader(self.p2p_socket.fileno())

        if p2p_connected.done():
            logging.info(f"sucsessfully upgraded to P2P, direct connection to {active_peer_addr}")

            #send an ack back on the sucsessful path 
            self.p2p_socket.sendto(b"HOLE_PUNCH_ACK", active_peer_addr)
            return self.p2p_socket, active_peer_addr
        else:
            logging.warning("failed to establish P2P, falling back to relay server")
            self.p2p_socket.close()
            self.p2p_socket = None
            return None, None