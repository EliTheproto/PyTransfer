import os 
import asyncio
import aiostun
#import aioice
import spake2
import websockets
import logging
import json

class NetworkClient:
    def __init__(self, server_uri, password):
        self.server_uri = server_uri
        self.password = password
        self.websocket = None
    #self.stun_client = aiostun.Client()
        #self.ice_agent = aioice.Agent()

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

    async def excange_ips(self):
        #placeholder for future NAT traversal implementation
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            #doesnt have to be reachable, just gets the local IP routing correctly
            s.connect(("10.255.255.255", 1))
            my_ip = s.getsockname()[0]
        except Exception:
            my_ip = "127.0.0.1"
        finally:
            s.close()
        
        logging.info(f"Determined local IP as {my_ip}")
        #send it to the peer via the relay server

        await self.websocket.send(json.dumps({
            "action": "ip_exchange",
            "ip": my_ip
        }))

        # wait for peers IP

        peer_message_raw = await self.websocket.recv()
        peer_data = json.loads(peer_message_raw)

        if peer_data.get("action") == "ip_exchange":
            peer_ip = peer_data.get("ip")
            logging.info(f"sucsessfully received peer IP: {peer_ip}")
            return peer_ip
        
        logging.error("Failed to exchange IPs")
        return None