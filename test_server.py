import asyncio
import json
import unittest
import websockets
from server.server import NetworkServer

class TestNetworkServer(unittest.IsolatedAsyncioTestCase):
    
    async def asyncSetUp(self):
        # We start the server in the background for every test
        self.server = NetworkServer(host="localhost", port=8765, password=None)
        
        # Start the websockets server and store the 'serving' object so we can close it later
        self.server_task = await websockets.serve(self.server.handler, self.server.host, self.server.port)
        
    async def asyncTearDown(self):
        # Shut down the server gracefully after each test
        self.server_task.close()
        await self.server_task.wait_closed()

    async def test_join_nonexistent_room(self):
        uri = "ws://localhost:8765"
        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({"action": "join", "room_id": "9999"}))
            response = await ws.recv()
            data = json.loads(response)
            
            # Assert that the server rejected the join attempt
            self.assertEqual(data.get("error"), "Room not found")

    async def test_successful_pairing_and_relay(self):
        uri = "ws://localhost:8765"
        room_id = "test_room_123"

        # 1. Connect Client A (Host)
        host_ws = await websockets.connect(uri)
        await host_ws.send(json.dumps({"action": "host", "room_id": room_id}))
        
        # Yield control slightly so the server processes the host message
        await asyncio.sleep(0.1) 
        
        # Assert the room was actually created on the server
        self.assertIn(room_id, self.server.rooms)

        # 2. Connect Client B (Joiner)
        joiner_ws = await websockets.connect(uri)
        await joiner_ws.send(json.dumps({"action": "join", "room_id": room_id}))

        # 3. Assert BOTH clients receive the 'peer_connected' message
        host_response = json.loads(await host_ws.recv())
        joiner_response = json.loads(await joiner_ws.recv())
        
        self.assertEqual(host_response.get("action"), "peer_connected")
        self.assertEqual(joiner_response.get("action"), "peer_connected")

        # 4. Test the Relay Pipe (Simulating SPAKE2 payloads)
        # Host sends a message to Joiner
        await host_ws.send("Hello from Host!")
        msg_at_joiner = await joiner_ws.recv()
        self.assertEqual(msg_at_joiner, "Hello from Host!")

        # Joiner sends a message to Host
        await joiner_ws.send("Hello from Joiner!")
        msg_at_host = await host_ws.recv()
        self.assertEqual(msg_at_host, "Hello from Joiner!")

        # 5. Test relaying of P2P signaling payloads
        host_candidates = {
            "action": "p2p_candidates",
            "candidates": [{"ip": "203.0.113.10", "port": 50001, "type": "stun"}],
        }
        await host_ws.send(json.dumps(host_candidates))
        joiner_signal = json.loads(await joiner_ws.recv())
        self.assertEqual(joiner_signal.get("action"), "p2p_candidates")
        self.assertEqual(joiner_signal.get("candidates")[0]["port"], 50001)

        joiner_candidates = {
            "action": "p2p_candidates",
            "candidates": [{"ip": "192.168.1.20", "port": 50002, "type": "local"}],
        }
        await joiner_ws.send(json.dumps(joiner_candidates))
        host_signal = json.loads(await host_ws.recv())
        self.assertEqual(host_signal.get("action"), "p2p_candidates")
        self.assertEqual(host_signal.get("candidates")[0]["ip"], "192.168.1.20")

        # Cleanup
        await host_ws.close()
        await joiner_ws.close()
