import asyncio
import websockets
import logging
import json

class NetworkServer:
    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
        self.rooms = {} # room_id -> set of websockets

    async def handler(self, websocket):
        try:
            # 1, wait for the client
            message = await websocket.recv()
            data = json.loads(message)

            action = data.get("action") # "join" or "host"
            room_id = data.get("room_id")

            if action == "host":
                await self._handle_host(websocket, room_id)
            elif action == "join":
                await self._handle_join(websocket, room_id)
            else:
                await websocket.send(json.dumps({"error": "Invalid action"}))
            
        except websockets.exceptions.ConnectionClosed:
            logging.info("Connection closed")


    async def _handle_host(self, websocket, room_id):
        #prevernt overwriting existing room
        if room_id in self.rooms:
            await websocket.send(json.dumps({"error": "Room already exists"}))
            return
        
        # Create heavily modified room object that includes an event
        # so this method knows when to stop blocking
        peer_joined_event = asyncio.Event()
        
        #create new room and wait for connection
        self.rooms[room_id] = {
            "host": websocket,
            "client": None,
            "connected_event": peer_joined_event
        }
        logging.info(f"Room {room_id} created, waiting for peer")

        try:
            # Wait securely without reading the websocket
            # This is a bit hacky but it allows us to block until the peer connects without consuming any messages from the websocket
            
            # Create a task to watch if the socket dies while waiting
            async def wait_socket_close():
                await websocket.wait_closed()
                logging.info(f"Host for room {room_id} disconnected while waiting for peer, closing")
            
            close_task = asyncio.create_task(wait_socket_close())
            event_task = asyncio.create_task(peer_joined_event.wait())

            done, pending = await asyncio.wait(
                [close_task, event_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel whatever didnt finish
            for task in pending:
                task.cancel()
            
            # If the socket closed before the event fired, cleanup
            if close_task in done:
                raise websockets.exceptions.ConnectionClosed(None, None)
            
            # If event_task finished, it means the joiner arrived. 
            # We exit cleanly and _handle_join takes over.
            if event_task in done:
                while room_id in self.rooms:
                    await asyncio.sleep(0.5)

        except websockets.exceptions.ConnectionClosed:
            #cleanup if connection drops early.
            if room_id in self.rooms:
                del self.rooms[room_id]
            logging.info(f"Host for room {room_id} disconnected, room closed")


    async def _handle_join(self, websocket, room_id):
        if room_id not in self.rooms:
            await websocket.send(json.dumps({"error" : "Room not found"}))
            return
        
        host_ws = self.rooms[room_id]["host"]
        self.rooms[room_id]["client"] = websocket
        logging.info(f"Client joined room {room_id}")

        self.rooms[room_id]["connected_event"].set()

        #notify host client has joined
        ready_msg = json.dumps({"action": "peer_connected"})
        await host_ws.send(ready_msg)
        await websocket.send(ready_msg)

        #start relaying messages between host and client
        await self._relay_messages(host_ws, websocket, room_id)

    async def _relay_messages(self, ws1, ws2, room_id):
        #create helper funcion to forward messages between peers:
        async def forward(src, dst):
            try:
                async for message in src:
                    await dst.send(message)
                    logging.info(f"Relayed message in room {room_id}: {message}")
            except websockets.exceptions.ConnectionClosed:
                pass
        
        # run both tasks concurrently

        task1 = asyncio.create_task(forward(ws1, ws2))
        task2 = asyncio.create_task(forward(ws2, ws1))

        # wait until ONE of the tasks completes

        await asyncio.wait([task1, task2], return_when=asyncio.FIRST_COMPLETED)

        #cleanup

        task1.cancel()
        task2.cancel()
        if room_id in self.rooms:
            del self.rooms[room_id]
        logging.info(f"Room {room_id} closed, peers disconnected")

    async def start(self):
        logging.info(f"starting server on {self.host}:{self.port}")
        #pass self.handler to act as the callback for incoming connections
        async with websockets.serve(self.handler, self.host, self.port):
            await asyncio.Future() # run forever

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = NetworkServer(host="localhost", port=8765, password= None)
    asyncio.run(server.start())
