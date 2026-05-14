import asyncio
from network import NetworkClient
import logging

async def main():
    action = input("Do you want to host or join a room? (host/join): ").strip().lower()
    room_code = "1234" # in a real app, generate this dynamically or let user choose
    server_uri = "ws://localhost:8765"

    # init client (pass room_code as password for key exchange)
    client = NetworkClient(server_uri, password=room_code)

    # 1. connect to server and wait for peer
    
    is_paired = await client.connect_and_pair(action, room_code)

    if is_paired:
        # 2. establish secure key
        is_host = (action == "host")
        key = await client.key_exchange(is_host)

        if key:
            print(f"Secure key established: {key[:4].hex()}...")
            
            peer_endpoint = await client.establish_p2p_connection()
            if peer_endpoint:
                print(f"Direct P2P endpoint established with peer: {peer_endpoint[0]}:{peer_endpoint[1]}")
                try:
                    await client.websocket.close()
                except Exception as error:
                    logging.warning(
                        f"Failed to close websocket signaling channel cleanly ({type(error).__name__}): {error}"
                    )
                logging.info("Websocket signaling channel closed after P2P upgrade")
                logging.info("press ctrl+C to exit")
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    pass
                finally:
                    if client.websocket and not client.websocket.closed:
                        await client.websocket.close()
                    client.close_p2p_socket()
                return
             
            logging.info("press ctrl+C to exit")
            try:
                # instead of waiting for nothing, wait for the socket to close
                await client.websocket.wait_closed()
                logging.info("Websocket connection closed, exiting")
            except asyncio.CancelledError:
                pass
            #try:
            #    await asyncio.Future() # run forever
            #except asyncio.CancelledError:
            #   pass
            
            # start NAT traversal
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
