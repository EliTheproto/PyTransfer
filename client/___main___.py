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
            
            #---START IP EXCHANGE---
            peer_ip = await client.excange_ips()
            if peer_ip:
                print(f"peers IP address is: {peer_ip}")
                #TODO start direct peer to peer connection here
            # ---END IP EXCHANGE 
            
            logging.info("press ctrl+C to exit")
            try:
                await asyncio.Future() # run forever
            except asyncio.CancelledError:
                pass
            
            # start NAT traversal
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
