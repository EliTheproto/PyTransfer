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
            # start NAT traversal
    
if __name__ == "__main__":
    asyncio.run(main())
