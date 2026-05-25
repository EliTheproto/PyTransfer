import asyncio
from network import NetworkClient
import logging
import os
import sys
from transfer import SecureFileTransfer

async def main():
    action = input("Do you want to host or join a room? (host/join): ").strip().lower()
    room_code = "1234" # in a real app, generate this dynamically or let user choose
    server_uri = "ws://localhost:8765" # change to your server's address

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
            peer_ip = await client.exchange_ips()
            if peer_ip:
                print(f"peers IP address is: {peer_ip}")
                #TODO start direct peer to peer connection here
            # ---END IP EXCHANGE 
            
            logging.info("press ctrl+C to exit")
            
            peer_endpoints = await client.exchange_ips()
            if peer_endpoints:
                logging.info(f"peer_endpoints: peer IPs: {peer_endpoints}")
                # Start NAT Traversal
                p2p_sock, p2p_addr = await client.upgrade_to_p2p(peer_endpoints)

                if p2p_sock:
                    logging.info(f"p2p is active directly with {p2p_addr}")
                    # Now you can use p2p_sock to send/recv data directly with the peer
                    # alongside client.websocket for relay fallback
                else:
                    logging.warning("Failed to establish P2P connection, will rely on relay")
            
            # --- START FILE TRANSFER ---
            file_transfer = SecureFileTransfer(
                p2p_socket=p2p_sock,
                relay_socket=client.websocket,
                session_key=key,
                peer_addr=p2p_addr
            )
            
            
            print("\nConnection ready")
            transfer_action = input("Do you want to send or receive a file? (send/recv): ").strip().lower()

            if transfer_action == "send":
                filepath = input("enter path to file to send: ").strip()
                if os.path.exists(filepath):
                    await file_transfer.send_file(filepath)
                else:
                    logging.error("error: File does not exist.")
            elif transfer_action == "recv":
                download_dir = "./download"
                print(f"Waiting ro receive file into '{download_dir}'... ")
                await file_transfer.receive_file(download_dir)
            else:
                print("unkown action")
            
            try:
                # instead of waiting for nothing, wait for the socket to close
                await client.websocket.wait_closed()
                logging.info("Websocket connection closed, exiting")
            except asyncio.CancelledError:
                pass
            
        
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
