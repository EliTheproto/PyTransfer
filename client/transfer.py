import os
import asyncio
import struct
import json 
import logging
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

class SecureFileTransfer:
    def __init__(self, p2p_socket, relay_socket, session_key, peer_addr):
        self.p2p_socket = p2p_socket
        self.relay_socket = relay_socket
        self.peer_addr = peer_addr

        # spake2 keys are typically 32 bytes, perfect for AES-256
        # if its a different length we may need to hash it to ensure its 32 bytes 
        self_key_bytes = session_key if len(session_key) == 32 else session_key.ljust(32, b'\0')[:32]
        self.aesgcm = AESGCM(self_key_bytes)



        # we need a chunk size small enough to fit within typical MTU after encryption overhead, but large enough for good performance.
        self.chunk_size = 1024
    
    async def _send_data(self, data: bytes):
        
    # for this we will jsut blast UDP and wait, 
    # in a produciton app we would want ACKs or websockets to gurantee
        if self.p2p_socket and self.peer_addr:
            try:
                self.p2p_socket.sendto(data, self.peer_addr)
            except Exception as e:
                logging.error(f"UDP send failed: {e} Falling back to websocket")

        else:
            # fallback to websocket if UDP hole punch completely failed
            await self.relay_socket.send(json.dumps({
                "action": "transfer_chunk",
                "payload": data.hex()
            }))

    def _encrypt(self, plaintext: bytes) -> bytes:
        # encrypts the data using AES-GCM and a random 12 byte nonce
        nonce = os.urandom(12)
        cipherText = self.aesgcm.encrypt(nonce, plaintext, None)
        return nonce + cipherText
    
    def _decrypt(self, encrypted_data: bytes) -> bytes:
        #decrypts data where the first 12 bytes are the nonce
        nonce, ciphertext = encrypted_data[:12], encrypted_data[12:]
        return self.aesgcm.decrypt(nonce, ciphertext, None)
    
    async def send_file(self, filepath):
        if not os.path.exists(filepath):
            logging.error("file does not exist.")
            return

        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)

        # send metadata
        metadata = json.dumps({
            "filename": filename,
            "filesize": filesize
        }).encode('utf-8')
        

        encrypted_metadata = self._encrypt(metadata)

        # pack sequence 0 to denote metadata
        packet = struct.pack("!I", 0) + encrypted_metadata
        await self._send_data(packet)
        logging.info(f"sent metadata for {filename} ({filesize} bytes)")

        # give peer a moment to parse metadata
        await asyncio.sleep(0.5)

        # 2. stream the file in chunks

        seq_num = 1 
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break # EOF

                encrypted_chunk = self._encrypt(chunk)

                # pack the seq num (4 byte unsigned int) and append payload
                packet = struct.pack("!I", seq_num) + encrypted_chunk

                await self._send_data(packet)
                seq_num += 1

                #slight throttle so we dont overwhelm UDP buffers
                if seq_num % 100 == 0:
                    await asyncio.sleep(0)

        # send EOF singal (seq_num = 0xFFFFFFFF)
        eof_packet = struct.pack("!I", 4294967295) + self._encrypt(b"EOF")
        await self._send_data(eof_packet)
        logging.info("file transmission complete.")

    async def receive_file(self, download_dir):
        os.makedirs(download_dir, exist_ok=True)

        loop = asyncio.get_running_loop()
        file_transfer_done = loop.create_future()
        out_file = None

        # abstract the processing logic so both UDP and Websockets can use it
        def process_packet(data):
            nonlocal out_file
            # unpack seq_num (first 4 bytes)
            seq_num = struct.unpack("!I", data[:4])[0]
            encrypted_payload = data[4:]

            try:
                plaintext = self._decrypt(encrypted_payload)
            except Exception as e:
                logging.error("Failed to decrypt chunk (wrong key or corrupted data?)")
                return
            
            # seq 0 is metadata
            if seq_num == 0:
                meta = json.loads(plaintext.decode('utf-8'))
                filepath = os.path.join(download_dir, meta['filename'])
                out_file = open(filepath, 'wb')
                logging.info(f"Receiving file: {meta['filename']} ({meta['filesize']} bytes)")

            # 0xFFFFFFFF is EOF
            elif seq_num == 4294967295:
                logging.info("EOF")
                if out_file:
                    out_file.close()
                if not file_transfer_done.done():
                    file_transfer_done.set_result(True)

            # standard file chunk

            else:
                if out_file:
                    out_file.write(plaintext)
                    if seq_num % 100 == 0:
                        logging.info(f"receivied chunk seq({seq_num})")

        # -- Listener 1L UDP (if p2p) ---

        def udp_receiver():
            try:
                data, addr = self.p2p_socket.recvfrom(2048)
                if data:
                    process_packet(data)
            except BlockingIOError:
                pass
            except Exception as e:
                logging.error(f"error reading UDP chunk: {e}")

        # --- LIstener 2: WebSocket (if p2p failed) ---

        async def websocket_receiver():
            try:
                while not file_transfer_done.done():
                    message = await self.relay_socket.recv()
                    data_json = json.loads(message)
                    if data_json.get("action") == "transfer_chunk":
                        #convert the hex string back to bytes
                        raw_data = bytes.fromhex(data_json["payload"])
                        process_packet(raw_data)
            except Exception as e:
                if not file_transfer_done.done():
                    logging.error(f"websocket receiver error: {e}")
        
        # start listening
        logging.info(f"listening for incoming file in '{download_dir}'...")

        if self.p2p_socket:
            logging.info("using P2P UDP...")
            loop.add_reader(self.p2p_socket.fileno(), udp_receiver)
        else:
            logging.info("unable to establish P2P, using Websocket fallback")
            ws_task = asyncio.create_task(websocket_receiver())

        # wait for EOF
        await file_transfer_done

        # cleanup

        if self.p2p_socket:
            loop.remove_reader(self.p2p_socket.fileno())
        else:
            ws_task.cancel()