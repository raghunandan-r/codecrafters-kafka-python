import socket
import threading
import struct
from dataclasses import dataclass
from enum import Enum, unique

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int
    fetch_version: int
    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        api_key, api_version, correlation_id = struct.unpack('>HHI', data[4:12])
        fetch_key = 1
        return KafkaRequest(api_key, api_version, correlation_id, fetch_key)


def make_response(request: KafkaRequest):
    response_header = struct.pack('>I', request.correlation_id)
    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )
    min_api_version, max_api_version = 0, 4
    min_fetch_version, max_fetch_version = 0, 16
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    response_body = struct.pack('>HBHHH', 
        error_code.value,
        3,  # int(2).to_bytes(1)
        request.api_key,
        min_api_version,
        max_api_version
    ) + tag_buffer
    + struct.pack('>HHH', 
        request.fetch_key,
        min_fetch_version,
        max_fetch_version
    ) + tag_buffer + struct.pack('>I', throttle_time_ms) + tag_buffer

    response_length = len(response_header) + len(response_body)
    return int(response_length).to_bytes(4) + response_header + response_body

def handle_client(client: socket.socket, addr):
    print(f"New request from {addr}")

    try:
        while True:
            request = KafkaRequest.from_client(client)
            if request is None:
                break
            print(f"Received request from {addr}: {request}")
            response = make_response(request)
            client.sendall(response)
            print(f"sent response to {addr}: {len(response)} bytes")
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        client.close()
        print(f"Connection from {addr} closed.")

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        client, addr = server.accept()
        client_thread = threading.Thread(target= handle_client, args=(client, addr))
        client_thread.start()

if __name__ == "__main__":
    main()