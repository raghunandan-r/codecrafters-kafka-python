import uuid
import socket
import threading
import struct
from dataclasses import dataclass
from enum import Enum, unique

FETCH = 1
VERSIONS = 18

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int
    error_code : ErrorCode

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        api_key, api_version, correlation_id = struct.unpack('>HHI', data[4:12])
        
        error_code = (
            ErrorCode.NONE
            if api_version in [0, 1, 2, 3, 4]
            else ErrorCode.UNSUPPORTED_VERSION
        )

        print(f"Received request: API Key: {api_key}, Version: {api_version}, Correlation ID: {correlation_id}")

        if api_key == 1 and api_version == 16:
            try:
                session_id = struct.unpack('>II', data[28:32])
                if len(data) >= 52:
                    topic_id = uuid.UUID(bytes=data[36:52])
                    print(f"Topic ID: {topic_id}")
                else:
                    print("Data too short to include Topic ID")

            except struct.error as e:
                print(f"Error parsing Fetch request: {e}")
            return KafkaRequest(api_key, api_version, correlation_id, error_code, session_id, topic_id)
        
        else:
            return KafkaRequest(api_key, api_version, correlation_id, error_code)


def make_response_apiversion(request: KafkaRequest):
    response_header = struct.pack('>I', request.correlation_id)
    
    response_body = struct.pack(
        '>hBHHHBHHHBIB',
        request.error_code.value,
        3,  # Number of API keys
        VERSIONS,
        0,  # Min version for VERSIONS
        4,  # Max version for VERSIONS
        0,  # Tag buffer for VERSIONS
        FETCH,
        0,  # Min version for FETCH
        16, # Max version for FETCH
        0,  # Tag buffer for FETCH
        0,  # throttle_time_ms
        0   # Final tag buffer
    )

    response_length = struct.pack('>I', len(response_header) + len(response_body))
    return response_length + response_header + response_body

def make_response_fetch(request: KafkaRequest):
    response_header = struct.pack('>I', request.correlation_id)
    responses = []

    response_body = struct.pack('>IhIBBB', 
        0,  # throttle_time_ms,
        0,  # error_code,
        0,  # session_id,
        0,  # tag buffer
        len(responses),
        0   # tag buffer
    )

    response_len = len(response_header) + len(response_body)
    return response_len.to_bytes(4) + response_header + response_body



def handle_client(client: socket.socket, addr):
    print(f"New request from {addr}")

    try:
        while True:
            request = KafkaRequest.from_client(client)
            if request is None:
                break
            print(f"Received request from {addr}: {request}")
            if request.api_key == FETCH:
                response = make_response_fetch(request)
            else:
                response = make_response_apiversion(request)
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