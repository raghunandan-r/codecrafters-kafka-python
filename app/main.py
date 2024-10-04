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
    session_id : int = None
    topic_id : uuid.UUID = None

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        api_key, api_version, correlation_id = struct.unpack('>HHI', data[4:12])
        
        error_code = (
            ErrorCode.NONE
            if api_version in [0, 1, 2, 3, 4]
            else ErrorCode.UNSUPPORTED_VERSION
        )
        
        session_id, topic_id = None, None
        #if api_key == 1 and api_version == 16:
        #    session_id, = struct.unpack('>I', data[29:33]) 
        #    topic_id = uuid.UUID(bytes=data[36:52])
        return KafkaRequest(api_key, api_version, correlation_id, error_code, session_id, topic_id)
        

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
    
    response_header = struct.pack('>I', request.correlation_id)  # 4 bytes

    # Main Response Body
    response_body = struct.pack('>IhI', 
        0,    # throttle_time_ms (INT32) - 4 bytes
        0,    # error_code (INT16) - 2 bytes
        0     # session_id (INT32) - 4 bytes
    )  # Total: 10 bytes

    # Topic Response
    partitions_response = struct.pack('>Ihqqqii', 
        0,   # partition_index (INT32)
        100,  # error_code.UNKNOWN_TOPIC (INT16)
        0,   # high_watermark (INT64)
        0,   # last_stable_offset (INT64)
        0,   # log_start_offset (INT64)
        0,   # aborted_transactions array length (INT32)
        0   # preferred_read_replica (INT32)
    )
    topic_response = (
        (1).to_bytes(16, byteorder='big') +  # topic_id (UUID)
        struct.pack('>i', 1) +                  # Number of partitions (1 in this case)
        partitions_response                     # Partitions response
    )

    responses_count = struct.pack('>i', 1)  # Number of topic responses
    tag_buffer = b'\x00' # Final TAG_BUFFER

    full_response = (
        response_header + 
        response_body + 
        responses_count +
        topic_response + 
        tag_buffer
    )

    total_length = len(full_response)
    length_prefix = struct.pack('>I', total_length)

    return length_prefix + full_response


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