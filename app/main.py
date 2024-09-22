import socket
import struct
from enum import Enum, unique
from dataclasses import dataclass

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        if len(data) < 12:
            raise ValueError("Incomplete Kafka request header received.")
        return KafkaRequest(
            api_key=int.from_bytes(data[4:6], byteorder='big'),
            api_version=int.from_bytes(data[6:8], byteorder='big'),
            correlation_id=int.from_bytes(data[8:12], byteorder='big'),
        )

def make_response(request: KafkaRequest):
    # Determine the error code based on the API version
    if request.api_version in [0, 1, 2, 3, 4]:
        error_code = ErrorCode.NONE.value.to_bytes(2, byteorder='big')
    else:
        error_code = ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(2, byteorder='big')

    # ThrottleTimeMs set to 0 (no throttling)
    throttle_time_ms = (0).to_bytes(4, byteorder='big')

    # Number of ApiKeys (1 in this case)
    api_keys_count = (1).to_bytes(4, byteorder='big')  # Corrected to 4 bytes

    # ApiKey entry: ApiKey=18 (ApiVersions), MinVersion=0, MaxVersion=4
    api_key_entry = struct.pack('>hhh', 18, 0, 4)  # Each 'h' is 2 bytes

    # Construct the response body in the correct order
    response_body = throttle_time_ms + error_code + api_keys_count + api_key_entry  # Correct order

    # Calculate message length: Correlation ID (4 bytes) + Response Body (16 bytes)
    message_length = 4 + len(response_body)  # 4 + 16 = 20 bytes

    # Frame Length: Total bytes following the Frame Length field
    frame_length = message_length

    # Pack the Frame Length and Correlation ID
    frame_length_bytes = frame_length.to_bytes(4, byteorder='big')
    correlation_id_bytes = request.correlation_id.to_bytes(4, byteorder='big')

    # Complete response
    full_response = frame_length_bytes + correlation_id_bytes + response_body

    return full_response

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on localhost:9092")

    while True:
        client_socket, client_address = server.accept()  # Wait for client
        print(f"Connection from {client_address}")

        try:
            # Receive the first 12 bytes: Frame Length (4) + API Key (2) + API Version (2) + Correlation ID (4)
            request = KafkaRequest.from_client(client_socket)
            print(f"Received header: API Key: {request.api_key}, Version: {request.api_version}, "
                  f"Correlation ID: {request.correlation_id}")

            # Receive the rest of the request body if necessary
            # For this test stage, you can ignore or process it as needed
            # request_body = client_socket.recv(1024)  # Not necessary for current test

            # Create the response based on the request
            full_response = make_response(request)

            # Send the response
            client_socket.sendall(full_response)
            print(f"Sent response: {full_response.hex()}")

        except Exception as e:
            print(f"Error handling client: {e}")

        finally:
            client_socket.close()

if __name__ == "__main__":
    main()