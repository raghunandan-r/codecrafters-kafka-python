import socket  
import struct

def parse_header(header):

    api_key = struct.unpack('>H', header[4:6])[0]
    api_version = struct.unpack('>H', header[6:8])[0]
    correlation_id = struct.unpack('>I', header[8:12])[0]
    client_id = None

    return api_key, api_version, correlation_id, client_id

def create_response(api_version, api_key, correlation_id):

    header = struct.pack('>I', correlation_id)

    if api_version not in [0, 1, 2, 3, 4]:
        error_code = struct.pack('>h',35)
    else:
        error_code = struct.pack('>h',0)

    api_key_count = struct.pack('>B', 1)
    throttle_time_ms = struct.pack('>I', 0)
    api_key_entry = struct.pack('>hhh', api_key, 0, 4)
    body = error_code + api_key_count + api_key_entry + throttle_time_ms
    total_length = 4 + len(header) + len(body)  # 4 bytes for length field itself
    length_prefix = struct.pack('>I', total_length)

    return length_prefix + header + body

def main():
    
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, client_address = server.accept() # wait for client
        print(f"Connection from {client_address}")

        try:
            request_header = client_socket.recv(12)
            print(f"request header: {request_header}")
            if len(request_header) < 12:
                print("Incomplete header received")
                continue
            api_key, api_version, correlation_id, client_id = parse_header(request_header)
            print(f"Received header: API Key: {api_key}, Version: {api_version}, Correlation ID: {correlation_id}, Client ID: {client_id}")

            
            request_body = client_socket.recv(1024)
            print(f"Request body: {request_body}")

            full_response = create_response(api_version, api_key, correlation_id)
            client_socket.sendall(full_response)

        except Exception as e:
            print(f"Error handling client: {e}")
        
        finally:
            client_socket.close()

if __name__ == "__main__":
    main()
