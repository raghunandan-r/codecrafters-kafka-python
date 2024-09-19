import socket  # noqa: F401
import struct

def parse_header(header):

    correlation_id = struct.unpack('>I', header[4:8])
    api_key, api_version, client_id = None, None, None

    return api_key, api_version, correlation_id, client_id

def create_response(correlation_id, body):

    body_bytes = body.encode('utf-8')

    msg_length = 8 + len(body_bytes)
    header = struct.pack('>II', msg_length, correlation_id)
    return header + body_bytes

def main():
    
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, client_address = server.accept() # wait for client
        print(f"Connection from {client_address}")

        try:
            request_header = client_socket.recv(16)

            if len(request_header) < 8:
                print("Incomplete header received")
                continue
            api_key, api_version, correlation_id, client_id = parse_header(request_header)
            print(f"Received request: API Key: {api_key}, Version: {api_version}, Correlation ID: {correlation_id}, Client ID: {client_id}")

            
            request_body = client_socket.recv(1024).decode('utf-8')
            print(f"Request Received: {request_body}")

            response = "response body dummy"

            full_response = create_response(correlation_id, response)
            client_socket.sendall(full_response)

        except Exception as e:
            print(f"Error handling client: {e}")
        
        finally:
            client_socket.close()

if __name__ == "__main__":
    main()
