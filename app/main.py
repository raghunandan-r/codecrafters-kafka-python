import socket  # noqa: F401
import struct

def parse_header(header):

    api_key = struct.unpack('>H', header[4:6])[0]
    api_version = struct.unpack('>H', header[6:8])[0]
    correlation_id = struct.unpack('>I', header[8:12])[0]
    client_id = None

    return api_key, api_version, correlation_id, client_id

def create_response(api_version, correlation_id, body):

    if api_version not in (0,1,2,3,4):
        body = struct.pack('>h',35)
    else:
        body = body.encode('utf-8') #check this for proper api version later

    msg_length = 4 + len(body)
    header = struct.pack('>II', msg_length, correlation_id)
    return header + body

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
            print(f"Received request: API Key: {api_key}, Version: {api_version}, Correlation ID: {correlation_id}, Client ID: {client_id}")

            
            # request_body = client_socket.recv(1024).decode('utf-8')
            # print(f"Request Received: {request_body}")

            full_response = create_response(api_version, correlation_id, "response body dummy")
            client_socket.sendall(full_response)

        except Exception as e:
            print(f"Error handling client: {e}")
        
        finally:
            client_socket.close()

if __name__ == "__main__":
    main()
