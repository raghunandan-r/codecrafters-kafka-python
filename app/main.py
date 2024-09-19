import socket  # noqa: F401
import struct

def create_response(body):

    body_bytes = body.encode('utf-8')

    msg_length = len(body_bytes)
    header = struct.pack('>II', msg_length, 7)
    return header + body_bytes

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("LFG!!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, client_address = server.accept() # wait for client
        print("Connection from {client_address}")

        try:

            request = client_socket.recv(1024).decode('utf-8')
            print("Request Received: {request}")

            response = 7

            full_response = create_response(response)
            client_socket.sendall(full_response)

        except Exception as e:
            print(f"Error handling client: {e}")
        
        finally:
            client_socket.close()

if __name__ == "__main__":
    main()
