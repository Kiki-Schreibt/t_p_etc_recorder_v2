import socket
import threading


class TCPServer:
    def __init__(self, host='localhost', port=50009):
        self.host = host
        self.port = port
        self.server_socket = None

    def start_server(self):
        try:
            # Create a TCP/IP socket
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Allow address reuse
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Bind the socket to the host and port
            self.server_socket.bind((self.host, self.port))
            # Listen for incoming connections
            self.server_socket.listen()
            print(f"Server listening on {self.host}:{self.port}")

            while True:
                # Wait for a client connection
                client_socket, client_address = self.server_socket.accept()
                print(f"Connection from {client_address}")
                # Handle the client in a new thread
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, client_address))
                client_thread.daemon = True  # Allows the thread to exit when main program exits
                client_thread.start()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
                print("Server socket closed.")

    def handle_client(self, client_socket, client_address):
        try:
            with client_socket:
                while True:
                    # Receive data from the client
                    data = client_socket.recv(4096)
                    if not data:
                        break  # No more data from client
                    command = data.decode('ascii').strip()
                    print(f"Received command from {client_address}: {command}")
                    # Process the command and send a response
                    response = self.process_command(command)
                    client_socket.sendall(response.encode('ascii'))
        except Exception as e:
            print(f"An error occurred while handling client {client_address}: {e}")
        finally:
            print(f"Client {client_address} connection closed.")

    def process_command(self, command):
        # Process the received command and return a response
        if command == '*IDN?':
            return 'MyServer,ModelXYZ,Serial12345,Version1.0\n'
        else:
            return 'Unknown Command\n'


if __name__ == "__main__":
    server = TCPServer()
    server.start_server()
