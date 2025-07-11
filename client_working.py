import socket  # For network communication (UDP sockets)
import json  # For encoding/decoding messages in JSON format
import threading # For running multiple functions in parallel (e.g., listening and sending)
import time
import uuid # To generate a unique ID for each client

class ChatClient:
    def __init__(self, discovery_port=7639):
        # The UDP port used for discovering the leader
        self.discovery_port = discovery_port

        # Create UDP Socket for Discovery and Messaging with SO_REUSEADDR enabled
        self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.discovery_socket.bind(('', self.discovery_port))
        
        # Create Client Socket on available port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.bind(('', 0)) # Bind to an available random port

        # Leader Server Data
        self.server_id = None  # Current leader ID
        self.server_address = None  # Current leader address (IP, port)
        self.connected = False  # Track connection status

        # Client ID and Port
        self.id = str(uuid.uuid4())  # Unique ID for the client
        self.port = self.client_socket.getsockname()[1]

    def discover_leader(self):
        """Listen on the discovery port for leader announcements."""
        print(f"Listening for leader-heartbeat messages on port {self.discovery_port}...")
        while True:
            try:
                # Receive UDP packet
                response, address = self.discovery_socket.recvfrom(1024)
                data = json.loads(response.decode())

                # Handle leader heartbeat message
                if data["type"] == "heartbeat":
                    server_id = data['id']
                    server_address = (address[0], data['port'])

                    # If a new leader is detected
                    if self.server_id != server_id:
                        print(f"New Leader discovered: {server_id} at {address[0]}:{data['port']}")
                        self.server_id = server_id
                        self.server_address = server_address
                        self.connect_server()
                        print("Successfully connected to leader server! \n")

                    # If reconnecting to the known leader after a lost connection
                    elif not self.connected:
                        # Reconnect to existing leader if connection was lost
                        print(f"Reconnecting to leader: {server_id}")
                        self.server_address = server_address
                        self.connect_server()
                        print("Successfully reconnected to leader server! \n")
                        
            except json.JSONDecodeError:
                print("Received malformed JSON in discovery")
            except Exception as e:
                print(f"Error in leader discovery: {e}")
                # Reset connection status on error
                self.connected = False # Mark as disconnected on error

    def connect_server(self):
        """Send join message to the leader server."""
        if self.server_address:
            try:
                join_message = {
                    "type": "join",
                    "id": self.id,
                    "port": self.port
                }
                # Send the join message to the leader server
                self.client_socket.sendto(json.dumps(join_message).encode(), self.server_address)
                self.connected = True
            except Exception as e:
                print(f"Failed to connect to server: {e}")
                self.connected = False

    def send_message(self, message):
        """Send a message to the leader server."""
        if self.server_address and self.connected:
            try:
                msg = json.dumps({"type": "message", "id": self.id, "text": message})
                # Send message to leader using discovery socket
                self.discovery_socket.sendto(msg.encode(), self.server_address)
            except Exception as e:
                print(f"Failed to send message: {e}")
                self.connected = False
                print("Connection lost. Waiting for new leader...")
        else:
            print("Not connected to any server. Waiting for leader...")

    def listen_for_messages(self):
        """Listen for messages from the leader server."""
        while True:
            try:
                response, address = self.client_socket.recvfrom(1024)
                data = json.loads(response.decode())
                if data["type"] == "message":
                    print(f"Message from server: {data['text']}")
            except json.JSONDecodeError:
                print("Received malformed JSON message")
            except Exception as e:
                print(f"Error receiving message: {e}")
                self.connected = False
                
# Entry point of the program
if __name__ == "__main__":
    # Create a new client instance
    client = ChatClient(discovery_port=7639)

    # Start a thread to discover the leader server
    threading.Thread(target=client.discover_leader, daemon=True).start()

    # Start listening for messages from the server
    threading.Thread(target=client.listen_for_messages, daemon=True).start()

    # Send messages to the server
    print("Client started. Waiting for leader discovery...")
    while True:
        try:
            if client.server_id is not None:
                # Prompt user to enter a message
                message = input("\nEnter message: ")
                if message.strip():  # Only send non-empty messages
                    client.send_message(message)
            else:
                # Wait until a leader is discovered
                print("Waiting for leader discovery...")
                time.sleep(2)
        except KeyboardInterrupt:
            print("Client shutting down...")
            break
