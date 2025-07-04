import socket
import threading
import json
import uuid
import time

class ChatServer:
    def __init__(self):
        # Define Server Port and Discovery Port
        self.port = 7777 # Adjust manually for every Server
        self.discovery_port = 7639

        # Create Unique ID and Leader State
        self.id = str(uuid.uuid4())
        self.is_leader = False
        self.last_heartbeat = time.time()
        self.election_in_progress = False  # Track if election is ongoing
        self.last_discovery_time = {}  # Track when we last heard from each server

        # Server Socket Initialization
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Create UDP Socket
        self.server_socket.bind(('', self.port))  # Bind socket to port
        self.ip = socket.gethostbyname(socket.gethostname())

        # Discovery Socket Initialization
        self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Create UDP Socket
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Enable address reuse
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Enable broadcast
        self.discovery_socket.bind(('', self.discovery_port))  # Bind to discovery port

        # Client and Server List
        self.known_clients = {}  # Store connected clients
        self.known_servers = {}  # Store discovered servers
      
    def start_server(self):
        print("------------------------------------------")
        print(f"Server IP: {self.ip} Server ID: {self.id}")
        print("------------------------------------------")
        print(f"Server running on port {self.port} and waiting for connections ...")
        print(f"Listening for discovery messages on port {self.discovery_port} ...")

        threading.Thread(target=self.listen_on_server_port, daemon=True).start()
        threading.Thread(target=self.listen_on_discovery_port, daemon=True).start()
        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()
        threading.Thread(target=self.broadcast_discovery, daemon=True).start()
        threading.Thread(target=self.monitor_server_health, daemon=True).start()  # Monitor server health

        # Delay election to allow discovery
        time.sleep(10)
        print("Checking for existing leader...")
        
        # Check if there's already a leader in the network
        if not self.has_active_leader():
            print("No active leader found. Initiating leader election at startup...")
            self.initiate_leader_election()
        else:
            print("Active leader found in network. Joining as follower.")
          
    def broadcast_discovery(self):
        while True:
            discover_message = {
                "type": "discover",
                "id": self.id,
                "port": self.port,
                "isLeader": self.is_leader
            }
            self.discovery_socket.sendto(json.dumps(discover_message).encode(),
                                ('<broadcast>', self.discovery_port))
            print(f"Broadcasting discovery message from {self.id}")
            time.sleep(10)  # Send discovery messages every 10 seconds

    def broadcast_heartbeat(self):
        """Leader periodically sends heartbeat messages to all servers."""
        while self.is_leader:
            heartbeat_message = {
                "type": "heartbeat",
                "id": self.id,
                "port": self.port
            }
            self.discovery_socket.sendto(json.dumps(heartbeat_message).encode(), ('<broadcast>', self.discovery_port))
            print(f"Heartbeat sent by the leader {self.port}.")
            time.sleep(10)  # Heartbeat interval

    def monitor_server_health(self):
        """Monitor the health of all known servers and trigger re-election if needed."""
        while True:
            time.sleep(15)  # Check every 15 seconds
            current_time = time.time()
            failed_servers = []
            
            for server_id, server_info in self.known_servers.items():
                if server_id == self.id:
                    continue  # Skip self
                    
                last_seen = self.last_discovery_time.get(server_id, 0)
                if current_time - last_seen > 30:  # 30 second timeout
                    print(f"Server {server_id} appears to be down (last seen {current_time - last_seen:.1f}s ago)")
                    failed_servers.append(server_id)
            
            # Remove failed servers
            leader_failed = False
            for server_id in failed_servers:
                if server_id in self.known_servers:
                    was_leader = self.known_servers[server_id].get("isLeader", False)
                    if was_leader:
                        leader_failed = True
                    self.known_servers.pop(server_id, None)
                    self.last_discovery_time.pop(server_id, None)
                    print(f"Removed failed server {server_id} from known servers")
            
            # If leader failed or we have no leader, start election
            if leader_failed or not self.has_active_leader():
                if not self.election_in_progress:
                    print("Leader failure detected or no active leader. Initiating re-election.")
                    self.initiate_leader_election()
    
    def monitor_heartbeat(self):
        """Monitor leader's heartbeat and trigger election on failure."""
        while True:
            time.sleep(10)  # Check every 10 seconds
            if not self.is_leader and not self.election_in_progress and (time.time() - self.last_heartbeat > 20):  # 20-second timeout
                print("Leader unresponsive. Initiating leader election.")
                self.initiate_leader_election()

    def monitor_server_health(self):
        """Monitor the health of all known servers and trigger re-election if needed."""
        while True:
            time.sleep(15)  # Check every 15 seconds
            current_time = time.time()
            failed_servers = []
            
            for server_id, server_info in self.known_servers.items():
                if server_id == self.id:
                    continue  # Skip self
                    
                last_seen = self.last_discovery_time.get(server_id, 0)
                if current_time - last_seen > 30:  # 30 second timeout
                    print(f"Server {server_id} appears to be down (last seen {current_time - last_seen:.1f}s ago)")
                    failed_servers.append(server_id)
            
            # Remove failed servers
            leader_failed = False
            for server_id in failed_servers:
                if server_id in self.known_servers:
                    was_leader = self.known_servers[server_id].get("isLeader", False)
                    if was_leader:
                        leader_failed = True
                    self.known_servers.pop(server_id, None)
                    self.last_discovery_time.pop(server_id, None)
                    print(f"Removed failed server {server_id} from known servers")
            
            # If leader failed or we have no leader, start election
            if leader_failed or not self.has_active_leader():
                if not self.election_in_progress:
                    print("Leader failure detected or no active leader. Initiating re-election.")
                    self.initiate_leader_election()
    
    def has_active_leader(self):
        """Check if there's currently an active leader."""
        for server_info in self.known_servers.values():
            if server_info.get("isLeader", False):
                return True
        return self.is_leader

    def listen_on_discovery_port(self):
        """Listen for discovery messages on the discovery port."""
        while True:
            try:
                message, address = self.discovery_socket.recvfrom(1024)
                data = json.loads(message.decode())
                server_id = data['id']
                server_ip = address[0]
                
                if data["type"] == "discover":
                    if server_id not in self.known_servers:
                        server_ip = server_ip
                        server_port = data ['port']
                        self.known_servers[server_id] = {
                            "id": server_id,
                            "ip": server_ip,
                            "port": server_port,
                            "isLeader": data['isLeader']
                        }
                        print(f"Discovered new server: {server_ip}:{server_port}")
                    
                    # Update last discovery time for this server
                    self.last_discovery_time[server_id] = time.time()
                elif data["type"] == "leader":
                    leader_id = server_id
                    server_port = data ['port']
                    print(f"Server {leader_id} has been elected as leader.")

                    #update status
                    self.is_leader = (leader_id == self.id)
                    self.election_in_progress = False  # Election completed
                    
                    # Cancel any pending election timeout
                    if hasattr(self, 'election_timer'):
                        self.election_timer.cancel()
                    
                    # Update the respective server entry with `isLeader=True`
                    # First, set all servers as not leader
                    for server in self.known_servers.values():
                        server["isLeader"] = False
                        
                    if leader_id in self.known_servers:
                        self.known_servers[leader_id]["isLeader"] = True
                    else:
                        print(f"Leader {leader_id} not found in known servers. Adding it.")
                        # Add leader to the known_servers if not already present
                        self.known_servers[leader_id] = {
                            "id": leader_id,
                            "ip": address[0],
                            "port": server_port,
                            "isLeader": True
                        }

                elif data["type"] == "heartbeat":
                        leader_id = server_id
                        if leader_id == self.id: # Ignore heartbeats from self
                            continue  
                        print(f"Heartbeat received from leader {server_ip}:{data['port']}.")
                        self.last_heartbeat = time.time()  # Update the last heartbeat timestamp
                        # Also update discovery time since heartbeat means server is alive
                        self.last_discovery_time[leader_id] = time.time()
            except Exception as e:
                print(f"Error in discovery port listener: {e}")

    def remove_failed_server(self, server_id):
        """Remove a failed server from known servers."""
        if server_id in self.known_servers:
            removed_server = self.known_servers.pop(server_id)
            print(f"Removed failed server {server_id} from known servers.")
            
            # If the failed server was the leader, initiate new election
            if removed_server.get("isLeader", False):
                print("Failed server was the leader. Initiating new election.")
                self.initiate_leader_election()

    def forward_token(self, token_id):
        """Send the election token to the next server in the logical ring."""
        if len(self.known_servers) <= 1:
            print("Only one server in the network. Declaring self as leader.")
            self.is_leader = True
            self.election_in_progress = False
            self.broadcast_leader()
            threading.Thread(target=self.broadcast_heartbeat, daemon=True).start()
            return

        sorted_servers = sorted(self.known_servers.values(), key=lambda x: x["id"])
        my_index = next((i for i, server in enumerate(sorted_servers) if server["id"] == self.id), None)

        if my_index is None:
            print("Error: This server is not in the known_servers list.")
            return

        # Try to forward to next servers in the ring until one succeeds
        attempts = 0
        max_attempts = len(sorted_servers)
        servers_to_remove = []
        
        while attempts < max_attempts:
            next_index = (my_index + 1 + attempts) % len(sorted_servers)
            next_server = sorted_servers[next_index]
            
            # Skip self
            if next_server["id"] == self.id:
                attempts += 1
                continue
                
            next_address = (next_server["ip"], next_server["port"])

            try:
                print(f"Forwarding token {token_id} to {next_server['id']} at {next_address}.")
                self.server_socket.settimeout(5)  # Set timeout for sending
                self.server_socket.sendto(json.dumps({"type": "election", "token": token_id}).encode(), next_address)
                self.server_socket.settimeout(None)  # Reset timeout
                return  # Successfully sent
            
            except Exception as e:
                print(f"Failed to send token {token_id} to {next_server['id']} at {next_address}: {e}")
                print(f"Marking server {next_server['id']} for removal due to failure.")
                servers_to_remove.append(next_server["id"])
                attempts += 1

        # Remove failed servers
        for server_id in servers_to_remove:
            self.known_servers.pop(server_id, None)
            self.last_discovery_time.pop(server_id, None)

        # If we reach here, all servers failed - declare self as leader
        print("All other servers unreachable. Declaring self as leader.")
        self.is_leader = True
        self.election_in_progress = False
        self.broadcast_leader()
        threading.Thread(target=self.broadcast_heartbeat, daemon=True).start()

    def broadcast_leader(self):
        """Broadcast leader information to all known servers."""
        leader_message = {
            "type": "leader",
            "id": self.id,
            "port": self.port
        }
        print(f"Leader {self.id} will be broadcasted to all known servers.")
        self.discovery_socket.sendto(json.dumps(leader_message).encode(), ('<broadcast>', self.discovery_port))

    def listen_on_server_port(self):
        while True:
            try:
                message, address = self.server_socket.recvfrom(1024)
                data = json.loads(message.decode())
    
                if data["type"] == "join":
                    # New client connected
                    client_id = data["id"]
                    client_ip = address[0]
                    client_port= data["port"]

                    if client_id not in self.known_clients:
                        self.known_clients[client_id] = {
                            "id": client_id,
                            "ip": client_ip,
                            "port": client_port,
                        }

                    print(f"Client {data['id']} connected from {address}")

                elif data["type"] == "message":
                    # Forward message
                    print(f"Received message from client {data['id']}: {data['text']}")
                    self.broadcast_message(data, data['id'])
                  
                elif data["type"] == "election":
                    token_id = data["token"]
                    print(f"Received election token {token_id} from {address}.")
                    
                    # Reset election timeout since we received a valid election message
                    if hasattr(self, 'election_timer'):
                        self.election_timer.cancel()
                    self.election_timer = threading.Timer(30.0, self.election_timeout)
                    self.election_timer.start()
                    
                    if token_id > self.id:
                        # Forward the token unchanged
                        self.forward_token(token_id)
                    elif token_id < self.id:
                        # Replace with this server's ID and forward
                        self.forward_token(self.id)
                    elif token_id == self.id:
                        # This server is the leader
                        print(f"I have won the election!")
                        self.is_leader = True
                        self.election_in_progress = False
                        if hasattr(self, 'election_timer'):
                            self.election_timer.cancel()
                        self.broadcast_leader()
                        threading.Thread(target=self.broadcast_heartbeat, daemon=True).start()
                
            except json.JSONDecodeError:
                print("Received malformed JSON message")
            except Exception as e:
                print(f"Error in server port listener: {e}")

    def broadcast_message(self, message, sender):
        # Send message to all other clients
        failed_clients = []
        for client_id, client_info in self.known_clients.items():
            if client_id != sender:  # Do not send the message back to the sender
                address = (client_info["ip"], client_info["port"])
                try:
                    self.server_socket.sendto(json.dumps(message).encode(), address)
                except Exception as e:
                    print(f"Failed to send message to client {client_id}: {e}")
                    failed_clients.append(client_id)
        
        # Remove failed clients
        for client_id in failed_clients:
            self.known_clients.pop(client_id, None)
            print(f"Removed failed client {client_id}")
                    
    def initiate_leader_election(self):
        if self.election_in_progress:
            print("Election already in progress, skipping.")
            return
            
        print(f"Server {self.id} initiating leader election.")
        self.election_in_progress = True
        self.is_leader = False  # Reset leader status
        
        # Clear all leader flags before starting election
        for server_info in self.known_servers.values():
            server_info["isLeader"] = False
        
        # Start the election with this server's ID
        self.forward_token(self.id)
        
        # Set a timeout for the election process
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()
        self.election_timer = threading.Timer(30.0, self.election_timeout)
        self.election_timer.start()
    
    def election_timeout(self):
        """Handle election timeout - if election is still in progress after timeout, declare self as leader."""
        if self.election_in_progress:
            print("Election timeout reached. Declaring self as leader.")
            self.is_leader = True
            self.election_in_progress = False
            self.broadcast_leader()
            threading.Thread(target=self.broadcast_heartbeat, daemon=True).start()


if __name__ == "__main__":
    server = ChatServer()
    server.start_server()
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Server shutting down...")
