import socket
import threading
import json
import random
import time

# Define the network configuration for the Paxos nodes
NODES = [("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)]
NODE_ID = None
SERVER_PORT = None


# Class representing a Paxos node
class Node:
    def __init__(self, node_id):
        self.id = node_id
        self.highest_proposal_number = 0
        self.accepted_proposal_number = 0
        self.accepted_value = None
        self.file = File(f"CISC5597_Node_{node_id}")
        self.lock = threading.Lock()

    # Handle the prepare phase of the Paxos algorithm
    def prepare(self, proposal_number):
        with self.lock:
            if proposal_number > self.highest_proposal_number:
                self.highest_proposal_number = proposal_number
            return (self.accepted_proposal_number, self.accepted_value)

    # Handle the accept phase of the Paxos algorithm
    def accept(self, proposal_number, value):
        with self.lock:
            if proposal_number >= self.highest_proposal_number:
                self.highest_proposal_number = proposal_number
                self.accepted_proposal_number = proposal_number
                self.accepted_value = value
                self.file.write(
                    f"{value} (having accepted proposal num {proposal_number})"
                )
                return True
            return False

    # Return the contents of the node's file
    def print_file(self):
        return f"Node {self.id}: {self.file.read()}"


# Class representing a file for persistent storage
class File:
    def __init__(self, filename):
        self.filename = filename
        self.content = ""

    def write(self, content):
        self.content = content

    def read(self):
        return self.content


# Class implementing the Paxos consensus algorithm
class PaxosProtocol:
    def __init__(self, node):
        self.node = node
        self.proposal_number = 0

    # Generate a unique proposal number
    def generate_proposal_number(self):
        self.proposal_number += 1
        return self.proposal_number * 10 + self.node.id

    # Run the Paxos algorithm to achieve consensus
    def run(self, value):
        max_attempts = 5
        for _ in range(max_attempts):
            proposal_number = self.generate_proposal_number()
            print(
                f"Node {self.node.id} attempting phase one with proposal {proposal_number}"
            )

            if self.phase_one(proposal_number):
                print(
                    f"Node {self.node.id} attempting phase two with proposal {proposal_number} and value '{value}'"
                )
                if self.phase_two(proposal_number, value):
                    print(
                        f"Node {self.node.id} successfully committed value '{value}' with proposal {proposal_number}"
                    )
                    self.node.file.write(
                        f"{value} (having accepted proposal num {proposal_number})"
                    )
                    self.broadcast_update(proposal_number, value)
                    return True
                else:
                    print(
                        f"Node {self.node.id} failed phase two with proposal {proposal_number}"
                    )
            else:
                print(
                    f"Node {self.node.id} did not receive majority response in phase one for proposal {proposal_number}"
                )
        return False

    # Implement phase one of the Paxos algorithm (prepare)
    def phase_one(self, proposal_number):
        promises = 0
        for ip, port in NODES:
            if port == SERVER_PORT:
                continue  # Skip self
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, port))
                    message = json.dumps(
                        {"type": "prepare", "proposal_number": proposal_number}
                    )
                    s.sendall(message.encode())
                    response = json.loads(s.recv(1024).decode())
                    if response.get("status") == "promise":
                        promises += 1
            except ConnectionRefusedError:
                print(f"Node {port} not reachable")
        return promises > len(NODES) // 2

    # Implement phase two of the Paxos algorithm (accept)
    def phase_two(self, proposal_number, value):
        accepts = 0
        for ip, port in NODES:
            if port == SERVER_PORT:
                continue  # Skip self
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, port))
                    message = json.dumps(
                        {
                            "type": "accept",
                            "proposal_number": proposal_number,
                            "value": value,
                        }
                    )
                    s.sendall(message.encode())
                    response = json.loads(s.recv(1024).decode())
                    if response.get("status") == "accepted":
                        accepts += 1
            except ConnectionRefusedError:
                print(f"Node {port} not reachable")
        return accepts > len(NODES) // 2

    # Broadcast the accepted value to all nodes
    def broadcast_update(self, proposal_number, value):
        for ip, port in NODES:
            if port == SERVER_PORT:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((ip, port))
                    message = json.dumps(
                        {
                            "type": "update",
                            "proposal_number": proposal_number,
                            "value": value,
                        }
                    )
                    s.sendall(message.encode())
            except ConnectionRefusedError:
                print(f"Node {port} not reachable")


# Function to handle client connections
def handle_client(client_socket, node):
    while True:
        try:
            data = client_socket.recv(1024).decode()
            if not data:
                break
            message = json.loads(data)

            if message["type"] == "prepare":
                # Handle prepare request
                accepted_proposal_number, accepted_value = node.prepare(
                    message["proposal_number"]
                )
                response = {
                    "status": "promise",
                    "accepted_proposal_number": accepted_proposal_number,
                    "accepted_value": accepted_value,
                }
                client_socket.send(json.dumps(response).encode())

            elif message["type"] == "accept":
                # Handle accept request
                if node.accept(message["proposal_number"], message["value"]):
                    response = {"status": "accepted"}
                else:
                    response = {"status": "reject"}
                client_socket.send(json.dumps(response).encode())

            elif message["type"] == "update":
                # Handle update request
                node.accept(message["proposal_number"], message["value"])

            elif message["type"] == "SubmitValue":
                # Handle value submission request
                paxos = PaxosProtocol(node)
                success = paxos.run(message["value"])
                if success:
                    client_socket.send("Value successfully committed.".encode())
                else:
                    client_socket.send("Failed to commit value.".encode())

            elif message["type"] == "print":
                # Handle print request
                file_content = node.print_file()
                client_socket.send(file_content.encode())

        except Exception as e:
            print(f"Error handling client: {e}")
            break

    client_socket.close()


# Function to print the current state of all nodes
def print_all_nodes():
    print("\nCurrent state of all nodes:")
    for ip, port in NODES:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                message = json.dumps({"type": "print"})
                s.sendall(message.encode())
                response = s.recv(1024).decode()
                print(response)
        except ConnectionRefusedError:
            print(f"Node on port {port} not reachable")


# Function to start the Paxos server
def start_server():
    global SERVER_PORT, NODE_ID
    NODE_ID = int(input("Enter the ID for this node (0, 1, 2): "))
    SERVER_PORT = NODES[NODE_ID][1]

    node = Node(NODE_ID)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", SERVER_PORT))
    server_socket.listen(5)
    print(f"Node {NODE_ID} connected!")

    while True:
        client_socket, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_socket, node)).start()


# Entry point of the script
if __name__ == "__main__":
    start_server()
