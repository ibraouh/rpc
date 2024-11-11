import socket
import json
import time
import random

# Define the network configuration for the Paxos nodes
NODES = [("127.0.0.1", 8001), ("127.0.0.1", 8002), ("127.0.0.1", 8003)]


# Function to submit a value to a specific Paxos node
def submit_value(node_id, value):
    node_ip, node_port = NODES[node_id]
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            # Prepare and send a JSON message to submit a value
            message = json.dumps({"type": "SubmitValue", "value": value})
            s.sendall(message.encode())
            # Receive and print the response from the node
            response = s.recv(1024).decode()
            print(f"Node {node_id}: {response}")
    except Exception as e:
        print(f"Could not connect to Node {node_id}: {e}")


# Function to request and print the contents of a node's file
def print_node_file(node_id):
    node_ip, node_port = NODES[node_id]
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_ip, node_port))
            # Prepare and send a JSON message to request file contents
            message = json.dumps({"type": "print"})
            s.sendall(message.encode())
            # Receive and print the response (file contents) from the node
            response = s.recv(1024).decode()
            print(response)
    except Exception as e:
        print(f"[ERROR] Could not connect to Node {node_id}: {e}")


# Function to simulate a scenario with a single proposer
def simulate_single_proposer():
    # Choose a random node to be the proposer
    node_id = random.choice([0, 1, 2])
    value = "Same value"
    submit_value(node_id, value)

    # Print the file contents of all nodes to verify consensus
    for node_id in [0, 1, 2]:
        print_node_file(node_id)
    print()


# Function to simulate scenarios with two proposers
def simulate_two_proposers(scenario):
    if scenario == "a_wins":
        # Simulate a scenario where proposer A wins
        submit_value(2, "B's value")
        time.sleep(0.2)  # Introduce a delay to ensure A proposes after B
        submit_value(1, "A's value")
    elif scenario == "b_wins":
        # Simulate a scenario where proposer B wins
        submit_value(0, "A's value")
        submit_value(2, "B's value")

    # Print the file contents of all nodes to verify consensus
    for node_id in [0, 1, 2]:
        print_node_file(node_id)
    print()


# Main menu function to interact with the user
def main_menu():
    while True:
        print("\nPaxos Client Menu:")
        print("1. Single proposer")
        print("2. Two proposers (A wins)")
        print("3. Two proposers (B wins)")
        print("4. Exit")

        choice = input("Enter your choice (1-6): ")

        if choice == "1":
            simulate_single_proposer()
        elif choice == "2":
            simulate_two_proposers("a_wins")
        elif choice == "3":
            simulate_two_proposers("b_wins")
        elif choice == "4":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")


# Entry point of the script
if __name__ == "__main__":
    main_menu()
