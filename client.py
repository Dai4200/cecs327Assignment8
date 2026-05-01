import socket
import sys
import ipaddress

query1 = "What is the average moisture inside our kitchen fridges in the past hours, week and month?"
query2 = "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?"
query3 = "Which house consumed more electricity in the past 24 hours, and by how much?"

def main():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while True:
        server_ip = input("Enter the server IP address: ").strip()
        try:
           ipcheck = ipaddress.ip_address(server_ip)
           break
        except ValueError:
            print("ERROR: Invalid IP Address.")

    while True:
        try:
            server_port = int(input("Enter the desired port number: ").strip())
            if 1 <= server_port <= 65535:
                break
            else:
                print("Port number out of range.")
                continue
        except ValueError:
            print("ERROR: Invalid port number")


    try:
        client_socket.connect((server_ip, server_port))
        print(f"\nConnected to server at {server_ip}:{server_port}\n")

    except OSError:
        print(f"Could not connect.")
        sys.exit(1)
    except ConnectionRefusedError:
        print("Connection refused.")
        sys.exit(1)

    while True:
        print(f"1. {query1}\n2. {query2}\n3. {query3}")
        message = input("Choose from the messages or enter 'quit' to exit: ").strip()

        if message.lower() == 'quit':
            print("Connection closed.")
            break

        elif message.strip() not in ('1', '2', '3'):
            print("Please only enter one of the three accepted queries.")
            continue

        else:
            if message.lower() == '1':
                message = query1
            elif message.lower() == '2':
                message = query2
            elif message.lower() == '3':
                message = query3

        client_socket.send(message.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        print(f"Server response: {response}\n")

    client_socket.close()

if __name__ == "__main__":
    main()