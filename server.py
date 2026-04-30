import socket
import psycopg2
from datetime import datetime, timedelta
import zoneinfo

query1 = "What is the average moisture inside our kitchen fridges in the past hours, week and month?"
query2 = "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?"
query3 = "Which house consumed more electricity in the past 24 hours, and by how much?"

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Ensures the inputted port is a number and within the proper range.
    while True:
        try:
            port = int(input("Enter the desired port number: ").strip())
            if 1 <= port <= 65535:
                break
            else:
                print("Port number out of range.")
                continue
        except ValueError:
            print("ERROR: Invalid port number")

    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    print('ip address:', ip_address, end = '\n')

    #Although binding to ' ' or 0.0.0.0 usually works, getting local ip address seemed to speed up connection.
    server_socket.bind((ip_address, port))

    server_socket.listen(1)
    print(f"\nListening on port {port}")

    client_socket, client_address = server_socket.accept()
    print(f"Connected to client: {client_address}\n")

    #Loops until the client types 'quit'.
    while True:
        data = client_socket.recv(1024)

        #When the client types 'quit', this branch activates.
        if not data:
            print("Client disconnected.")
            break

        message = data.decode('utf-8')
        print(f"Received: {message}")

        if message == query1:
            #paste queries here when done.
            #should probably return one response
            pass

        if message == query2:
            pass

        if message == query3:
            pass
        response = "hi"
        client_socket.send(response.encode('utf-8'))
        print(f"Sent: {response}\n")

    client_socket.close()
    server_socket.close()
    print("Server closed.")

if __name__ == "__main__":
    main()