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
        messages = ''

        if message == query1:
            conn2 = psycopg2.connect(
                "postgresql://neondb_owner:npg_MiFfNHu9SY8R@ep-autumn-recipe-amo5a3ph-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
            )

            conn = psycopg2.connect(
                "postgresql://neondb_owner:npg_SkA08nzqVJaN@ep-crimson-rice-am6t1zgy.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
            )

            cur = conn.cursor()
            # get timestamp and convert to Los Angeles datetime
            # gets timestamps
            cur.execute(
                'SELECT payload, payload ->> \'timestamp\' AS timestamp, payload ->> \'asset_uid\' AS asset_uid FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' ORDER BY timestamp DESC LIMIT 8;')

            results = cur.fetchall()
            print(datetime.fromtimestamp(1777513025, tz=zoneinfo.ZoneInfo("America/Los_Angeles")))
            for item in results:
                # converts Unix timestamp to datetime
                print(datetime.fromtimestamp(int(item[1]), tz=zoneinfo.ZoneInfo("America/Los_Angeles")))

            # Julian
            cur.execute(
                'SELECT AVG((payload ->> \'Moisture Meter - smartfridgemoisture\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'3z7-285-2v4-972\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
            avg = cur.fetchone()[0]
            avgper = avg
            cur.execute(
                'SELECT AVG((payload ->> \'Moisture Meter - smartfridgemoisture 1 24774f23-d689-411b-84e5-4bbe5665cd06\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'4ea9f08a-5969-4f57-b4c5-cc78d82276f7\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
            avg = cur.fetchone()[0]
            avgper += avg
            avgper /= 2
            moisture = (avgper - 0) / (40 - 0) * 100
            messages += f"Average moisture was {moisture:.5f} % for the last hour in Julian's fridges\n"

            cur.execute(
                'SELECT AVG((payload ->> \'Moisture Meter - smartfridgemoisture\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'3z7-285-2v4-972\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
            avg = cur.fetchone()[0]
            avgper = avg
            cur.execute(
                'SELECT AVG((payload ->> \'Moisture Meter - smartfridgemoisture 1 24774f23-d689-411b-84e5-4bbe5665cd06\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'4ea9f08a-5969-4f57-b4c5-cc78d82276f7\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
            avg = cur.fetchone()[0]
            avgper += avg
            avgper /= 2
            moisture = (avgper - 0) / (40 - 0) * 100
            messages += f"Average moisture was {moisture:.5f} % for the last week in Julian's fridges\n"

            # Diego
            cur.execute(
                'SELECT AVG((payload ->> \'Fridge-Moist\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'v8f-h0p-qdn-4hp\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
            avg = cur.fetchone()[0]
            avgper = avg

            cur.execute(
                'SELECT AVG((payload ->> \'Fridge-Moist 2 706390d0-9625-4fd2-b6b4-bf81ce0fe5ad\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'804352b9-3afd-4591-bc3d-b0c4cad5a141\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
            avg = cur.fetchone()[0]
            avgper += avg
            avgper /= 2
            moisture = (avgper - 0) / (40 - 0) * 100
            messages += f"Average moisture was {moisture:.5f} % for the last hour in Diego's fridges\n"

            cur.execute(
                'SELECT AVG((payload ->> \'Fridge-Moist\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'v8f-h0p-qdn-4hp\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
            avg = cur.fetchone()[0]
            avgper = avg

            cur.execute(
                'SELECT AVG((payload ->> \'Fridge-Moist 2 706390d0-9625-4fd2-b6b4-bf81ce0fe5ad\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'804352b9-3afd-4591-bc3d-b0c4cad5a141\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
            avg = cur.fetchone()[0]
            avgper += avg
            avgper /= 2
            moisture = (avgper - 0) / (40 - 0) * 100
            messages += f"Average moisture was {moisture:.5f} % for the last hour in Diego's fridges\n"

            conn.close()
            conn2.close()

        if message == query2:
            conn = psycopg2.connect(
                "postgresql://neondb_owner:npg_SkA08nzqVJaN@ep-crimson-rice-am6t1zgy.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
            )
            cur = conn.cursor()

            # get the average pulses from the water flow sensor for the last hour for Julians dishwasher
            cur.execute(
                'SELECT AVG((payload ->> \'YF-S201 - smartdishwasherwaterflow\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
            avg = cur.fetchone()[0]
            avgLperMin = (avg / 450) / 5
            messages += f"Average water consumption was {avgLperMin:.5f} L/min for the last hour days by Julian's dishwasher\n"

            # get the average pulses from the water flow sensor for the last 7 days for Julians dishwasher
            cur.execute(
                'SELECT AVG((payload ->> \'YF-S201 - smartdishwasherwaterflow\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
            avg = cur.fetchone()[0]
            avgLperMin = (avg / 450) / 5
            messages += f"Average water consumption was {avgLperMin:.5f} L/min for the last 7 days by Julian's dishwasher\n"

            # get the average pulses from the water flow sensor for the last 30 days for Julians dishwasher
            cur.execute(
                'SELECT AVG((payload ->> \'YF-S201 - smartdishwasherwaterflow\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'30 days\';')
            avg = cur.fetchone()[0]
            avgLperMin = (avg / 450) / 5
            messages += f"Average water consumption was {avgLperMin:.5f} L/min for the last 30 days by Julian's dishwasher\n"

            # get the average pulses from the water flow sensor for the last hour for Diegos dishwasher
            cur.execute(
                'SELECT AVG((payload ->> \'Dish-WaterConsumptionSensor\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'7p7-n87-wnt-y6w\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
            avg = cur.fetchone()[0]
            avgLperMin = (avg / 450)
            messages += f"Average water consumption was {avgLperMin:.5f} L/min for the last hour by Diego's dishwasher\n"

            # get the average pulses from the water flow sensor for the last 7 days for Diegos dishwasher
            cur.execute(
                'SELECT AVG((payload ->> \'Dish-WaterConsumptionSensor\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'7p7-n87-wnt-y6w\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
            avg = cur.fetchone()[0]
            avgLperMin = (avg / 450)
            messages += f"Average water consumption was {avgLperMin:.5f} L/min for the last 7 days by Diego's dishwasher\n"

            # get the average pulses from the water flow sensor for the last 30 days for Diegos dishwasher
            cur.execute(
                'SELECT AVG((payload ->> \'Dish-WaterConsumptionSensor\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'7p7-n87-wnt-y6w\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'30 days\';')
            avg = cur.fetchone()[0]
            avgLperMin = (avg / 450)
            messages += f"Average water consumption was {avgLperMin:.5f} L/min for the last 30 days by Diego's dishwasher\n"

            cur.close()
            conn.close()

        if message == query3:
            pass

        client_socket.send(messages.encode('utf-8'))
        print(f"Sent: {messages}\n")

    client_socket.close()
    server_socket.close()
    print("Server closed.")

if __name__ == "__main__":
    main()