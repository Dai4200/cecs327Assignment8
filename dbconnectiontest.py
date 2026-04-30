import psycopg2
from datetime import datetime, timedelta
import zoneinfo



def main():
    #gets your computers time
    now = datetime.now()
    print(now)
    #the datetime 30 days ago
    thirtydaysago = now - timedelta(days=30)
    print(thirtydaysago)

    conn = psycopg2.connect(
        "postgresql://neondb_owner:npg_SkA08nzqVJaN@ep-crimson-rice-am6t1zgy.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    )

    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) AS total_entries FROM "My_IoT_Table_virtual";')

    result = cur.fetchone()  # returns a single row
    total_entries = result[0]

    #get timestamp and convert to Los Angeles datetime
    #gets timestamps
    cur.execute('SELECT payload, payload ->> \'timestamp\' AS timestamp, payload ->> \'asset_uid\' AS asset_uid FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' ORDER BY timestamp DESC LIMIT 8;')

    results = cur.fetchall()
    print("Total entries:", total_entries)
    print(datetime.fromtimestamp(1777513025, tz = zoneinfo.ZoneInfo("America/Los_Angeles")))
    for item in results:
        print(item)
        #converts Unix timestamp to datetime
        print(datetime.fromtimestamp(int(item[1]), tz= zoneinfo.ZoneInfo("America/Los_Angeles")))
    print("\n\n\nNext Part")
    #get the last eight water flow sensor entries
    cur.execute('SELECT payload, payload ->> \'timestamp\' AS timestamp FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' ORDER BY timestamp DESC LIMIT 8;')
    results = cur.fetchall()
    print(results)
    print()
    for item in results:
        print(item[0])
        print(item[0]["board_name"])
        print(item[0]["YF-S201 - smartdishwasherwaterflow"])


    #AVERAGES FOR DIFFERENT TIME PERIODS FOR WATER CONSUMPTION FOR JULIAN
    # get the average pulses from the water flow sensor for the last hour for Julians dishwasher
    cur.execute(
        'SELECT AVG((payload ->> \'YF-S201 - smartdishwasherwaterflow\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
    avg = cur.fetchone()[0]
    avgLperMin = (avg/450) / 5
    print(f"Average water consumption was {avgLperMin:.5f} L/min for the last hour days by Julian's dishwasher")

    # get the average pulses from the water flow sensor for the last 7 days for Julians dishwasher
    cur.execute(
        'SELECT AVG((payload ->> \'YF-S201 - smartdishwasherwaterflow\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
    avg = cur.fetchone()[0]
    avgLperMin = (avg / 450) / 5
    print(f"Average water consumption was {avgLperMin:.5f} L/min for the last 7 days by Julian's dishwasher")

    # get the average pulses from the water flow sensor for the last 30 days for Julians dishwasher
    cur.execute(
        'SELECT AVG((payload ->> \'YF-S201 - smartdishwasherwaterflow\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'30 days\';')
    avg = cur.fetchone()[0]
    avgLperMin = (avg/450) / 5
    print(f"Average water consumption was {avgLperMin:.5f} L/min for the last 30 days by Julian's dishwasher")

    # time interval in seconds between sensor entries for Julian's sensors
    cur.execute(
        'SELECT payload, payload ->> \'timestamp\' AS timestamp FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'0l4-31e-g1h-037\' ORDER BY timestamp DESC LIMIT 8;')
    results = cur.fetchall()
    #time interval in seconds between sensor entries for Julian's sensors
    print(int(results[0][1]) - int(results[1][1]))
    print()

    print("FIND AVG WATER CONSUMPTION FOR DIEGO'S DISHWASHER")
    cur.execute(
        'SELECT payload, payload ->> \'timestamp\' AS timestamp FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\' ORDER BY timestamp DESC LIMIT 8;')
    for item in cur.fetchall():
        print(item)

    # get the average pulses from the water flow sensor for the last hour for Diegos dishwasher
    cur.execute(
        'SELECT AVG((payload ->> \'Dish-WaterConsumptionSensor\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'7p7-n87-wnt-y6w\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'1 hour\';')
    avg = cur.fetchone()[0]
    avgLperMin = (avg / 450)
    print(f"Average water consumption was {avgLperMin:.5f} L/min for the last hour by Diego's dishwasher")

    # get the average pulses from the water flow sensor for the last 7 days for Diegos dishwasher
    cur.execute(
        'SELECT AVG((payload ->> \'Dish-WaterConsumptionSensor\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'7p7-n87-wnt-y6w\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'7 days\';')
    avg = cur.fetchone()[0]
    avgLperMin = (avg / 450)
    print(f"Average water consumption was {avgLperMin:.5f} L/min for the last 7 days by Diego's dishwasher")

    # get the average pulses from the water flow sensor for the last 30 days for Diegos dishwasher
    cur.execute(
        'SELECT AVG((payload ->> \'Dish-WaterConsumptionSensor\')::double precision) FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' = \'diegosaurus2004@gmail.com/Assignment7\' AND payload ->> \'asset_uid\' = \'7p7-n87-wnt-y6w\' AND to_timestamp(CAST(payload ->> \'timestamp\' AS INTEGER)) >= NOW() - INTERVAL \'30 days\';')
    avg = cur.fetchone()[0]
    avgLperMin = (avg / 450)
    print(f"Average water consumption was {avgLperMin:.5f} L/min for the last 30 days by Diego's dishwasher")

    cur.close()
    conn.close()
main()
