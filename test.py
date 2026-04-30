import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import zoneinfo

def main():
    #gets your computers time
    now = datetime.now()
    print(now)
    #the datetime 30 days ago
    thirtydaysago = now - timedelta(days=30)
    print(thirtydaysago)

    conn2 = psycopg2.connect(
        "postgresql://neondb_owner:npg_MiFfNHu9SY8R@ep-autumn-recipe-amo5a3ph-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    )

    conn = psycopg2.connect(
        "postgresql://neondb_owner:npg_SkA08nzqVJaN@ep-crimson-rice-am6t1zgy.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    )

    cur = conn.cursor()
    #get timestamp and convert to Los Angeles datetime
    #gets timestamps
    cur.execute('SELECT payload, payload ->> \'timestamp\' AS timestamp, payload ->> \'asset_uid\' AS asset_uid FROM "My_IoT_Table_virtual" WHERE payload ->> \'topic\' <> \'diegosaurus2004@gmail.com/Assignment7\' ORDER BY timestamp DESC LIMIT 8;')

    results = cur.fetchall()
    print(datetime.fromtimestamp(1777513025, tz = zoneinfo.ZoneInfo("America/Los_Angeles")))
    for item in results:
        #converts Unix timestamp to datetime
        print(datetime.fromtimestamp(int(item[1]), tz= zoneinfo.ZoneInfo("America/Los_Angeles")))
    
    #Julian 
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
    print(f"Average moisture was {moisture:.5f} % for the last hour in Julian's fridges")

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
    print(f"Average moisture was {moisture:.5f} % for the last week in Julian's fridges")

    #Diego
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
    print(f"Average moisture was {moisture:.5f} % for the last hour in Diego's fridges")

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
    print(f"Average moisture was {moisture:.5f} % for the last hour in Diego's fridges")

    conn.close()
    conn2.close()
main()