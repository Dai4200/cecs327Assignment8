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



    cur.close()
    conn.close()
main()
