import psycopg2

def main():
    conn = psycopg2.connect(
        "postgresql://neondb_owner:npg_SkA08nzqVJaN@ep-crimson-rice-am6t1zgy.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    )

    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) AS total_entries FROM "My_IoT_Table_virtual";')

    result = cur.fetchone()  # returns a single row
    total_entries = result[0]

    print("Total entries:", total_entries)


    cur.close()
    conn.close()
main()
