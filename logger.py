from dotenv import load_dotenv
import mysql.connector as mysql
from os import getenv
from pymemcache.client import base
import datetime

if __name__ == "__main__":
    load_dotenv()


    client = base.Client((getenv("MEMCACHED_IP"), getenv("MEMCACHED_PORT")))
    stats = client.stats()
    client.close()

    db = mysql.connect(
        host=getenv("MYSQL_HOST"),
        user=getenv("MYSQL_USERNAME"),
        password=getenv("MYSQL_PASSWORD"),
        database=getenv("MYSQL_DATABASE")
    )
    
    cursor = db.cursor()

    insert = (
        "INSERT INTO Log(Created, Uptime, Used_Cache_Size, Total_Connections, Get_Cmd, Set_Cmd, Items_Evicted, Bytes_Read, Bytes_Written, Current_Items, Hits, Misses)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    data = (
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        stats.get(b'uptime'),
        stats.get(b'bytes'),
        stats.get(b'total_connections'),
        stats.get(b'cmd_get'),
        stats.get(b'cmd_set'),
        stats.get(b'evictions'),
        stats.get(b'bytes_read'),
        stats.get(b'bytes_written'),
        stats.get(b'curr_items'),
        stats.get(b'get_hits'),
        stats.get(b'get_misses')
    )

    cursor.execute(insert, data)
    db.commit()

    delete = ("DELETE FROM Log where Created < %s")
    delete_date = datetime.datetime.now() - datetime.timedelta(days = 30 * 14)

    cursor.execute(delete, (delete_date.strftime("%Y-%m-%d %H:%M:%S"),))
    db.commit()

    db.close()
