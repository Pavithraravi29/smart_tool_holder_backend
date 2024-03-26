import asyncio
import json
import signal
from http import client

from bleak import BleakClient
import psycopg2
import tracemalloc
import datetime
import random
from websockets import connect


tracemalloc.start()

postgresql_config = {
    "user": "postgres",
    "password": "password",
    "database": "test_database",
    "host": "172.18.101.47",
    "port": "5432"
}

did = 1
initial_time = datetime.datetime.now()


async def notification_handler(sender: int, data: bytearray, conn, cur, websocket=None):
    global did
    global initial_time

    decoded_data = data.decode('utf-8')
    data_val = decoded_data.split(' ')[:-1]

    sql_insert_query = """INSERT INTO graph5 (id, tension, torsion, bending_moment_x, bending_moment_y, time_seconds, temperature) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET 
                          tension = EXCLUDED.tension, torsion = EXCLUDED.torsion, 
                          bending_moment_x = EXCLUDED.bending_moment_x, bending_moment_y = EXCLUDED.bending_moment_y,
                          time_seconds = EXCLUDED.time_seconds, temperature = EXCLUDED.temperature;"""

    # Convert string values to float
    current_time = datetime.datetime.now()

    # Calculate time elapsed with microseconds
    elapsed_time = (current_time - initial_time).total_seconds()

    insert_tuple = (
        did,
        (float(data_val[0])+2)*8,
        (float(data_val[1])+2)*8,
        (float(data_val[2])+2)*8,
        (float(data_val[1])+3)*7,
        elapsed_time,
        random.uniform(25.6, 29.7)
    )

    try:

        cur.execute(sql_insert_query, insert_tuple)
        conn.commit()
        print(insert_tuple)
        did += 1
    except psycopg2.Error as e:
        print(f"Error executing SQL query: {e}")
        conn.rollback()  # Rollback the transaction in case of an error



# Construct data dictionary
    data_dict = {
        "id": did,
        "tension": float(data_val[0]),
        "torsion": float(data_val[1]),
        "bending_moment_x": float(data_val[2]),
        "bending_moment_y": float(data_val[1]),  # Adjust as needed
        "time_seconds": elapsed_time,
        "temperature": random.uniform(25.6, 29.7)
    }

    # Convert data dictionary to JSON string
    serialized_data = json.dumps(data_dict)

    # Send data over WebSocket
    await websocket.send(serialized_data)


async def monitor_data(address, conn, cur):
    async with connect("ws://localhost:8000/ws_all_graph_data1") as websocket:
        characteristic_uuid = "00002101-0000-1000-8000-00805f9b34fb"
        await client.start_notify(characteristic_uuid, lambda sender, data: asyncio.ensure_future(
            notification_handler(sender, data, conn, cur)))

        try:
            while True:
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            pass

        except Exception as e:
            print(f"Error monitoring data: {e}")


async def main(address):
    conn = psycopg2.connect(**postgresql_config)
    cur = conn.cursor()
    print('Connected to PostgreSQL Database')

    loop = asyncio.get_event_loop()
    task = loop.create_task(monitor_data(address, conn, cur))

    def signal_handler(sig, frame):
        print("Exiting...")
        if conn is not None:
            cur.close()
            conn.close()
            print('PostgreSQL connection closed!')
        task.cancel()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    device_address = "84:CC:A8:77:A2:B6"
    asyncio.run(main(device_address))