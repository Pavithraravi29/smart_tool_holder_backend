import asyncio
from datetime import datetime
from random import uniform, random

import asyncpg
from fastapi import FastAPI, HTTPException, WebSocket, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import csv
import asyncpg
import time
import random
from datetime import datetime, timezone
import json
from decimal import Decimal


app = FastAPI()
current_time = datetime.now().isoformat()
start_time = None

# CORS Configuration
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def connect_to_postgres():
    # Update the following parameters with your PostgreSQL connection details
    postgres_config = {
        "user": "postgres",
        "password": "password",
        "database": "test_database",
        "host": "localhost",
    }

    pool = await asyncpg.create_pool(**postgres_config)
    return pool


@app.on_event("startup")
async def startup():
    app.state.postgres_pool = await connect_to_postgres()


@app.post("/add_user")
async def add_user(username: str, password: str):
    query = "INSERT INTO login (username, password) VALUES ($1, $2)"
    async with app.state.postgres_pool.acquire() as connection:
        try:
            await connection.execute(query, username, password)
            return {"message": "User added successfully"}
        except asyncpg.exceptions.UniqueViolationError:
            raise HTTPException(status_code=400, detail="Username already exists")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to add user: {str(e)}")


@app.get("/check_user")
async def check_user(username: str, password: str):
    query = "SELECT * FROM login WHERE username = $1 AND password = $2"
    async with app.state.postgres_pool.acquire() as connection:
        result = await connection.fetchrow(query, username, password)
        if result:
            return {"exists": True}
        else:
            return {"exists": False}


# @app.get("/all_graph_data")
# async def get_all_graph_data():
#     query = """
#     SELECT "id", "tension", "torsion", "bending_moment_x", "bending_moment_y", "time_seconds", "temperature"
#     FROM "graph"
#     """
#     async with app.state.postgres_pool.acquire() as connection:
#         start_time = time.time()
#
#         results = await connection.fetch(query)
#         if results:
#             data = [dict(row) for row in results]
#             end_time = time.time()
#             print(f"Total Time: {(end_time - start_time) * 1000} ms")
#             return data
#         else:
#             raise HTTPException(status_code=404, detail="No data found")

# Custom JSON encoder to handle Decimal serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


@app.websocket("/ws_all_graph_data1")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    query = """
    SELECT "id", "tension", "torsion", "bending_moment_x", "bending_moment_y", "time_seconds", "temperature"
    FROM "graph"
    """
    async with app.state.postgres_pool.acquire() as connection:
        while True:
            results = await connection.fetch(query)
            if results:
                for row in results:
                    data = dict(row)
                    data = {key: round(value, 4) for key, value in data.items()}
                    # print(data)
                    serialized_data = json.dumps(data, cls=DecimalEncoder)
                    await websocket.send_text(serialized_data)
                    # print(serialized_data)
                    await asyncio.sleep(0.1)  # Delay between sending each item
            else:
                raise HTTPException(status_code=404, detail="No data found")



# @app.websocket("/ws_all_graph_data")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#
#     batch_size = 1000  # Set the batch size
#     query = """
#     SELECT "id", "tension", "torsion", "bending_moment_x", "bending_moment_y", "time_seconds", "temperature"
#     FROM "graph"
#     """
#     async with app.state.postgres_pool.acquire() as connection:
#         while True:
#             results = await connection.fetch(query)
#             if results:
#                 for i in range(0, len(results), batch_size):
#                     batch = results[i:i + batch_size]
#                     data = [dict(row) for row in batch]
#                     serialized_data = json.dumps(data, cls=DecimalEncoder)
#                     await websocket.send_text(serialized_data)
#                     await asyncio.sleep(0.3)  # Delay between sending each batch
#             else:
#                 raise HTTPException(status_code=404, detail="No data found")


@app.websocket("/ws_all_graph_data")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    batch_size = 1000  # Set the batch size
    last_fetched_id = 0  # Initialize the last fetched ID

    while True:
        query = f"""
        SELECT "id", "tension", "torsion", "bending_moment_x", "bending_moment_y", "time_seconds", "temperature"
        FROM "graph"
        WHERE "id" > {last_fetched_id}
        ORDER BY "id"
        LIMIT {batch_size}
        """

        async with app.state.postgres_pool.acquire() as connection:
            results = await connection.fetch(query)

            if results:
                last_fetched_id = results[-1]['id']  # Update last fetched ID

                for i in range(0, len(results), batch_size):
                    batch = results[i:i + batch_size]
                    data = [dict(row) for row in batch]
                    serialized_data = json.dumps(data, cls=DecimalEncoder)
                    await websocket.send_text(serialized_data)
                    await asyncio.sleep(0.1)  # Delay between sending each batch
            else:
                # No new data found, wait before checking again
                await asyncio.sleep(5)  # Adjust this interval as needed



@app.websocket("/ws_cockpit")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    while True:
        bending_moment = round(random.uniform(0, 100), 0)  # Round to the nearest whole number
        torsion = round(random.uniform(-100, 100), 0)      # Round to the nearest whole number
        data = {
            "BendingMoment": f"{bending_moment}%",
            "Torsion": f"{torsion}%"
        }
        # Convert the data to JSON format
        json_data = json.dumps(data)

        # Send the JSON data through the WebSocket
        await websocket.send_text(json_data)

        # Sleep for a while before sending the next random values
        await asyncio.sleep(8)





async def send_sensor_data(websocket: WebSocket):
    global start_time
    await websocket.accept()
    start_time = datetime.now(timezone.utc)  # Set the start time
    while True:
        current_time = datetime.now(timezone.utc).astimezone().isoformat()
        tension = random.uniform(10.0, 50.0)
        torsion = random.uniform(5.0, 30.0)
        bending_moment_x = random.uniform(-5.0, 30.0)
        bending_moment_y = random.uniform(-5.0, 30.0)

        # Calculate time difference in seconds
        time_difference = (datetime.now(timezone.utc) - start_time).total_seconds()

        sensor_data = {
            "Timestamp": current_time,
            "Tension": tension,
            "Torsion": torsion,
            "Bending_moment_x": bending_moment_x,
            "Bending_moment_y": bending_moment_y,
            "Time_seconds": round(time_difference, 6),  # Round to 6 decimal places for time seconds
            "Temperature": random.uniform(15.0, 40.0)
        }

        await asyncio.sleep(0.1)
        await websocket.send_json(sensor_data)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await send_sensor_data(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="172.18.101.47", port=1234)

# TO RUN
# uvicorn main:app --reload --host 172.18.101.47 --port 1234
