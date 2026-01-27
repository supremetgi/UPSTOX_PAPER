import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import pika
import json
import threading

app = FastAPI()

# This class manages browser connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- RABBITMQ CONSUMER THREAD ---
def start_rabbitmq_consumer(loop):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=600))
    channel = connection.channel()
    channel.queue_declare(queue='insider_alerts')

    def callback(ch, method, properties, body):
        # When a trade arrives, tell the FastAPI loop to broadcast it
        message = body.decode()
        asyncio.run_coroutine_threadsafe(manager.broadcast(message), loop)

    channel.basic_consume(queue='insider_alerts', on_message_callback=callback, auto_ack=True)
    print("🚀 Bridge connected to RabbitMQ. Waiting for trades...")
    channel.start_consuming()

@app.on_event("startup")
async def startup_event():
    # Start RabbitMQ in a separate thread so it doesn't block the web server
    loop = asyncio.get_event_loop()
    threading.Thread(target=start_rabbitmq_consumer, args=(loop,), daemon=True).start()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)