import asyncio
import logging
from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call_result
from ocpp.v16.enums import Action
import websockets

#CHARGER_SUPERVISION_URL = ws://192.168.0.10:9000


logging.basicConfig(level=logging.INFO)

class ChargePoint(cp):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)

    @on(Action.status_notification)
    async def on_status_notification(self, connector_id: int, error_code: str, status: str, **kwargs):
        """
        Handle StatusNotification messages from the charger.
        Logs the connector ID, status, and error code.
        """
        logging.info(f"StatusNotification received: Connector {connector_id}, Status: {status}, Error Code: {error_code}")
        return call_result.StatusNotification()

    @on(Action.heartbeat)
    async def on_heartbeat(self, **kwargs):
        """
        Handle Heartbeat messages from the charger.
        Responds with the current server time.
        """
        from datetime import datetime, timezone
        current_time = datetime.now(timezone.utc).isoformat()
        logging.info(f"Heartbeat received. Responding with current time: {current_time}")
        return call_result.Heartbeat(current_time=current_time)

async def on_connect(websocket):
    """
    Handle a new connection from a charge point.
    Create a ChargePoint instance and start listening for messages.
    """
    charge_point_id = websocket.request.path.strip("/")
    charge_point = ChargePoint(charge_point_id, websocket)

    logging.info(f"Charge point {charge_point_id} connected.")
    try:
        await charge_point.start()
    except Exception as e:
        logging.error(f"Error handling charge point {charge_point_id}: {e}")

async def main():
    """
    Start the WebSocket server to listen for OCPP connections.
    """
    server = await websockets.serve(
        on_connect, "0.0.0.0", 9000, subprotocols=["ocpp1.6"]
    )

    logging.info("OCPP 1.6 server started. Waiting for connections...")
    try:
        await server.wait_closed()
    except asyncio.CancelledError:
        logging.info("Server shutdown gracefully.")
    except Exception as e:
        logging.error(f"Unexpected server error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
