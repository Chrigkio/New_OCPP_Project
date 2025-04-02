import asyncio
import logging
from datetime import datetime, timezone  # Import for handling timestamps
from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call_result, call
from ocpp.v16.enums import Action
import websockets


#CHARGER_SUPERVISION_URL = ws://192.168.1.101:9000

logging.basicConfig(level=logging.INFO)

# Shared dictionary to store meter data (voltage, active power, current, etc.)
meter_data = {}

# Suppress OCPP library logs or set a specific level
logging.getLogger("ocpp").setLevel(logging.WARNING)  # Change to WARNING or ERROR to reduce verbosity

class ChargePoint(cp):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)


    @on(Action.start_transaction)
    async def on_start_transaction(self, connector_id: int, id_tag: str, meter_start: int, timestamp: str, **kwargs):
        logging.info(f"StartTransaction received: Connector ID: {connector_id}, ID Tag: {id_tag}, Meter Start: {meter_start}, Timestamp: {timestamp}")
        # Respond with an Accepted status and a transaction ID
        return call_result.StartTransaction(
        transaction_id=1,  # Replace with your logic to generate a transaction ID
        id_tag_info={"status": "Accepted"}
    )

    @on(Action.stop_transaction)
    async def on_stop_transaction(self, **kwargs):
        """
        Handle StopTransaction messages from the charger.
        Acknowledge the message and log the transaction details.
        """
        logging.info(f"StopTransaction received: {kwargs}")
        return call_result.StopTransaction() 

    @on(Action.status_notification)
    async def on_status_notification(self, connector_id: int, error_code: str, status: str, **kwargs):
        """
        Handle StatusNotification messages from the charger.
        Logs the connector ID, status, and error code.
        """
        self.status = status
        #logging.info(f"StatusNotification received: Connector {connector_id}, Status: {status}, Error Code: {error_code}")
        return call_result.StatusNotification()

    @on(Action.heartbeat)
    async def on_heartbeat(self, **kwargs):
        """
        Handle Heartbeat messages from the charger.
        Responds with the current server time.
        """
        current_time = datetime.now(timezone.utc).isoformat()

        #logging.info(f"Heartbeat received. Responding with current time: {current_time}")
        return call_result.Heartbeat(current_time=current_time)

    @on(Action.boot_notification)
    async def on_boot_notification(self, charge_point_model: str, charge_point_vendor: str, **kwargs):
        """
        Handle BootNotification messages from the charger.
        Logs the charge point model and vendor, and responds with an Accepted status.
        """
        #logging.info(f"BootNotification received: Model: {charge_point_model}, Vendor: {charge_point_vendor}")
        # Respond with an Accepted status and a heartbeat interval
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=30,  # Heartbeat interval in seconds
            status="Accepted"
        )

    @on(Action.security_event_notification)
    async def on_security_event_notification(self, type: str, timestamp: str, **kwargs):
        """
        Handle SecurityEventNotification messages from the charger.
        Logs the event type and timestamp.
        """
        #logging.info(f"SecurityEventNotification received: Type: {type}, Timestamp: {timestamp}")
        return call_result.SecurityEventNotification()


    @on(Action.meter_values)
    async def on_meter_values(self, connector_id: int, meter_value: list, **kwargs):
        """
        Handle MeterValues messages from the charger.
        Logs the connector ID and the meter values.
        Extracts and logs voltage, active power, and current.
        """
        global meter_data  # Access the shared meter data dictionary

        # Initialize the connector's data if not already present
        if connector_id not in meter_data:
            meter_data[connector_id] = {}

        for value in meter_value:
            timestamp = value.get("timestamp")
            sampled_values = value.get("sampled_value", [])
            for sampled_value in sampled_values:
                measurand = sampled_value.get("measurand", "Unknown")
                phase = sampled_value.get("phase", "Unknown")
                value = sampled_value.get("value", "Unknown")
                unit = sampled_value.get("unit", "Unknown")

                # Store data based on the measurand
                if measurand == "Voltage" and phase == "L1-N":
                    meter_data[connector_id]["voltage"] = {
                        "timestamp": timestamp,
                        "value": value,
                        "unit": unit
                    }
                    logging.info(f"L1 Voltage for Connector {connector_id}: {value} {unit} at {timestamp}")
                elif measurand == "Power.Active.Import":
                    meter_data[connector_id]["active_power"] = {
                        "timestamp": timestamp,
                        "value": value,
                        "unit": unit
                    }
                    logging.info(f"Active Power for Connector {connector_id}: {value} {unit} at {timestamp}")
                elif measurand == "Current.Import":
                    meter_data[connector_id]["current"] = {
                        "timestamp": timestamp,
                        "value": value,
                        "unit": unit
                    }
                    logging.info(f"Current for Connector {connector_id}: {value} {unit} at {timestamp}")

        return call_result.MeterValues()
    

async def send_remote_stop_transaction(transaction_id: int, charge_point: ChargePoint):
    """
    Send a RemoteStopTransaction command to the charge point.
    :param transaction_id: The ID of the transaction to stop.
    :param charge_point: The ChargePoint instance.
    """
    if charge_point:
        # Use the correct RemoteStopTransaction class
        request = call.RemoteStopTransaction(transaction_id=transaction_id)
        response = await charge_point.call(request)

        if response.status == "Accepted":
            logging.info(f"RemoteStopTransaction for transaction ID {transaction_id} was accepted.")
        else:
            logging.warning(f"RemoteStopTransaction for transaction ID {transaction_id} was rejected.")
    else:
        logging.error("No charge point connected to send RemoteStopTransaction.")

async def send_remote_start_transaction(id_tag: str, connector_id: int, charge_point: ChargePoint):
    """
    Send a RemoteStartTransaction command to the charge point.
    :param id_tag: The ID tag to authorize the transaction.
    :param connector_id: The connector ID to start charging on.
    :param charge_point: The ChargePoint instance.
    """
    if charge_point:
        request = call.RemoteStartTransaction(
            id_tag=id_tag,
            connector_id=connector_id
        )
        response = await charge_point.call(request)

        if response.status == "Accepted":
            logging.info(f"RemoteStartTransaction for Connector {connector_id} with ID Tag {id_tag} was accepted.")
        else:
            logging.warning(f"RemoteStartTransaction for Connector {connector_id} with ID Tag {id_tag} was rejected.")
    else:
        logging.error("No charge point connected to send RemoteStartTransaction.")

async def listen_for_commands(charge_point: ChargePoint):
    """
    Listen for "start" or "stop" input in the terminal to control charging remotely.
    Runs the input function in a separate thread to avoid blocking the event loop.
    """
    while True:
        # Run the input function in a separate thread
        user_input = await asyncio.to_thread(input, "Type 'start' to start charging or 'stop' to stop charging: ")
        user_input = user_input.strip().lower()
        if user_input == "start":
            id_tag = "TACW2242422T8395"  # Replace with the actual ID tag
            connector_id = 1  # Replace with the actual connector ID
            await send_remote_start_transaction(id_tag, connector_id, charge_point)
            print(f"Charging started on Connector {connector_id} with ID Tag {id_tag}.")  # User feedback
        elif user_input == "stop":
            transaction_id = 1  # Replace with the actual transaction ID
            await send_remote_stop_transaction(transaction_id, charge_point)
            print(f"Transaction {transaction_id} stopped successfully.")  # User feedback

async def on_connect(websocket):
    """
    Handle a new connection from a charge point.
    Create a ChargePoint instance and start listening for messages.
    """
    charge_point_id = websocket.request.path.strip("/")
    charge_point = ChargePoint(charge_point_id, websocket)

    logging.info(f"Charge point {charge_point_id} connected.")
    try:
        # Start listening for "start" and "stop" input in a separate task
        asyncio.create_task(listen_for_commands(charge_point))
        await charge_point.start()
    except Exception as e:
        logging.error(f"Error handling charge point {charge_point_id}: {e}")

async def main():
    """
    Start the WebSocket server to listen for OCPP connections.
    """
    # Configure the WebSocket server with a custom ping interval and timeout
    server = await websockets.serve(
        on_connect,
        "0.0.0.0",
        9000,
        subprotocols=["ocpp1.6"],
        ping_interval=30,  # Send a ping every 30 seconds
        ping_timeout=60,   # Wait 60 seconds for a pong before considering the connection closed
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
