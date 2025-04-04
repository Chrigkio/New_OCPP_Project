import asyncio
import logging
from datetime import datetime, timezone
from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call_result, call
from ocpp.v16.enums import Action
import websockets
from flask import Flask, render_template, request
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)

# Shared dictionary to store meter data (voltage, active power, current, etc.)
meter_data = {}

# Suppress OCPP library logs or set a specific level
logging.getLogger("ocpp").setLevel(logging.WARNING)

app = Flask(__name__)
command = None  # Global variable to store the command ("start" or "stop")


@app.route("/", methods=["GET", "POST"])
def index():
    global command
    if request.method == "POST":
        if "start" in request.form:
            command = "start"
        elif "stop" in request.form:
            command = "stop"
    return """
    <form method="post">
        <button name="start" type="submit">Start</button>
        <button name="stop" type="submit">Stop</button>
    </form>
    """


def run_flask():
    app.run(host="0.0.0.0", port=5000)


class ChargePoint(cp):
    def __init__(self, id, websocket):
        super().__init__(id, websocket)

    @on(Action.start_transaction)
    async def on_start_transaction(self, connector_id: int, id_tag: str, meter_start: int, timestamp: str, **kwargs):
        logging.info(f"StartTransaction received: Connector ID: {connector_id}, ID Tag: {id_tag}, Meter Start: {meter_start}, Timestamp: {timestamp}")
        return call_result.StartTransaction(
            transaction_id=1,
            id_tag_info={"status": "Accepted"}
        )

    @on(Action.stop_transaction)
    async def on_stop_transaction(self, **kwargs):
        logging.info(f"StopTransaction received: {kwargs}")
        return call_result.StopTransaction()

    @on(Action.meter_values)
    async def on_meter_values(self, connector_id: int, meter_value: list, **kwargs):
        global meter_data
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
    if charge_point:
        request = call.RemoteStopTransaction(transaction_id=transaction_id)
        response = await charge_point.call(request)

        if response.status == "Accepted":
            logging.info(f"RemoteStopTransaction for transaction ID {transaction_id} was accepted.")
        else:
            logging.warning(f"RemoteStopTransaction for transaction ID {transaction_id} was rejected.")
    else:
        logging.error("No charge point connected to send RemoteStopTransaction.")


async def send_remote_start_transaction(id_tag: str, connector_id: int, charge_point: ChargePoint):
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


async def listen_for_commands(charge_point: ChargePoint, command: str):
    """
    Process the given command to start or stop charging.
    :param charge_point: The ChargePoint instance.
    :param command: The command to process ("start" or "stop").
    """
    command = command.strip().lower()
    if command == "start":
        id_tag = "TACW2242422T8395"  # Replace with the actual ID tag
        connector_id = 1  # Replace with the actual connector ID
        await send_remote_start_transaction(id_tag, connector_id, charge_point)
        print(f"Charging started on Connector {connector_id} with ID Tag {id_tag}.")
    elif command == "stop":
        transaction_id = 1  # Replace with the actual transaction ID
        await send_remote_stop_transaction(transaction_id, charge_point)
        print(f"Transaction {transaction_id} stopped successfully.")
    else:
        print(f"Unknown command: {command}. Please use 'start' or 'stop'.")


async def simulated_input(charge_point: ChargePoint):
    """
    Simulate input by using the global 'command' variable set by Flask buttons.
    """
    global command
    while True:
        if command:
            await listen_for_commands(charge_point, command)
            command = None  # Reset the command after processing
        await asyncio.sleep(1)  # Check for new commands every second


async def on_connect(websocket):
    """
    Handle a new connection from a charge point.
    Create a ChargePoint instance and start listening for messages.
    """
    charge_point_id = websocket.request.path.strip("/")
    charge_point = ChargePoint(charge_point_id, websocket)

    logging.info(f"Charge point {charge_point_id} connected.")
    try:
        # Start the simulated input task
        asyncio.create_task(simulated_input(charge_point))
        await charge_point.start()
    except Exception as e:
        logging.error(f"Error handling charge point {charge_point_id}: {e}")


async def main():
    server = await websockets.serve(
        on_connect,
        "0.0.0.0",
        9000,
        subprotocols=["ocpp1.6"],
        ping_interval=30,
        ping_timeout=60,
    )

    logging.info("OCPP 1.6 server started. Waiting for connections...")

    try:
        await server.wait_closed()
    except asyncio.CancelledError:
        logging.info("Server shutdown gracefully.")
    except Exception as e:
        logging.error(f"Unexpected server error: {e}")


if __name__ == "__main__":
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    asyncio.run(main())