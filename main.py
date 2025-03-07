"""
Data Collection System

Main entry point for the data collection system, coordinating sensor data
collection, publisher control, and graceful shutdown using a control file.
"""

import os
import asyncio
import json
import logging
import signal
import sys
import platform
import subprocess
from pathlib import Path
from typing import List, TypedDict

from services import DataCollector, SensorConfig, CollectorState, Sensor
from drivers import DavisVantagePro2

# Crear la carpeta 'logs' si no existe
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/data_collection.log")],
)
logger = logging.getLogger("data_collector")


class StationConfig(TypedDict):
    name: str
    location: str
    latitude: float
    longitude: float
    elevation: float


class ControlState(TypedDict):
    data_collector: str
    publisher: str


async def shutdown(
    collector: DataCollector, publisher_process: subprocess.Popen
) -> None:
    """Perform a graceful shutdown for both data collector and publisher."""
    logger.info("Shutting down...")
    if collector:
        collector.state = CollectorState.STOPPING
        await asyncio.sleep(1)  # Dar tiempo a que termine
    if publisher_process and publisher_process.poll() is None:
        with open("control.json", "w") as f:
            json.dump(
                {"data_collector": "STOPPED", "publisher": "STOPPED"}, f, indent=4
            )
        publisher_process.terminate()
        try:
            publisher_process.wait(timeout=5)
            logger.info("Publisher stopped")
        except subprocess.TimeoutExpired:
            publisher_process.kill()
            logger.warning("Publisher process killed after timeout")


def signal_handler(
    collector: DataCollector,
    publisher_process: subprocess.Popen,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Handle OS signals for graceful shutdown."""
    logger.info("Received termination signal")
    asyncio.create_task(shutdown(collector, publisher_process))
    loop.call_later(2, loop.stop)


async def main() -> None:
    """Main entry point for the application."""
    logger.info("Starting data collection system")

    # Cargar configuración desde archivo
    with open("config.json") as f:
        config = json.load(f)

    station_config: StationConfig = config["station"]
    logger.info(
        f"Station: {station_config['name']} at {station_config['location']} "
        f"(Lat: {station_config['latitude']}, Lon: {station_config['longitude']}, "
        f"Elev: {station_config['elevation']} m)"
    )

    sensors_config: List[SensorConfig] = config["sensors"]

    # Mapear nombres de sensores a sus clases
    sensor_classes = {
        "davisvp2": lambda: DavisVantagePro2(port="COM4"),
    }

    # Crear instancias de sensores
    sensors: List[Sensor] = [sensor_classes[cfg["name"]]() for cfg in sensors_config]

    # Configurar columnas
    columns = ["timestamp"]
    for sensor_config in sensors_config:
        columns.extend(sensor_config["keys"])

    # Inicializar DataCollector y Publisher
    async with DataCollector(output_path=Path("data"), logger=logger) as collector:
        collector.set_columns(columns)

        # Iniciar publisher como subprocesso
        publisher_process = None
        try:
            publisher_process = subprocess.Popen(
                ["python", "publisher.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            logger.info(f"Publisher started with PID {publisher_process.pid}")
        except Exception as e:
            logger.error(f"Error starting publisher: {e}")

        # Escribir estado inicial en control.json
        with open("control.json", "w") as f:
            json.dump(
                {"data_collector": "RUNNING", "publisher": "RUNNING"}, f, indent=4
            )

        loop = asyncio.get_running_loop()

        # Manejo de signals dependiendo el sistema operativo
        if platform.system() != "Windows":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig, lambda: signal_handler(collector, publisher_process, loop)
                )
        else:

            async def windows_signal_handler():
                try:
                    while collector.state != CollectorState.STOPPING:
                        await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    signal_handler(collector, publisher_process, loop)

            asyncio.create_task(windows_signal_handler())

        # Tareas de colección
        collection_tasks = [
            asyncio.create_task(collector.collect_data(sensor, config))
            for sensor, config in zip(sensors, sensors_config)
        ]
        processing_task = asyncio.create_task(
            collector.process_and_save_data(output_interval=60.0, batch_size=10)
        )

        tasks = collection_tasks + [processing_task]
        try:
            # Bucle de control para manejar comandos del usuario
            while True:
                command = input("> ").strip().lower()
                if command in ["start", "pause", "stop", "exit"]:
                    with open("control.json", "r") as f:
                        control = json.load(f)
                    if command == "start":
                        control["data_collector"] = "RUNNING"
                        control["publisher"] = "RUNNING"
                        logger.info("Starting both services")
                    elif command == "pause":
                        control["data_collector"] = "PAUSED"
                        control["publisher"] = "PAUSED"
                        logger.info("Pausing both services")
                    elif command == "stop":
                        control["data_collector"] = "STOPPED"
                        control["publisher"] = "STOPPED"
                        logger.info("Stopping both services")
                    elif command == "exit":
                        control["data_collector"] = "STOPPED"
                        control["publisher"] = "STOPPED"
                        logger.info("Exiting...")
                        break
                    with open("control.json", "w") as f:
                        json.dump(control, f, indent=4)
                else:
                    print(
                        "Unknown command. Available commands: start, pause, stop, exit"
                    )

                await asyncio.sleep(
                    0.1
                )  # Pequeña pausa para evitar consumo excesivo de CPU

            # Esperar a que las tareas terminen
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Tasks canceled")
        finally:
            await shutdown(collector, publisher_process)
            logger.info("Data collection system stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
