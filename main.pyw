"""
Data Collection System

Main entry point for the data collection system with a Tkinter GUI to control
DataCollector and Publisher services using a control file.
This version (.pyw) runs without showing a console window.
"""

import os
import asyncio
import json
import logging
import signal
import sys
import platform
import threading
from pathlib import Path
from typing import List, TypedDict

from services import (
    DataCollector,
    SensorConfig,
    CollectorState,
    Sensor,
    CSVPublisher,
    PublisherState,
    WinAQMSPublisher,
)
from drivers import DavisVantagePro2
from ui import create_app, run_app

# Crear la carpeta 'logs' si no existe
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Crear la carpeta 'assets' si no existe
assets_dir = "assets"
if not os.path.exists(assets_dir):
    os.makedirs(assets_dir)

# Configure logging (centralizado para todos los módulos)
logging.basicConfig(
    level=logging.INFO,  # Nivel INFO para eventos clave y errores
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "data_collection.log")),
    ],
)
logger = logging.getLogger("data_collection")  # Logger principal


class StationConfig(TypedDict):
    name: str
    location: str
    latitude: float
    longitude: float
    elevation: float


class ControlState(TypedDict):
    data_collector: str
    publisher: str
    winaqms_publisher: str


async def main() -> None:
    """Main entry point for the application with Tkinter GUI."""
    logger.info("Starting data collection system")

    try:
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

            # Iniciar publisher en un hilo separado
            publisher = CSVPublisher(logger=logger)  # Pasar el logger compartido
            publisher_thread = None

            # Iniciar winaqms_publisher en un hilo separado
            winaqms_publisher = WinAQMSPublisher(
                logger=logger
            )  # Pasar el logger compartido
            winaqms_publisher_thread = None

            # Escribir estado inicial en control.json
            with open("control.json", "w") as f:
                json.dump(
                    {
                        "data_collector": "RUNNING",
                        "publisher": "RUNNING",
                        "winaqms_publisher": "RUNNING",
                    },
                    f,
                    indent=4,
                )

            # Iniciar el hilo de publisher si está en estado RUNNING
            with open("control.json", "r") as f:
                control = json.load(f)
            if control.get("publisher", "STOPPED").upper() == "RUNNING":
                publisher_thread = threading.Thread(target=publisher.run)
                publisher_thread.daemon = True  # El hilo se detendrá al cerrar el programa
                publisher_thread.start()
                logger.info("Publisher started")

            # Iniciar el hilo de winaqms_publisher si está en estado RUNNING
            with open("control.json", "r") as f:
                control = json.load(f)
            if control.get("winaqms_publisher", "STOPPED").upper() == "RUNNING":
                winaqms_publisher_thread = threading.Thread(target=winaqms_publisher.run)
                winaqms_publisher_thread.daemon = (
                    True  # El hilo se detendrá al cerrar el programa
                )
                winaqms_publisher_thread.start()
                logger.info("WinAQMS Publisher started")

            # Manejo de señales para Windows
            if platform.system() == "Windows":

                async def windows_signal_handler():
                    try:
                        while collector.state != CollectorState.STOPPED:
                            await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        await shutdown(collector, publisher, winaqms_publisher)

                asyncio.create_task(windows_signal_handler())
            else:
                # Manejo de señales para Unix
                def handle_shutdown():
                    asyncio.create_task(shutdown(collector, publisher, winaqms_publisher))

                for sig in (signal.SIGINT, signal.SIGTERM):
                    signal.signal(sig, lambda signum, frame: handle_shutdown())
            
            # Tareas de colección
            collection_tasks = [
                asyncio.create_task(collector.collect_data(sensor, config))
                for sensor, config in zip(sensors, sensors_config)
            ]
            processing_task = asyncio.create_task(
                collector.process_and_save_data(output_interval=60.0, batch_size=10)
            )

            # Crear y correr la ventana de Tkinter usando módulo UI
            window = create_app(
                collector,
                publisher,
                winaqms_publisher,
                publisher_thread,
                collection_tasks + [processing_task],
            )
            
            # Ejecutar la aplicación
            ui_task = asyncio.create_task(
                run_app(
                    window,
                    collector,
                    publisher,
                    winaqms_publisher,
                    publisher_thread,
                    collection_tasks + [processing_task],
                )
            )

            try:
                # Esperar a que terminen las tareas
                await asyncio.gather(ui_task)
                
                # Cancelar las tareas de colección cuando la UI termina
                for task in collection_tasks + [processing_task]:
                    if not task.done():
                        task.cancel()
                
                # Esperar a que las tareas se cancelen
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Main task canceled")
            except Exception as e:
                logger.error(f"Unhandled exception in main: {e}", exc_info=True)
            finally:
                # Asegurarse de que los servicios se detengan correctamente
                await shutdown(collector, publisher, winaqms_publisher)
                logger.info("Data collection system stopped")
    except Exception as e:
        logger.error(f"Critical error in main: {e}", exc_info=True)
        sys.exit(1)


async def shutdown(
    collector: DataCollector,
    publisher: CSVPublisher,
    winaqms_publisher: WinAQMSPublisher,
) -> None:
    """Perform a graceful shutdown for both data collector and publisher."""
    logger.info("Shutting down services...")

    try:
        # 1. Update internal enum states
        if collector:
            collector.state = CollectorState.STOPPED

        if publisher:
            if hasattr(publisher, "state"):  # Check if publisher has state attribute
                publisher.state = PublisherState.STOPPED

        if winaqms_publisher:
            if hasattr(
                winaqms_publisher, "state"
            ):  # Check if winaqms_publisher has state attribute
                winaqms_publisher.state = PublisherState.STOPPED

        # 2. Update control.json for persistence and external control
        with open("control.json", "w") as f:
            json.dump(
                {
                    "data_collector": "STOPPED",
                    "publisher": "STOPPED",
                    "winaqms_publisher": "STOPPED",
                },
                f,
                indent=4,
            )

        # 3. Give time for tasks to finish gracefully
        await asyncio.sleep(1)

        logger.info("All services stopped")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1) 