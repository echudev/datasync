"""
Data Collection System

Main entry point for the data collection system with a Tkinter GUI to control
DataCollector and Publisher services using a control file.
"""

import os
import asyncio
import aiofiles
import json
import logging
import sys
import signal
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
from utils.control import update_control_file, initialize_control_file

# Crear la carpeta 'logs' si no existe
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging (centralizado para todos los módulos)
logging.basicConfig(
    level=logging.INFO,  # Nivel INFO para eventos clave y errores
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, "data_collection.log")),
    ],
)
logger = logging.getLogger("data_collection")  # Logger principal

# Ruta del control.json usando Path
CONTROL_FILE = Path("c:\\datasync\\control.json")


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

    # Crear un evento de cierre
    shutdown_event = asyncio.Event()
    tasks = []

    # Inicializar control.json si no existe
    await initialize_control_file()

    # Configurar el manejador de señales de forma compatible con Windows
    def signal_handler(signum, frame):
        """Handle Ctrl+C signal"""
        logger.info("Ctrl+C detected, initiating shutdown...")
        # Usar set_result para evitar problemas con coroutines
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(shutdown_event.set)

    # Registrar el manejador de señales de forma compatible con Windows
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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
        sensors: List[Sensor] = [
            sensor_classes[cfg["name"]]() for cfg in sensors_config
        ]

        # Configurar columnas
        columns = ["timestamp"]
        for sensor_config in sensors_config:
            columns.extend(sensor_config["keys"])

        async with DataCollector(output_path=Path("data"), logger=logger) as collector:
            collector.set_columns(columns)

            # Inicializar publishers
            publisher = CSVPublisher(logger=logger)
            winaqms_publisher = WinAQMSPublisher(logger=logger)

            # Escribir estado inicial en control.json
            await update_control_file("data_collector", "RUNNING")
            await update_control_file("publisher", "RUNNING")
            await update_control_file("winaqms_publisher", "RUNNING")

            with open("control.json", "r") as f:
                control = json.load(f)

            # Crear todas las tareas de una vez
            tasks = []

            # Agregar tareas de recolección
            tasks.extend(
                [
                    asyncio.create_task(collector.collect_data(sensor, config))
                    for sensor, config in zip(sensors, sensors_config)
                ]
            )

            # Agregar tarea de procesamiento
            tasks.append(
                asyncio.create_task(
                    collector.process_and_save_data(output_interval=60.0, batch_size=10)
                )
            )

            # Agregar publishers si están activos
            if control.get("publisher", "STOPPED").upper() == "RUNNING":
                tasks.append(asyncio.create_task(publisher.run()))
                logger.info("Publisher started")

            if control.get("winaqms_publisher", "STOPPED").upper() == "RUNNING":
                tasks.append(asyncio.create_task(winaqms_publisher.run()))
                logger.info("WinAQMS Publisher started")

            # Agregar UI task
            window = create_app(collector, publisher, winaqms_publisher, shutdown_event)
            tasks.append(
                asyncio.create_task(
                    run_app(window, collector, publisher, winaqms_publisher)
                )
            )

            try:
                # Esperar a que se active el evento de cierre o terminen las tareas
                await shutdown_event.wait()

                # Cancelar todas las tareas
                for task in tasks:
                    if not task.done():
                        task.cancel()

                # Esperar a que todas las tareas se cancelen
                await asyncio.gather(*tasks, return_exceptions=True)

                # Asegurarse de que los servicios se detengan
                await shutdown(collector, publisher, winaqms_publisher)

            except asyncio.CancelledError:
                logger.info("Main task canceled")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}", exc_info=True)
            finally:
                # Último intento de detener servicios
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
            if hasattr(publisher, "state"):
                publisher.state = PublisherState.STOPPED

        if winaqms_publisher:
            if hasattr(winaqms_publisher, "state"):
                winaqms_publisher.state = PublisherState.STOPPED

        # 2. Update control.json for persistence and external control
        await update_control_file("data_collector", "STOPPED")
        await update_control_file("publisher", "STOPPED")
        await update_control_file("winaqms_publisher", "STOPPED")

        # 3. Give time for tasks to finish gracefully
        await asyncio.sleep(1)

        logger.info("All services stopped")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Asegurar que el loop se cierre correctamente
        try:
            loop.run_until_complete(main())
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected")
        finally:
            try:
                # Cancelar todas las tareas pendientes
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                # Esperar a que se completen las cancelaciones
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")
            finally:
                loop.close()

    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
    finally:
        sys.exit(0)
