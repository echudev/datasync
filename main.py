"""
Data Collection System

Main entry point for the data collection system, coordinating sensor data
collection and graceful shutdown.
"""

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from typing import List, TypedDict

from services import (
    DataCollector,
    SensorConfig,
    CollectorState,
)

from drivers import (
    DavisVantagePro2,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("data_collection.log")],
)
logger = logging.getLogger("data_collector")


# Definir tipo para datos de la estación (opcional, para tipado estricto)
class StationConfig(TypedDict):
    name: str
    location: str
    latitude: float
    longitude: float
    elevation: float


async def shutdown(collector: DataCollector) -> None:
    """Perform a graceful shutdown."""
    logger.info("Shutting down...")
    collector.state = CollectorState.STOPPING
    await asyncio.sleep(1)


def signal_handler(collector: DataCollector, loop: asyncio.AbstractEventLoop) -> None:
    """Handle OS signals for graceful shutdown."""
    logger.info("Received termination signal")
    asyncio.create_task(shutdown(collector))
    loop.call_later(2, loop.stop)


async def main() -> None:
    """Main entry point for the application."""
    logger.info("Starting data collection system")

    # Cargar configuración desde archivo
    with open("config.json") as f:
        config = json.load(f)

    # Extraer datos de la estación
    station_config: StationConfig = config["station"]
    logger.info(
        f"Station: {station_config['name']} at {station_config['location']} "
        f"(Lat: {station_config['latitude']}, Lon: {station_config['longitude']}, "
        f"Elev: {station_config['elevation']} m)"
    )

    # Extraer configuración de sensores
    sensors_config: List[SensorConfig] = config["sensors"]

    # Mapear nombres de sensores a sus clases
    sensor_classes = {
        "davisvp2": DavisVantagePro2,
    }

    # Crear instancias de sensores
    sensors = [sensor_classes[cfg["name"]]() for cfg in sensors_config]

    # Configurar columnas
    columns = ["timestamp"]
    for sensor_config in sensors_config:
        columns.extend(sensor_config["keys"])

    # Usar DataCollector como gestor de contexto
    async with DataCollector(output_path=Path("data"), logger=logger) as collector:
        collector.set_columns(columns)

        # Crear tareas
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: signal_handler(collector, loop))

        collection_tasks = [
            asyncio.create_task(collector.collect_data(sensor, config))
            for sensor, config in zip(sensors, sensors_config)
        ]
        processing_task = asyncio.create_task(
            collector.process_and_save_data(output_interval=60.0, batch_size=10)
        )

        tasks = collection_tasks + [processing_task]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Tasks canceled")
        finally:
            logger.info("Data collection system stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
