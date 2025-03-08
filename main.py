"""
Data Collection System

Main entry point for the data collection system with a Tkinter GUI to control
DataCollector and Publisher services using a control file.
"""

import os
import asyncio
import json
import logging
import signal
import sys
import platform
import threading
import tkinter as tk
from tkinter import ttk
from pathlib import Path
from typing import List, TypedDict

from services import DataCollector, SensorConfig, CollectorState, Sensor, CSVPublisher
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


async def shutdown(collector: DataCollector, publisher: CSVPublisher) -> None:
    """Perform a graceful shutdown for both data collector and publisher."""
    logger.info("Shutting down...")
    if collector:
        collector.state = CollectorState.STOPPING
        await asyncio.sleep(1)  # Dar tiempo a que termine
    if publisher:
        # Actualizar control.json para detener el publisher
        with open("control.json", "w") as f:
            json.dump(
                {"data_collector": "STOPPED", "publisher": "STOPPED"}, f, indent=4
            )
        logger.info("Publisher stopped")


async def update_status(window: tk.Tk, status_labels: dict) -> None:
    """Update the status labels periodically by reading control.json."""
    while True:
        try:
            with open("control.json", "r") as f:
                control = json.load(f)
            status_labels["data_collector"].config(
                text=f"Data Collector: {control['data_collector']}"
            )
            status_labels["publisher"].config(text=f"Publisher: {control['publisher']}")
        except Exception as e:
            logger.error(f"Error updating status: {e}")
        await asyncio.sleep(2)  # Actualizar cada 2 segundos


async def run_gui(
    window: tk.Tk,
    collector: DataCollector,
    publisher: CSVPublisher,
    publisher_thread: threading.Thread,
    tasks: List[asyncio.Task],
) -> None:
    """Run the Tkinter GUI and handle events asynchronously."""

    def update_control(service: str, state: str) -> None:
        try:
            with open("control.json", "r") as f:
                control = json.load(f)
            control[service] = state
            with open("control.json", "w") as f:
                json.dump(control, f, indent=4)
            logger.info(f"Updated {service} state to {state}")
            if state == "STOPPED" and service == "data_collector":
                collector.state = CollectorState.STOPPING
                for task in tasks:
                    task.cancel()
            elif state == "STOPPED" and service == "publisher":
                # El hilo de publisher verificará el estado en control.json y se detendrá
                logger.info("Publisher stop requested via GUI")
        except Exception as e:
            logger.error(f"Error updating control file: {e}")

    # Configurar la interfaz
    tk.Label(
        window, text="Data Collection and Publishing System", font=("Arial", 14, "bold")
    ).pack(pady=10)

    # Data Collector
    tk.Label(window, text="Data Collector", font=("Arial", 12, "bold")).pack(pady=5)
    dc_frame = ttk.Frame(window)
    dc_frame.pack(pady=5)
    ttk.Button(
        dc_frame,
        text="Iniciar",
        command=lambda: update_control("data_collector", "RUNNING"),
    ).grid(row=0, column=0, padx=5)
    ttk.Button(
        dc_frame,
        text="Pausar",
        command=lambda: update_control("data_collector", "PAUSED"),
    ).grid(row=0, column=1, padx=5)
    ttk.Button(
        dc_frame,
        text="Detener",
        command=lambda: update_control("data_collector", "STOPPED"),
    ).grid(row=0, column=2, padx=5)
    dc_status = tk.Label(window, text="Data Collector: RUNNING", font=("Arial", 10))
    dc_status.pack(pady=5)

    # Publisher
    tk.Label(window, text="Publisher", font=("Arial", 12, "bold")).pack(pady=5)
    pub_frame = ttk.Frame(window)
    pub_frame.pack(pady=5)
    ttk.Button(
        pub_frame,
        text="Iniciar",
        command=lambda: update_control("publisher", "RUNNING"),
    ).grid(row=0, column=0, padx=5)
    ttk.Button(
        pub_frame, text="Pausar", command=lambda: update_control("publisher", "PAUSED")
    ).grid(row=0, column=1, padx=5)
    ttk.Button(
        pub_frame,
        text="Detener",
        command=lambda: update_control("publisher", "STOPPED"),
    ).grid(row=0, column=2, padx=5)
    pub_status = tk.Label(window, text="Publisher: RUNNING", font=("Arial", 10))
    pub_status.pack(pady=5)

    status_labels = {"data_collector": dc_status, "publisher": pub_status}

    # Tarea para actualizar el estado
    asyncio.create_task(update_status(window, status_labels))

    # Iniciar el bucle de eventos de Tkinter
    while True:
        if window.state() != "withdrawn":  # Verificar si la ventana está abierta
            window.update()
        await asyncio.sleep(0.01)  # Pequeña pausa para no bloquear el bucle


async def main() -> None:
    """Main entry point for the application with Tkinter GUI."""
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

        # Iniciar publisher en un hilo separado
        publisher = CSVPublisher()
        publisher_thread = None

        # Escribir estado inicial en control.json
        with open("control.json", "w") as f:
            json.dump(
                {"data_collector": "RUNNING", "publisher": "RUNNING"}, f, indent=4
            )

        # Iniciar el hilo de publisher si está en estado RUNNING
        with open("control.json", "r") as f:
            control = json.load(f)
        if control.get("publisher", "STOPPED").upper() == "RUNNING":
            publisher_thread = threading.Thread(target=publisher.run)
            publisher_thread.daemon = True  # El hilo se detendrá al cerrar el programa
            publisher_thread.start()
            logger.info("Publisher thread started.")

        # Manejo de señales para Windows
        if platform.system() == "Windows":

            async def windows_signal_handler():
                try:
                    while collector.state != CollectorState.STOPPING:
                        await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    asyncio.create_task(shutdown(collector, publisher))

            asyncio.create_task(windows_signal_handler())
        else:
            # Manejo de señales para Unix
            def handle_shutdown():
                asyncio.create_task(shutdown(collector, publisher))

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

        # Crear y correr la ventana de Tkinter
        window = tk.Tk()
        window.title("Data Collection and Publishing System")
        window.geometry("400x300")
        asyncio.create_task(
            run_gui(
                window,
                collector,
                publisher,
                publisher_thread,
                collection_tasks + [processing_task],
            )
        )

        try:
            await asyncio.gather(*collection_tasks, processing_task)
        except asyncio.CancelledError:
            logger.info("Tasks canceled")
        finally:
            await shutdown(collector, publisher)
            window.destroy()
            logger.info("Data collection system stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
