"""
Main UI Application Module

This module contains the main application class and functions to create and run the UI.
"""

import os
import asyncio
import json
import logging
import tkinter as tk
from tkinter import ttk, scrolledtext
import glob
import pandas as pd
from datetime import datetime
import pystray
from PIL import Image
import time
import threading

from services import (
    CollectorState,
    PublisherState,
)

from .services_tab import create_services_tab
from .measurements_tab import create_measurements_tab
from .logs_tab import create_logs_tab

# Obtener el logger
logger = logging.getLogger("data_collection")

# Variable global para el ícono de la bandeja del sistema
tray_icon = None

# Global flags for actions
SHOW_WINDOW_FLAG = False
EXIT_APP_FLAG = False


def create_app(collector, publisher, winaqms_publisher, publisher_thread, tasks):
    """
    Create the main application window and setup the UI.
    
    Args:
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
        publisher_thread: The publisher thread
        tasks: The collection tasks
        
    Returns:
        The main window instance
    """
    # Crear la ventana principal
    window = tk.Tk()
    window.title("Sistema de Monitoreo Ambiental")
    window.geometry("800x600")
    
    # Configurar el ícono de la aplicación (si existe)
    try:
        icon_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", "icon.ico")
        if os.path.exists(icon_path):
            window.iconbitmap(icon_path)
    except Exception as e:
        logger.error(f"Error loading icon: {e}")
    
    # Crear un notebook (pestañas)
    notebook = ttk.Notebook(window)
    notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
    
    # Crear las pestañas
    services_tab, services_frame = create_services_tab(notebook, collector, publisher, winaqms_publisher)
    measurements_tab, measurements_frame = create_measurements_tab(notebook)
    logs_tab, logs_text = create_logs_tab(notebook)
    
    # Almacenar referencias a los frames y widgets importantes
    window.services_frame = services_frame
    window.measurements_frame = measurements_frame
    window.logs_text = logs_text
    
    # Agregar las pestañas al notebook
    notebook.add(services_tab, text="Servicios")
    notebook.add(measurements_tab, text="Mediciones")
    notebook.add(logs_tab, text="Logs")
    
    # Función para salir de la aplicación
    def exit_application():
        logger.info("Exit button clicked, shutting down application")
        
        # Detener el ícono si existe
        global tray_icon
        if tray_icon:
            try:
                tray_icon.stop()
            except Exception as e:
                logger.error(f"Error stopping tray icon: {e}")
        
        # Detener los servicios
        try:
            asyncio.run(shutdown(collector, publisher, winaqms_publisher))
        except Exception as e:
            logger.error(f"Error shutting down services: {e}")
        
        # Cerrar la ventana
        window.quit()
    
    # Crear un frame para el botón de salir
    button_frame = ttk.Frame(window)
    button_frame.pack(pady=5, fill=tk.X)
    
    # Agregar botón de salir
    exit_button = ttk.Button(
        button_frame,
        text="Salir",
        command=exit_application
    )
    exit_button.pack(side=tk.RIGHT, padx=10, pady=5)
    
    # Configurar el comportamiento al cerrar la ventana
    def on_closing():
        logger.info("Window closing, minimizing to tray")
        # Minimizar la ventana primero
        window.withdraw()
        
        # Luego crear el ícono (esto es importante para evitar problemas de orden)
        global tray_icon
        if tray_icon is None:
            logger.info("Creating tray icon")
            time.sleep(0.2)  # Pequeña pausa para asegurar que la ventana esté oculta
            tray_icon = create_tray_icon(window, collector, publisher, winaqms_publisher)
            if tray_icon is None:
                logger.error("Failed to create tray icon, showing window again")
                window.deiconify()
                window.lift()
    
    window.protocol("WM_DELETE_WINDOW", on_closing)
    
    # Schedule the check for tray icon flags
    window.after(100, lambda: check_tray_flags(window))
    
    return window


def create_tray_icon(window, collector, publisher, winaqms_publisher):
    """
    Create a system tray icon for the application.
    
    Args:
        window: The main window instance
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
    """
    # Global variables
    global tray_icon, SHOW_WINDOW_FLAG, EXIT_APP_FLAG
    
    # Detener el ícono existente si hay uno
    if tray_icon is not None:
        try:
            logger.info("Stopping existing tray icon")
            tray_icon.stop()
            tray_icon = None
            time.sleep(1.0)
        except Exception as e:
            logger.error(f"Error stopping existing tray icon: {e}")
            tray_icon = None
    
    # Simplified show_window function that sets a flag
    def show_window(icon, item):
        logger.info("Show window flag set")
        global SHOW_WINDOW_FLAG
        SHOW_WINDOW_FLAG = True
    
    # Simplified exit_app function that sets a flag
    def exit_app(icon, item):
        logger.info("Exit app flag set")
        global EXIT_APP_FLAG
        EXIT_APP_FLAG = True
        # Force application to exit if needed
        try:
            icon.stop()
        except Exception as e:
            logger.error(f"Error stopping tray icon: {e}")
        # Use os._exit as a last resort if window.quit doesn't work
        os._exit(0)
    
    # Menu setup
    menu = (
        pystray.MenuItem('Mostrar', show_window),
        pystray.MenuItem('Salir', exit_app)
    )
    
    try:
        # Create the icon
        icon = pystray.Icon("DSyncIcon")
        icon.menu = pystray.Menu(*menu)
        
        # Set the icon image
        current_dir = os.path.dirname(os.path.abspath(__file__))
        icon_path = os.path.join(current_dir, "..", "assets", "icon.ico")
        if os.path.exists(icon_path):
            icon.icon = Image.open(icon_path)
        else:
            logger.warning(f"Icon file not found at {icon_path}")
            # Create a simple default icon
            image = Image.new('RGB', (64, 64), color = (0, 128, 0))
            icon.icon = image
        
        # Store the icon reference
        tray_icon = icon
        
        # Run the icon in a thread
        def run_icon():
            logger.info("Starting tray icon")
            icon.run()
            logger.info("Tray icon stopped")
        
        # Start the icon thread
        threading.Thread(target=run_icon, daemon=True).start()
        
        return icon
    except Exception as e:
        logger.error(f"Error creating tray icon: {e}")
        return None


async def run_app(window, collector, publisher, winaqms_publisher, publisher_thread, tasks):
    """
    Run the application main loop.
    
    Args:
        window: The main window instance
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
        publisher_thread: The publisher thread
        tasks: The collection tasks
    """
    # Iniciar el bucle de eventos de Tkinter
    running = True
    
    # Iniciar la actualización de UI solo una vez
    ui_update_task = asyncio.create_task(
        update_ui(
            window, 
            window.services_frame, 
            window.measurements_frame, 
            window.logs_text,
            collector,
            publisher,
            winaqms_publisher
        )
    )
    
    try:
        while running:
            try:
                window.update()
                await asyncio.sleep(0.01)  # Pequeña pausa para no bloquear el bucle
            except tk.TclError as e:
                if "application has been destroyed" in str(e):
                    # La ventana ha sido destruida, salir del bucle
                    running = False
                    logger.info("Window has been destroyed, exiting UI loop")
                    break
                else:
                    # Otro error de Tcl, registrar pero continuar
                    logger.warning(f"Tcl error in UI loop: {e}")
            except Exception as e:
                logger.error(f"Error in UI loop: {e}")
                await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        logger.info("UI task cancelled")
    finally:
        # Cancelar la tarea de actualización de UI
        if ui_update_task and not ui_update_task.done():
            ui_update_task.cancel()
        
        # Detener el ícono de la bandeja si existe
        global tray_icon
        if tray_icon:
            try:
                # Solo intentar detener si el ícono existe
                if hasattr(tray_icon, 'stop'):
                    tray_icon.stop()
            except Exception as e:
                logger.error(f"Error stopping tray icon: {e}")
        
        logger.info("UI loop ended")


async def shutdown(collector, publisher, winaqms_publisher):
    """
    Perform a graceful shutdown of all services.
    
    Args:
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
    """
    logger.info("Shutting down services...")

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


async def update_ui(
    window: tk.Tk, 
    services_frame: ttk.Frame, 
    measurements_frame: ttk.Frame, 
    logs_text: scrolledtext.ScrolledText,
    collector,
    publisher,
    winaqms_publisher
) -> None:
    """
    Update the UI elements periodically.
    
    Args:
        window: The main window instance
        services_frame: The services tab frame
        measurements_frame: The measurements tab frame
        logs_text: The logs text widget
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
    """
    # Crear los widgets de servicios solo una vez
    service_labels = {}
    service_indicators = {}
    
    # Limpiar el frame de servicios para evitar duplicados
    for widget in services_frame.winfo_children():
        widget.destroy()
    
    # Crear widgets para servicios
    for service in ["data_collector", "publisher", "winaqms_publisher"]:
        try:
            service_frame = ttk.Frame(services_frame)
            service_frame.pack(pady=5, fill=tk.X)
            
            # Indicador visual (círculo de color)
            indicator = tk.Canvas(service_frame, width=20, height=20)
            indicator.grid(row=0, column=0, padx=5)
            indicator.create_oval(5, 5, 15, 15, fill="gray", tags="indicator")
            service_indicators[service] = indicator
            
            # Etiqueta de estado
            label = ttk.Label(service_frame, text=f"{service.replace('_', ' ').title()}: Unknown")
            label.grid(row=0, column=1, sticky=tk.W)
            service_labels[service] = label
            
            # Botones de control
            ttk.Button(
                service_frame,
                text="Iniciar",
                command=lambda s=service: update_control(s, "RUNNING", collector, publisher, winaqms_publisher),
            ).grid(row=0, column=2, padx=5)
            
            ttk.Button(
                service_frame,
                text="Detener",
                command=lambda s=service: update_control(s, "STOPPED", collector, publisher, winaqms_publisher),
            ).grid(row=0, column=3, padx=5)
        except Exception as e:
            logger.error(f"Error creating service controls: {e}")
    
    # Crear widgets para mediciones
    wad_data_frame = ttk.LabelFrame(measurements_frame, text="Datos WAD (WinAQMS)")
    wad_data_frame.pack(pady=5, fill=tk.BOTH, expand=True)
    
    csv_data_frame = ttk.LabelFrame(measurements_frame, text="Datos CSV")
    csv_data_frame.pack(pady=5, fill=tk.BOTH, expand=True)
    
    # Crear tabla para datos WAD
    wad_tree = ttk.Treeview(wad_data_frame, columns=("sensor", "value", "unit", "timestamp"), show="headings")
    wad_tree.heading("sensor", text="Sensor")
    wad_tree.heading("value", text="Valor")
    wad_tree.heading("unit", text="Unidad")
    wad_tree.heading("timestamp", text="Timestamp")
    wad_tree.pack(fill=tk.BOTH, expand=True)
    
    # Crear tabla para datos CSV
    csv_tree = ttk.Treeview(csv_data_frame, columns=("sensor", "value", "unit", "timestamp"), show="headings")
    csv_tree.heading("sensor", text="Sensor")
    csv_tree.heading("value", text="Valor")
    csv_tree.heading("unit", text="Unidad")
    csv_tree.heading("timestamp", text="Timestamp")
    csv_tree.pack(fill=tk.BOTH, expand=True)
    
    # Solo actualizar la UI, no crear nuevos widgets en cada iteración
    while True:
        try:
            # Verificar si la ventana todavía existe
            if not window.winfo_exists():
                logger.info("Window no longer exists, stopping UI updates")
                break
                
            # Verificar si la ventana está visible
            if window.state() == 'withdrawn':
                # Si la ventana está minimizada, no actualizar la UI
                await asyncio.sleep(2)
                continue
            
            # Actualizar estado de servicios
            try:
                with open("control.json", "r") as f:
                    control = json.load(f)
                
                for service, label in service_labels.items():
                    try:
                        if label.winfo_exists():
                            state = control.get(service, "UNKNOWN")
                            label.config(text=f"{service.replace('_', ' ').title()}: {state}")
                            
                            # Actualizar indicador visual
                            indicator = service_indicators[service]
                            if indicator.winfo_exists():
                                color = "green" if state == "RUNNING" else "red" if state == "STOPPED" else "gray"
                                indicator.itemconfig("indicator", fill=color)
                    except tk.TclError:
                        pass  # Ignorar errores si el widget ya no existe
            except Exception as e:
                logger.error(f"Error reading control file: {e}")
            
            # Actualizar datos de mediciones (WAD)
            try:
                if wad_tree.winfo_exists():
                    # Limpiar tabla
                    for item in wad_tree.get_children():
                        wad_tree.delete(item)
                    
                    # Buscar el archivo WAD más reciente
                    wad_files = glob.glob("C:\\Data\\*.wad")
                    if wad_files:
                        latest_wad = max(wad_files, key=os.path.getmtime)
                        try:
                            # Leer el archivo WAD como si fuera un CSV
                            wad_df = pd.read_csv(latest_wad)
                            if not wad_df.empty:
                                last_row = wad_df.iloc[-1]
                                timestamp = last_row.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M"))
                                
                                # Mostrar cada columna como un sensor separado
                                for col in wad_df.columns:
                                    if col != 'timestamp':
                                        value = last_row.get(col, "N/A")
                                        unit = "µg/m³" if "PM" in col else "ppb" if col in ["O3", "NO2", "SO2"] else "ppm" if col == "CO" else "N/A"
                                        wad_tree.insert("", "end", values=(col, value, unit, timestamp))
                        except Exception as e:
                            logger.error(f"Error reading WAD file: {e}")
            except tk.TclError:
                pass  # Ignorar errores si el widget ya no existe
            except Exception as e:
                logger.error(f"Error updating WAD data: {e}")
            
            # Actualizar datos de mediciones (CSV)
            try:
                if csv_tree.winfo_exists():
                    # Limpiar tabla
                    for item in csv_tree.get_children():
                        csv_tree.delete(item)
                    
                    # Buscar el archivo CSV más reciente
                    csv_files = glob.glob("data/*.csv")
                    if csv_files:
                        latest_csv = max(csv_files, key=os.path.getmtime)
                        try:
                            df = pd.read_csv(latest_csv)
                            if not df.empty:
                                last_row = df.iloc[-1]
                                timestamp = last_row.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M"))
                                
                                # Mostrar cada columna como un sensor separado
                                for col in df.columns:
                                    if col != 'timestamp':
                                        value = last_row.get(col, "N/A")
                                        csv_tree.insert("", "end", values=(col, value, "N/A", timestamp))
                        except Exception as e:
                            logger.error(f"Error reading CSV file: {e}")
            except tk.TclError:
                pass  # Ignorar errores si el widget ya no existe
            except Exception as e:
                logger.error(f"Error updating CSV data: {e}")
            
            # Actualizar logs
            try:
                if logs_text.winfo_exists():
                    log_dir = "logs"
                    log_file = os.path.join(log_dir, "data_collection.log")
                    if os.path.exists(log_file):
                        with open(log_file, "r") as f:
                            logs_content = f.read()
                            logs_text.delete(1.0, tk.END)
                            logs_text.insert(tk.END, logs_content)
                            logs_text.see(tk.END)  # Desplazar al final
            except tk.TclError:
                pass  # Ignorar errores si el widget ya no existe
            except Exception as e:
                logger.error(f"Error updating logs: {e}")
                
        except tk.TclError as e:
            logger.warning(f"TclError in update_ui: {e}")
        except Exception as e:
            logger.error(f"Error updating UI: {e}")
            
        await asyncio.sleep(2)  # Actualizar cada 2 segundos


def update_control(service, state, collector, publisher, winaqms_publisher):
    """
    Update the control state of a service.
    
    Args:
        service: The service name
        state: The new state
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
    """
    try:
        # Update control.json
        with open("control.json", "r") as f:
            control = json.load(f)
        control[service] = state
        with open("control.json", "w") as f:
            json.dump(control, f, indent=4)

        logger.info(f"{service.capitalize()} state updated to {state}")

        # Update internal enum states for consistent state management
        if service == "data_collector":
            if state == "STOPPED":
                collector.state = CollectorState.STOPPED
            elif state == "RUNNING":
                collector.state = CollectorState.RUNNING

        elif service == "publisher" and publisher:
            if state == "STOPPED":
                if hasattr(publisher, "state"):
                    publisher.state = PublisherState.STOPPED
                logger.info("Publisher stop requested via GUI")
            elif state == "RUNNING":
                if hasattr(publisher, "state"):
                    publisher.state = PublisherState.RUNNING
                    
        elif service == "winaqms_publisher" and winaqms_publisher:
            if state == "STOPPED":
                if hasattr(winaqms_publisher, "state"):
                    winaqms_publisher.state = PublisherState.STOPPED
                logger.info("WinAQMS Publisher stop requested via GUI")
            elif state == "RUNNING":
                if hasattr(winaqms_publisher, "state"):
                    winaqms_publisher.state = PublisherState.RUNNING

    except Exception as e:
        logger.error(f"Error updating control file for {service}: {e}") 

# Function to check flags and perform actions in the main thread
def check_tray_flags(window):
    global SHOW_WINDOW_FLAG, EXIT_APP_FLAG
    
    # Check if show window flag is set
    if SHOW_WINDOW_FLAG:
        logger.info("Processing show window flag")
        try:
            window.deiconify()
            window.lift()
            window.focus_force()
        except Exception as e:
            logger.error(f"Error showing window: {e}")
        SHOW_WINDOW_FLAG = False
    
    # Check if exit app flag is set
    if EXIT_APP_FLAG:
        logger.info("Processing exit app flag")
        try:
            window.quit()
        except Exception as e:
            logger.error(f"Error quitting application: {e}")
    
    # Schedule next check
    if not EXIT_APP_FLAG:  # Don't schedule if we're exiting
        window.after(100, lambda: check_tray_flags(window)) 