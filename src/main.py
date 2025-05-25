"""
Data Collection System

Main entry point for the data collection system with a PySide2 GUI to control
DataCollector and Publisher services using a control file.
"""
from PySide2.QtWidgets import QApplication, QSystemTrayIcon, QMenu, QAction
from PySide2.QtQml import QQmlApplicationEngine
from PySide2.QtCore import QUrl, Signal, QObject, Slot, Property
from PySide2.QtWidgets import QStyle
from qasync import QEventLoop
import sys
import os
import asyncio
import json
from typing import List, Dict

from core.services import DataCollector, WinAQMSPublisher, CSVPublisher
from core.models import StationConfig, DeviceConfig
from core.factories.device_factory import DeviceFactory
from core.utils.control import update_control_file, initialize_control_file
from core.utils.log_manager import LogManager
from core.utils.path_dir import CONFIG_DIR


class App(QObject):
    # Señales para QML
    serviceStateChanged = Signal(str, str)  # service_id, state
    deviceStateChanged = Signal(str, str)  # device_name, state
    minimizeToTrayRequested = Signal()
    logTextChanged = Signal()  # Señal para notificar cambios en el texto del log

    def __init__(self, loop: asyncio.AbstractEventLoop, logger):
        super().__init__()
        self._loop = loop
        self.logger = logger
        self.tray_icon = None
        self.window = None
        self._tasks = {}  # Diccionario de tareas: {task_id: {"task": task, "running": bool}}
        self._collector = None
        self._csv_publisher = None
        self._winaqms_publisher = None
        self._devices = None
        self._devices_config = None
        self._logText = ""  # Inicializar el texto del log

    def register_task(self, task_id: str, task: asyncio.Task):
        """Registra una nueva tarea en el sistema"""
        self._tasks[task_id] = {
            "task": task,
            "running": True
        }
        # Actualizar estado en control.json
        update_control_file(task_id, "RUNNING")
        # Emitir señal de cambio de estado
        self.serviceStateChanged.emit(task_id, "RUNNING")
        self.logger.info(f"Tarea {task_id} registrada")

    @Slot(str, result=str)
    def get_service_state(self, service_id: str) -> str:
        """Obtiene el estado de un servicio basado en sus tareas registradas"""
        if service_id == "data_collector":
            # Para data_collector, verificar si tiene tareas activas
            has_running_tasks = any(
                task_info["running"] 
                for task_id, task_info in self._tasks.items() 
                if task_id.startswith("data_collector_")
            )
            return "RUNNING" if has_running_tasks else "STOPPED"
        else:
            # Para otros servicios, verificar su tarea principal
            task_info = self._tasks.get(service_id)
            return "RUNNING" if task_info and task_info["running"] else "STOPPED"

    @Slot(str, result=str)
    def get_device_state(self, device_name: str) -> str:
        """Obtiene el estado actual de un dispositivo"""
        if self._collector:
            return "RUNNING" if self._collector.get_device_state(device_name) else "STOPPED"
        return "STOPPED"

    @Slot(str)
    def start_device(self, device_name: str):
        """Inicia la recolección de datos para un dispositivo específico"""
        if self._collector:
            self._loop.create_task(self._collector.start_device(device_name))
            self.deviceStateChanged.emit(device_name, "RUNNING")
            self.logger.info(f"Iniciando recolección para dispositivo {device_name}")

    @Slot(str)
    def stop_device(self, device_name: str):
        """Detiene la recolección de datos para un dispositivo específico"""
        if self._collector:
            self._loop.create_task(self._collector.stop_device(device_name))
            self.deviceStateChanged.emit(device_name, "STOPPED")
            self.logger.info(f"Deteniendo recolección para dispositivo {device_name}")

    @Slot(str)
    def start_task(self, task_id: str):
        """Inicia una tarea específica"""
        current_state = self.get_service_state(task_id)
        if current_state == "STOPPED":
            if task_id == "data_collector":
                # Iniciar el procesamiento de datos
                task = self._loop.create_task(self._collector.start_collection())
                self._tasks[f"{task_id}_processor"] = {
                    "task": task,
                    "running": True
                }
                # Registrar el servicio principal
                self.register_task(task_id, None)
            elif task_id == "csv_publisher":
                task = self._loop.create_task(self._csv_publisher.run())
                self.register_task(task_id, task)
            elif task_id == "winaqms_publisher":
                task = self._loop.create_task(self._winaqms_publisher.run())
                self.register_task(task_id, task)
            self.logger.info(f"Tarea {task_id} iniciada")

    @Slot(str)
    def stop_task(self, task_id: str):
        """Detiene una tarea específica"""
        if task_id in self._tasks or any(k.startswith(task_id) for k in self._tasks.keys()):
            # Detener todas las subtareas relacionadas
            tasks_to_stop = [k for k in self._tasks.keys() if k.startswith(task_id)]
            for task_key in tasks_to_stop:
                if self._tasks[task_key]["running"]:
                    task = self._tasks[task_key]["task"]
                    if task is not None:  # Only cancel if task exists
                        task.cancel()
                    self._tasks[task_key]["running"] = False
                    self.logger.info(f"Tarea {task_key} detenida")
            
            # Actualizar estado en control.json
            update_control_file(task_id, "STOPPED")
            # Emitir señal de cambio de estado
            self.serviceStateChanged.emit(task_id, "STOPPED")

    def set_services(self, collector, csv_publisher, winaqms_publisher, devices, devices_config):
        """Configura los servicios para ser utilizados por las tareas"""
        self._collector = collector
        self._csv_publisher = csv_publisher
        self._winaqms_publisher = winaqms_publisher
        self._devices = devices
        self._devices_config = devices_config

    @Slot()
    def minimizeToTray(self):
        if self.window and self.tray_icon:
            self.window.setProperty('visible', False)
            self.tray_icon.show()

    @Slot()
    def restoreFromTray(self):
        if self.window and self.tray_icon:
            self.window.setProperty('visible', True)


if __name__ == "__main__":
    try:   
        app = QApplication(sys.argv)
        loop = QEventLoop(app)       
        asyncio.set_event_loop(loop)
        
        # Crear el engine de QML
        engine = QQmlApplicationEngine()
        # Agregar la carpeta 'ui' como ruta de importación QML
        ui_dir = os.path.join(os.path.dirname(__file__), "ui")
        engine.addImportPath(ui_dir)

        # Initialize logging using LogManager
        log_manager = LogManager(log_file="main.log")
        logger = log_manager.logger
        initialize_control_file()
       
        # Cargar configuración de la estación
        with open(CONFIG_DIR / "station.json") as sf:
            station: StationConfig = json.load(sf)

        logger.info(
            f"Station: {station['name']} at {station['location']} "
            f"(Lat: {station['latitude']}, Lon: {station['longitude']}, "
            f"Elev: {station['elevation']} m)"
        )

        # Cargar configuración de los analizadores / meteo
        with open(CONFIG_DIR / "devices.json") as df:
            df_json = json.load(df)

        # Cargo los dispositivos configurados por el usuario
        devices_config: List[DeviceConfig] = df_json

        # Creo una instancia del factory de dispositivos
        device_factory = DeviceFactory.get_instance()
        
        # Instancio los dispositivos configurados por el usuario
        devices = [
            device_factory.create_device(
                cfg["name"],
                port=cfg["port"],
                logger=logger
            ) for cfg in devices_config
        ]

        # Configurar columnas
        columns = ["timestamp"]
        for device_config in devices_config:
            columns.extend(device_config["keys"])
    
        # Creo instancia del recolector de datos
        collector = DataCollector(columns=columns, logger=logger)
        
        # Agregar dispositivos al collector
        for device, config in zip(devices, devices_config):
            loop.create_task(collector.add_device(device, config))
        
        # Inicializar publishers
        csv_publisher = CSVPublisher()
        winaqms_publisher = WinAQMSPublisher()
    
        # Instanciar la aplicación
        app_logic = App(loop, logger)
        app_logic.set_services(collector, csv_publisher, winaqms_publisher, devices, devices_config)
        
        # Exponer la aplicación al QML
        engine.rootContext().setContextProperty("python", app_logic)

        # Cargar el QML principal desde la carpeta 'ui'
        qmlMainFile = os.path.join(ui_dir, "main.qml")
        engine.load(QUrl.fromLocalFile(qmlMainFile))

        if not engine.rootObjects():
            logger.error("No se pudo cargar el archivo QML principal.")
            sys.exit(-1)

        # Referencia a la ventana principal QML
        main_window = engine.rootObjects()[0]
        app_logic.window = main_window

        # Crear el icono de bandeja
        icon = app.style().standardIcon(QStyle.SP_ComputerIcon)
        tray_icon = QSystemTrayIcon(icon, app)
        tray_menu = QMenu()
        restore_action = QAction("Restaurar", tray_menu)
        quit_action = QAction("Salir", tray_menu)
        tray_menu.addAction(restore_action)
        tray_menu.addAction(quit_action)
        tray_icon.setContextMenu(tray_menu)
        tray_icon.setToolTip("Control de Loop Asíncrono")
        app_logic.tray_icon = tray_icon

        # Restaurar ventana desde el tray
        restore_action.triggered.connect(app_logic.restoreFromTray)
        tray_icon.activated.connect(lambda reason: app_logic.restoreFromTray() if reason == QSystemTrayIcon.Trigger else None)
        quit_action.triggered.connect(app.quit)

        # Conectar evento de cierre de ventana QML
        def handle_close(event):
            event.ignore()
            app_logic.minimizeToTray()
        main_window.closeEvent = handle_close

        with loop:
            loop.run_forever()

    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)