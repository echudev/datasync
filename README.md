# Proyecto datasync

Datalogger de calidad ambiental

## Estructura del Proyecto

```
datasync/
├── config/                         # Configuración general
│   ├── control.json
│   ├── devices.json
│   └── station.json
├── data/                           # Carpeta para datos generados o utilizados
├── logs/                           # Archivos de logs
├── src/                            # Código fuente principal
│   ├── core/
│   │   ├── device_adapters/        # Interacción hardware (TCP, serial, CRC, parseo)
│   │   ├── models/                 # Modelos de datos (device.py, station.py, etc.)
│   │   ├── services/               # Servicios principales (data_collector, publisher, etc.)
│   │   └── utils/                  # Utilidades y helpers
│   ├── infrastructure/
│   │   ├── persistence/            # Acceso y gestión de bases de datos (csv.py, influx.py)
│   │   └── repositories/           # Repositorios de datos
│   ├── main.py                     # Punto de entrada principal
│   └── ui/                         # Interfaz de usuario (QML)
│       ├── Theme/
│       ├── main.qml
│       └── views/                  # Vistas: CronJobsView, DevicesView, LogsView, MeasuresView
├── tests/                          # Pruebas unitarias
│   ├── test_data_collector.py
│   ├── test_publisher.py
│   └── test_winaqms_publisher.py
```

## Dependencias principales
- Python >= 3.8
- aiocsv, aiofiles, aiohttp
- pandas
- pyserial
- pyside2 (interfaz gráfica)
- qasync (integración Qt + asyncio)
- pytest (pruebas)
- python-dotenv (variables de entorno)
- requests

## Uso rápido

1. Instalar dependencias:
   ```sh
   pip install -r requirements.txt
   ```
2. Configurar variables en `.env` y archivos en `config/`.
3. Ejecutar la aplicación:
   ```sh
   python src/main.py
   ```

## Organización del código
- **src/core/**: Lógica principal, integración con dispositivos, modelos y servicios.
- **src/infrastructure/**: Acceso a datos y bases de datos.
- **src/ui/**: Interfaz gráfica en QML.
- **tests/**: Pruebas unitarias.
- **config/**: Configuración de dispositivos y estaciones.

---

Para más detalles, consulta los archivos y carpetas correspondientes o contacta al equipo de desarrollo.
