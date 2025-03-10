# Sistema de Monitoreo Ambiental

Este sistema permite la recolección de datos de sensores ambientales, su almacenamiento y publicación a servicios externos.

## Estructura del Proyecto

```shell
.
├── assets/            # Recursos gráficos (iconos, etc.)
├── drivers/           # Controladores para los sensores
├── logs/              # Archivos de registro
├── services/          # Servicios principales
│   ├── data_collector.py   # Recolector de datos
│   ├── publisher.py        # Publicador de datos CSV
│   └── winaqms_publisher.py # Publicador de datos WinAQMS
├── ui/                # Interfaz de usuario
│   ├── app.py              # Aplicación principal
│   ├── services_tab.py     # Pestaña de servicios
│   ├── measurements_tab.py # Pestaña de mediciones
│   └── logs_tab.py         # Pestaña de logs
├── config.json        # Configuración del sistema
├── control.json       # Control de estado de los servicios
├── main.py            # Punto de entrada principal (con consola)
├── main.pyw           # Punto de entrada sin consola
```

## Características

- **Recolección de Datos**: Recopila datos de sensores ambientales.
- **Almacenamiento**: Guarda los datos en archivos CSV.
- **Publicación**: Envía los datos a servicios externos.
- **Interfaz Gráfica**: Proporciona una interfaz con tres pestañas:
  - **Servicios**: Muestra y controla el estado de los servicios.
  - **Mediciones**: Muestra las últimas mediciones de los sensores.
  - **Logs**: Muestra los registros del sistema.
- **Ejecución en Segundo Plano**: La aplicación puede minimizarse a la bandeja del sistema y seguir ejecutándose.

## Requisitos

- Python 3.8+
- Pandas
- pystray
- Pillow
- python-dotenv
- requests
- tenacity

## Uso

### Ejecución con Consola

Para iniciar el sistema con una ventana de consola:

```bash
python main.py
```

### Ejecución sin Consola (Solo Interfaz Gráfica)

Para iniciar el sistema sin mostrar una ventana de consola:

```bash
pythonw main.pyw
```

O simplemente haga doble clic en el archivo `main.pyw` en el Explorador de Windows.


## Configuración

La configuración del sistema se realiza a través del archivo `config.json`. Este archivo contiene:

- Información de la estación
- Configuración de los sensores
- Parámetros de publicación