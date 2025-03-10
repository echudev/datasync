@echo off
REM Script para iniciar el Sistema de Monitoreo Ambiental sin mostrar la consola

REM Verificar si existe el ícono, si no, crearlo
if not exist assets\icon.ico (
    echo Creando icono...
    python create_icon.py
)

REM Iniciar la aplicación sin mostrar la consola
start "" pythonw main.pyw

echo Sistema de Monitoreo iniciado en segundo plano.
echo Busque el icono en la bandeja del sistema.
timeout /t 5 