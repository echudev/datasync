import os
import csv
import json
import time
from datetime import datetime, timedelta
import schedule  # Asegúrate de tener instalada la librería schedule (pip install schedule)


def process_hourly_data():
    # Obtener la fecha y hora actual
    now = datetime.now()

    # Determinar la hora a procesar:
    # Si es medianoche, se procesa la última hora del día anterior.
    if now.hour == 0:
        process_date = now - timedelta(days=1)
        hour_to_process = 23
    else:
        process_date = now
        hour_to_process = now.hour - 1

    # Construir cadenas de año, mes y día
    year_str = process_date.strftime("%Y")
    month_str = process_date.strftime("%m")
    day_str = process_date.strftime("%d")

    # Ruta de la carpeta y nombre del archivo .wad
    # Formato: c:/data/{yyyy}/{mm}/eco{yyyymmdd}.wad
    folder_path = os.path.join("c:/data", year_str, month_str)
    wad_file = "eco{}{}{}.wad".format(year_str, month_str, day_str)
    wad_path = os.path.join(folder_path, wad_file)

    # Verificar que el archivo existe
    if not os.path.exists(wad_path):
        print("El archivo {} no existe.".format(wad_path))
        return

    # Definir el rango de tiempo a filtrar (la hora a procesar)
    # Ejemplo: "2025/02/01 00:00:00" hasta "2025/02/01 00:59:59"
    start_time = datetime.strptime(
        "{} {:02d}:00:00".format(process_date.strftime("%Y/%m/%d"), hour_to_process),
        "%Y/%m/%d %H:%M:%S",
    )
    end_time = start_time + timedelta(hours=1)

    # Lista de sensores a procesar
    sensors = ["C1", "C2", "C3", "C4", "C5", "C6"]
    # Diccionario para acumular los datos por sensor
    sensor_data = {sensor: [] for sensor in sensors}

    # Leer el archivo CSV (formato .wad)
    with open(wad_path, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convertir la columna Date_Time a objeto datetime
            try:
                dt = datetime.strptime(row["Date_Time"], "%Y/%m/%d %H:%M:%S")
            except Exception:
                continue  # Omitir fila si falla la conversión

            # Filtrar las filas que se encuentran en la hora a procesar
            if start_time <= dt < end_time:
                for sensor in sensors:
                    val_str = row.get(sensor, "").strip()
                    if val_str != "":
                        try:
                            value = float(val_str)
                            sensor_data[sensor].append(value)
                        except ValueError:
                            continue

    # Calcular el promedio de cada sensor (se espera que sean 60 datos por sensor)
    averages = {}
    for sensor, values in sensor_data.items():
        if values:
            averages[sensor] = sum(values) / len(values)
        else:
            averages[sensor] = None  # O se puede asignar 0.0 según se requiera

    # Estructurar el resultado en un diccionario
    result = {
        "fecha": start_time.strftime("%Y-%m-%d"),
        "hora": "{:02d}".format(hour_to_process),
        "promedios": averages,
    }

    # Guardar el resultado en un archivo JSON
    output_file = "promedio_{}{}{}_{}.json".format(
        year_str, month_str, day_str, "{:02d}".format(hour_to_process)
    )
    output_path = os.path.join(folder_path, output_file)
    with open(output_path, "w") as jsonfile:
        json.dump(result, jsonfile, indent=4)

    print("Archivo JSON generado: {}".format(output_path))


def main():
    # Programar la tarea para que se ejecute al comienzo de cada hora.
    # schedule.every().hour.at(":00") ejecuta la tarea en el minuto 0 de cada hora.
    schedule.every().hour.at(":00").do(process_hourly_data)
    print("Scheduler iniciado. Esperando la ejecución programada...")

    # Bucle infinito para mantener la ejecución del scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
