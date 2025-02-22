import os
import csv
import json
from datetime import datetime, timedelta


def get_last_hour():
    # Obtener la fecha y hora actual
    now = datetime.now()

    # Determinar la hora a procesar: si es 00:00, se procesa la última hora del día anterior.
    if now.hour == 0:
        process_date = now - timedelta(days=1)
        hour_to_process = 23
    else:
        process_date = now
        hour_to_process = now.hour - 1

    # Construir las cadenas de año, mes y día
    year_str = process_date.strftime("%Y")
    month_str = process_date.strftime("%m")
    day_str = process_date.strftime("%d")

    # Ruta de la carpeta y nombre del archivo .wad
    # La ruta es: c:/data/{yyyy}/{mm}/eco{yyyymmdd}.wad
    folder_path = os.path.join("c:/data", year_str, month_str)
    wad_file = f"eco{year_str}{month_str}{day_str}.wad"
    wad_path = os.path.join(folder_path, wad_file)

    # Verificar que el archivo existe
    if not os.path.exists(wad_path):
        print(f"El archivo {wad_path} no existe.")
        return

    # Definir el rango de tiempo a filtrar (hora a procesar)
    start_time = datetime.strptime(
        f"{process_date.strftime('%Y/%m/%d')} {hour_to_process:02d}:00:00",
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
                print("Error al convertir la fecha: {}".format(row["Date_Time"]))
                continue  # Si falla la conversión, se omite la fila

            # Filtrar filas que se encuentran en la hora a procesar
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
            avg = sum(values) / len(values)
            if sensor in ("C1", "C2", "C3", "C4"):
                avg = round(avg, 3)
            elif sensor == "C6":
                avg = round(avg)
            averages[sensor] = avg
        else:
            averages[sensor] = None

    # Estructurar el resultado en un diccionario
    result = {
        "FECHA": start_time.strftime("%Y-%m-%d"),
        "HORA": f"{hour_to_process:02d}",
        "NO": averages.get("C2"),
        "NO2": averages.get("C3"),
        "NOX": averages.get("C4"),
        "CO": averages.get("C1"),
        "O3": averages.get("C5"),
        "PM10": averages.get("C6"),
    }

    # Guardar el resultado en un archivo JSON
    output_file = f"promedio_{year_str}{month_str}{day_str}_{hour_to_process:02d}.json"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(base_dir, output_file)
    with open(output_path, "w") as jsonfile:
        json.dump(result, jsonfile, indent=4)

    print(f"Archivo JSON generado: {output_path}")


if __name__ == "__main__":
    get_last_hour()
