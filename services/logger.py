# logger.py
import csv
import os
from datetime import datetime, timedelta


class WeatherLogger:
    def __init__(self, filename="data/minutely_data.csv"):
        self.filename = filename
        os.makedirs(
            os.path.dirname(self.filename), exist_ok=True
        )  # Crear carpeta /data si no existe

    def log_data(self, data):
        """Almacena un diccionario de datos en el CSV."""
        if not data:
            print("No hay datos para guardar")
            return

        keys = data.keys()
        with open(self.filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        print(f"Datos guardados en {self.filename}")

    def get_last_hour_average(self):
        """Calcula el promedio horario de los últimos 60 minutos desde el CSV."""
        try:
            with open(self.filename, "r", newline="") as csvfile:
                reader = list(csv.DictReader(csvfile))  # Leer todas las filas
                if not reader:
                    print("No hay datos en el CSV")
                    return None

                # Convertir timestamps a datetime y filtrar últimos 60 minutos
                now = datetime.now()
                one_hour_ago = now - timedelta(minutes=60)
                recent_data = []
                for row in reader:
                    row_time = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
                    if row_time >= one_hour_ago:
                        recent_data.append(
                            {
                                k: float(v) if k != "timestamp" else v
                                for k, v in row.items()
                            }
                        )

                if not recent_data:
                    print("No hay datos suficientes en la última hora")
                    return None

                # Calcular promedios
                avg_data = {
                    "timestamp": f"{now.strftime('%Y-%m-%d %H:%M:%S')} (hourly avg)"
                }
                for key in recent_data[0].keys():
                    if key != "timestamp":
                        avg_data[key] = sum(d[key] for d in recent_data) / len(
                            recent_data
                        )
                return avg_data
        except Exception as e:
            print(f"Error al leer CSV: {e}")
            return None
