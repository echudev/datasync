# main.py
from drivers.davis_vantage_pro2 import DavisVantagePro2
from services import WeatherLogger
import time


def collect_minutely_data(duration_seconds=600):
    station = DavisVantagePro2(port="COM4")
    logger = WeatherLogger(filename="data/minutely_data.csv")

    with station:
        print("Iniciando recolección de datos minutales...")
        for data in station.get_loop_data(
            interval_seconds=60, duration_seconds=duration_seconds
        ):
            logger.log_data(data)


def get_last_hour():
    station = DavisVantagePro2(port="COM4")
    with station:
        hourly_data = station.get_last_hour()
        print("Último promedio horario:", hourly_data)
        return hourly_data


if __name__ == "__main__":
    # Recolectar datos minutales (opcional)
    collect_minutely_data(duration_seconds=600)
    time.sleep(2)  # Esperar a que se guarden los datos
