import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from services import CSVPublisher, PublisherState

# Para evitar problemas en Windows, establecemos la política de event loop adecuada.
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# -------------------------------
# Fixture para la configuración de entorno
# -------------------------------
@pytest.fixture
def env_setup(monkeypatch, tmp_path):
    """
    Configura las variables de entorno necesarias para inicializar CSVPublisher
    y usa un directorio temporal para la lectura de CSV.
    """
    monkeypatch.setenv("GOOGLE_POST_URL", "http://dummyendpoint.com/api")
    monkeypatch.setenv("ORIGEN", "dummy_origin")
    monkeypatch.setenv("API_KEY", "dummy_api_key")
    return str(tmp_path / "data")


# -------------------------------
# Fixture asíncrona para instanciar CSVPublisher
# -------------------------------
@pytest.fixture
async def publisher_instance(env_setup):
    """
    Crea una instancia asíncrona de CSVPublisher usando el directorio de datos temporal
    y un logger de prueba. Al ser una fixture asíncrona, se asegura que se crea
    dentro de un event loop activo.
    """
    test_logger = logging.getLogger("test_logger")
    return CSVPublisher(csv_dir=env_setup, logger=test_logger)


# -------------------------------
# Test para _build_csv_path
# -------------------------------
@pytest.mark.asyncio
async def test_build_csv_path(publisher_instance):
    path = publisher_instance._build_csv_path("2022", "1", "5")
    expected = os.path.join(publisher_instance.csv_dir, "2022", "01", "05.csv").replace(
        "\\", "/"
    )
    assert path == expected, "La ruta generada no coincide con la esperada."


# -------------------------------
# Test para update_state y get_state
# -------------------------------
@pytest.mark.asyncio
async def test_update_and_get_state(publisher_instance):
    await publisher_instance.update_state("STOPPED")
    state = await publisher_instance.get_state()
    assert state == PublisherState.STOPPED, "El estado debería haber pasado a STOPPED."

    await publisher_instance.update_state("RUNNING")
    state = await publisher_instance.get_state()
    assert state == PublisherState.RUNNING, "El estado debería haber pasado a RUNNING."


# -------------------------------
# Test para _calculate_hourly_averages
# -------------------------------
@pytest.mark.asyncio
async def test_calculate_hourly_averages(publisher_instance):
    target_hour = datetime(2022, 1, 1, 10, 0, 0)
    data = {
        "timestamp": [
            target_hour + timedelta(minutes=10),
            target_hour + timedelta(minutes=20),
        ],
        "Temperature": [20, 22],
        "Humidity": [50, 55],
        "Pressure": [1013, 1015],
        "WindSpeed": [5, 6],
        "WindDirection": [180, 190],
        "RainRate": [0, 0],
        "UV": [0.3, 0.4],
        "SolarRadiation": [200, 210],
    }
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    result = publisher_instance._calculate_hourly_averages(df, target_hour)
    assert result is not None, (
        "El resultado no debe ser None cuando hay datos en la hora."
    )
    assert result["TEMP"] == 21.0
    assert result["HR"] == 52.5
    assert result["PA"] == 1014.0
    assert result["VV"] == 5.5
    assert result["DV"] == 185.0
    assert result["LLUVIA"] == 0.0
    assert result["UV"] == 0.35
    assert result["RS"] == 205.0


# -------------------------------
# Test para _read_csv
# -------------------------------
@pytest.mark.asyncio
async def test_read_csv(tmp_path, publisher_instance):
    year = "2022"
    month = "01"
    day = "05"
    csv_dir = Path(publisher_instance.csv_dir)
    file_dir = csv_dir / year / month
    file_dir.mkdir(parents=True, exist_ok=True)
    csv_file = file_dir / f"{day}.csv"

    content = (
        "timestamp,Temperature,Humidity,Pressure,WindSpeed,WindDirection,RainRate,UV,SolarRadiation\n"
        "2022-01-05 10:10:00,20,50,1013,5,180,0,0.3,200\n"
    )
    csv_file.write_text(content, encoding="utf-8")

    df = await publisher_instance._read_csv(year, month, day)
    assert "timestamp" in df.columns, "El DataFrame debe tener la columna 'timestamp'."
    assert df.shape[0] == 1, "El DataFrame debe contener 1 registro."
    assert float(df["Temperature"].iloc[0]) == 20.0


# -------------------------------
# Test para _read_control
# -------------------------------
@pytest.mark.asyncio
async def test_read_control(tmp_path, publisher_instance):
    control_file = tmp_path / "control.json"
    timestamp = datetime(2022, 1, 1, 10, 0, 0).isoformat()
    data = {"last_successful": {"publisher": timestamp}}
    control_file.write_text(json.dumps(data), encoding="utf-8")
    publisher_instance.control_file = control_file

    last_successful = await publisher_instance._read_control()
    assert last_successful == datetime.fromisoformat(timestamp), (
        "La fecha leída del control file no es la esperada."
    )


# -------------------------------
# Test para _send_to_endpoint
# -------------------------------
@pytest.mark.asyncio
async def test_send_to_endpoint(monkeypatch, publisher_instance):
    class DummyResponse:
        async def text(self):
            return "OK response"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        # Definida como función normal en lugar de asíncrona,
        # de modo que async with funcione correctamente.
        def post(self, url, headers, json, raise_for_status):
            return DummyResponse()

    # Parcheamos el ClientSession en el espacio de nombres de aiohttp
    monkeypatch.setattr("aiohttp.ClientSession", lambda: DummySession())

    dummy_data = {
        "timestamp": "2022-01-01 10:00",
        "TEMP": 21.0,
        "HR": 52.5,
        "PA": 1014.0,
        "VV": 5.5,
        "DV": 185.0,
        "LLUVIA": 0.0,
        "UV": 0.35,
        "RS": 205.0,
    }
    result = await publisher_instance._send_to_endpoint(dummy_data)
    assert result is True, (
        "El envío a endpoint debería retornar True cuando es exitoso."
    )


# -------------------------------
# Test para _execute_publish_cycle
# -------------------------------
@pytest.mark.asyncio
async def test_execute_publish_cycle(monkeypatch, publisher_instance):
    now = datetime.now()
    fixed_last = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)

    async def fake_read_control():
        return fixed_last

    monkeypatch.setattr(publisher_instance, "_read_control", fake_read_control)

    async def fake_read_csv(year, month, day):
        target_hour = fixed_last + timedelta(hours=1)
        data = {
            "timestamp": [target_hour + timedelta(minutes=10)],
            "Temperature": [20],
            "Humidity": [50],
            "Pressure": [1013],
            "WindSpeed": [5],
            "WindDirection": [180],
            "RainRate": [0],
            "UV": [0.3],
            "SolarRadiation": [200],
        }
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    monkeypatch.setattr(publisher_instance, "_read_csv", fake_read_csv)

    async def fake_send_to_endpoint(data):
        return True

    monkeypatch.setattr(publisher_instance, "_send_to_endpoint", fake_send_to_endpoint)

    update_calls = []

    async def fake_update_control_file(key, data):
        update_calls.append(data)

    # Utilizamos la ruta correcta del módulo dinámicamente.
    monkeypatch.setattr(
        f"{publisher_instance.__module__}.update_control_file", fake_update_control_file
    )

    await publisher_instance._execute_publish_cycle()
    assert len(update_calls) >= 1, (
        "Se esperaba que se actualizara el archivo de control al menos una vez."
    )


# -------------------------------
# Test para run (verificando que se detenga correctamente)
# -------------------------------
@pytest.mark.asyncio
async def test_run_stops_immediately(monkeypatch, publisher_instance):
    # Hacemos que get_state retorne RUNNING en la primera llamada y STOPPED a partir de la segunda.
    call_count = 0

    async def fake_get_state():
        nonlocal call_count
        if call_count == 0:
            call_count += 1
            return PublisherState.RUNNING
        return PublisherState.STOPPED

    monkeypatch.setattr(publisher_instance, "get_state", fake_get_state)

    execution_flag = False

    async def fake_execute_publish_cycle():
        nonlocal execution_flag
        execution_flag = True

    monkeypatch.setattr(
        publisher_instance, "_execute_publish_cycle", fake_execute_publish_cycle
    )

    await publisher_instance.run()
    assert execution_flag is True, (
        "El método run debería haber ejecutado al menos un ciclo de publicación."
    )


# -------------------------------
# Bloque para ejecutar los tests directamente
# -------------------------------
if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v"]))
