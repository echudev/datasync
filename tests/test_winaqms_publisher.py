import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from services import WinAQMSPublisher, PublisherState

# Para evitar problemas en Windows, se establece la política adecuada para el event loop.
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# -------------------------------
# Fixture para configurar variables de entorno y directorio temporal para archivos WAD
# -------------------------------
@pytest.fixture
def env_setup(monkeypatch, tmp_path):
    """
    Configura las variables de entorno necesarias (GOOGLE_POST_URL, ORIGEN, API_KEY)
    y utiliza un directorio temporal para simular el directorio de archivos WAD.
    """
    monkeypatch.setenv("GOOGLE_POST_URL", "http://dummyendpoint.com/api")
    monkeypatch.setenv("ORIGEN", "dummy_origin")
    monkeypatch.setenv("API_KEY", "dummy_api_key")
    # Retornamos el directorio 'wad_data' dentro del directorio temporal
    return str(tmp_path / "wad_data")


# -------------------------------
# Fixture asíncrona para crear una instancia de WinAQMSPublisher
# -------------------------------
@pytest.fixture
async def publisher_instance(env_setup):
    """
    Crea una instancia de WinAQMSPublisher utilizando el directorio temporal
    y un logger de prueba.
    """
    test_logger = logging.getLogger("test_logger")
    return WinAQMSPublisher(wad_dir=env_setup, logger=test_logger)


# -------------------------------
# Test para _build_wad_path
# -------------------------------
@pytest.mark.asyncio
async def test_build_wad_path(publisher_instance):
    path = publisher_instance._build_wad_path("2022", "3", "5")
    expected = Path(publisher_instance.wad_dir) / "2022" / "03" / "eco20220305.wad"
    assert path == expected, "El path WAD generado no coincide con el esperado."


# -------------------------------
# Test para update_state y get_state
# -------------------------------
@pytest.mark.asyncio
async def test_update_and_get_state(publisher_instance):
    await publisher_instance.update_state("STOPPED")
    state = await publisher_instance.get_state()
    # Comparamos el valor para evitar problemas de identidad
    assert state.value == PublisherState.STOPPED.value, "El estado debería ser STOPPED."

    await publisher_instance.update_state("RUNNING")
    state = await publisher_instance.get_state()
    assert state.value == PublisherState.RUNNING.value, "El estado debería ser RUNNING."


# -------------------------------
# Test para _calculate_hourly_averages
# -------------------------------
@pytest.mark.asyncio
async def test_calculate_hourly_averages(publisher_instance):
    target_hour = datetime(2022, 1, 1, 10, 0, 0)
    # Simulamos dos registros con los sensores C1 a C6.
    data = {
        "Date_Time": [
            target_hour + timedelta(minutes=10),
            target_hour + timedelta(minutes=20),
        ],
        "C1": [1.234, 1.236],
        "C2": [0.5, 0.55],
        "C3": [2.0, 2.0],
        "C4": [1.0, 1.0],
        "C5": [0.123, 0.125],
        "C6": [10, 12],
    }
    df = pd.DataFrame(data)
    df["Date_Time"] = pd.to_datetime(df["Date_Time"])

    result = publisher_instance._calculate_hourly_averages(df, target_hour)
    assert result is not None, (
        "El resultado no debe ser None cuando hay datos en la hora."
    )
    # Se esperan:
    # CO: (1.234 + 1.236)/2 = 1.235 (redondeado a 3 decimales)
    # NO: (0.5 + 0.55)/2 = 0.525
    # NO2: 2.0, NOx: 1.0,
    # O3: (0.123 + 0.125)/2 = 0.124 redondeado a 2 decimales => 0.12
    # PM10: (10 + 12)/2 = 11 redondeado como entero
    assert result["CO"] == 1.235
    assert result["NO"] == 0.525
    assert result["NO2"] == 2.0
    assert result["NOx"] == 1.0
    assert result["O3"] == 0.12
    assert result["PM10"] == 11


# -------------------------------
# Test para _read_wad_file
# -------------------------------
@pytest.mark.asyncio
async def test_read_wad_file(tmp_path, publisher_instance):
    # Se crea un archivo .wad temporal en la estructura esperada: wad_dir/year/month/ecoYYYYMMDD.wad
    wad_dir = Path(publisher_instance.wad_dir)
    year = "2022"
    month = "01"
    day = "05"
    file_dir = wad_dir / year / month
    file_dir.mkdir(parents=True, exist_ok=True)
    wad_file = file_dir / f"eco{year}{month}{day}.wad"

    # Contenido del archivo: encabezado y un registro
    content = (
        "Date_Time,C1,C2,C3,C4,C5,C6\n2022/01/05 10:10:00,1.234,0.5,2.0,1.0,0.123,10\n"
    )
    wad_file.write_text(content, encoding="utf-8")

    df = await publisher_instance._read_wad_file(year, month, day)
    assert "Date_Time" in df.columns, (
        "El DataFrame debe contener la columna 'Date_Time'."
    )
    assert df.shape[0] == 1, "El DataFrame debe tener 1 registro."
    dt_expected = pd.to_datetime("2022/01/05 10:10:00", format="%Y/%m/%d %H:%M:%S")
    assert df["Date_Time"].iloc[0] == dt_expected, (
        "La fecha convertida no es la esperada."
    )


# -------------------------------
# Test para _read_control
# -------------------------------
@pytest.mark.asyncio
async def test_read_control(tmp_path, publisher_instance):
    # Se crea un archivo de control temporal con la clave "winaqms_publisher"
    control_file = tmp_path / "control.json"
    timestamp = datetime(2022, 1, 1, 10, 0, 0).isoformat()
    data = {"last_successful": {"winaqms_publisher": timestamp}}
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

        # Se define post como función normal para que async with funcione correctamente.
        def post(self, url, headers, json, raise_for_status):
            return DummyResponse()

    monkeypatch.setattr("aiohttp.ClientSession", lambda **kwargs: DummySession())

    dummy_data = {
        "timestamp": "2022-01-01 10:00",
        "CO": 1.235,
        "NO": 0.525,
        "NO2": 2.0,
        "NOx": 1.0,
        "O3": 0.12,
        "PM10": 11,
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

    publisher_instance._read_control = fake_read_control

    async def fake_read_wad_file(year, month, day):
        target_hour = fixed_last + timedelta(hours=1)
        data = {
            "Date_Time": [target_hour + timedelta(minutes=10)],
            "C1": [1.234],
            "C2": [0.5],
            "C3": [2.0],
            "C4": [1.0],
            "C5": [0.123],
            "C6": [10],
        }
        df = pd.DataFrame(data)
        df["Date_Time"] = pd.to_datetime(df["Date_Time"])
        return df

    publisher_instance._read_wad_file = fake_read_wad_file

    async def fake_send_to_endpoint(sensor_data):
        return True

    publisher_instance._send_to_endpoint = fake_send_to_endpoint

    update_calls = []

    async def fake_update_control_file(key, data):
        update_calls.append(data)

    monkeypatch.setattr(
        f"{publisher_instance.__module__}.update_control_file", fake_update_control_file
    )

    await publisher_instance._execute_publish_cycle()
    assert len(update_calls) >= 1, (
        "Se esperaba que se actualizara el archivo de control al menos una vez."
    )


# -------------------------------
# Bloque para ejecutar los tests directamente
# -------------------------------
if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v"]))
