import asyncio
import logging
import serial
import time
from typing import Dict
from array import array

from services import Sensor

logger = logging.getLogger("davis_vantage_pro2")


class DavisVantagePro2(Sensor):
    CRC_TABLE = (
        0x0,
        0x1021,
        0x2042,
        0x3063,
        0x4084,
        0x50A5,
        0x60C6,
        0x70E7,
        0x8108,
        0x9129,
        0xA14A,
        0xB16B,
        0xC18C,
        0xD1AD,
        0xE1CE,
        0xF1EF,
        0x1231,
        0x0210,
        0x3273,
        0x2252,
        0x52B5,
        0x4294,
        0x72F7,
        0x62D6,
        0x9339,
        0x8318,
        0xB37B,
        0xA35A,
        0xD3BD,
        0xC39C,
        0xF3FF,
        0xE3DE,
        0x2462,
        0x3443,
        0x0420,
        0x1401,
        0x64E6,
        0x74C7,
        0x44A4,
        0x5485,
        0xA56A,
        0xB54B,
        0x8528,
        0x9509,
        0xE5EE,
        0xF5CF,
        0xC5AC,
        0xD58D,
        0x3653,
        0x2672,
        0x1611,
        0x0630,
        0x76D7,
        0x66F6,
        0x5695,
        0x46B4,
        0xB75B,
        0xA77A,
        0x9719,
        0x8738,
        0xF7DF,
        0xE7FE,
        0xD79D,
        0xC7BC,
        0x48C4,
        0x58E5,
        0x6886,
        0x78A7,
        0x0840,
        0x1861,
        0x2802,
        0x3823,
        0xC9CC,
        0xD9ED,
        0xE98E,
        0xF9AF,
        0x8948,
        0x9969,
        0xA90A,
        0xB92B,
        0x5AF5,
        0x4AD4,
        0x7AB7,
        0x6A96,
        0x1A71,
        0x0A50,
        0x3A33,
        0x2A12,
        0xDBFD,
        0xCBDC,
        0xFBBF,
        0xEB9E,
        0x9B79,
        0x8B58,
        0xBB3B,
        0xAB1A,
        0x6CA6,
        0x7C87,
        0x4CE4,
        0x5CC5,
        0x2C22,
        0x3C03,
        0x0C60,
        0x1C41,
        0xEDAE,
        0xFD8F,
        0xCDEC,
        0xDDCD,
        0xAD2A,
        0xBD0B,
        0x8D68,
        0x9D49,
        0x7E97,
        0x6EB6,
        0x5ED5,
        0x4EF4,
        0x3E13,
        0x2E32,
        0x1E51,
        0x0E70,
        0xFF9F,
        0xEFBE,
        0xDFDD,
        0xCFFC,
        0xBF1B,
        0xAF3A,
        0x9F59,
        0x8F78,
        0x9188,
        0x81A9,
        0xB1CA,
        0xA1EB,
        0xD10C,
        0xC12D,
        0xF14E,
        0xE16F,
        0x1080,
        0x00A1,
        0x30C2,
        0x20E3,
        0x5004,
        0x4025,
        0x7046,
        0x6067,
        0x83B9,
        0x9398,
        0xA3FB,
        0xB3DA,
        0xC33D,
        0xD31C,
        0xE37F,
        0xF35E,
        0x02B1,
        0x1290,
        0x22F3,
        0x32D2,
        0x4235,
        0x5214,
        0x6277,
        0x7256,
        0xB5EA,
        0xA5CB,
        0x95A8,
        0x8589,
        0xF56E,
        0xE54F,
        0xD52C,
        0xC50D,
        0x34E2,
        0x24C3,
        0x14A0,
        0x0481,
        0x7466,
        0x6447,
        0x5424,
        0x4405,
        0xA7DB,
        0xB7FA,
        0x8799,
        0x97B8,
        0xE75F,
        0xF77E,
        0xC71D,
        0xD73C,
        0x26D3,
        0x36F2,
        0x0691,
        0x16B0,
        0x6657,
        0x7676,
        0x4615,
        0x5634,
        0xD94C,
        0xC96D,
        0xF90E,
        0xE92F,
        0x99C8,
        0x89E9,
        0xB98A,
        0xA9AB,
        0x5844,
        0x4865,
        0x7806,
        0x6827,
        0x18C0,
        0x08E1,
        0x3882,
        0x28A3,
        0xCB7D,
        0xDB5C,
        0xEB3F,
        0xFB1E,
        0x8BF9,
        0x9BD8,
        0xABBB,
        0xBB9A,
        0x4A75,
        0x5A54,
        0x6A37,
        0x7A16,
        0x0AF1,
        0x1AD0,
        0x2AB3,
        0x3A92,
        0xFD2E,
        0xED0F,
        0xDD6C,
        0xCD4D,
        0xBDAA,
        0xAD8B,
        0x9DE8,
        0x8DC9,
        0x7C26,
        0x6C07,
        0x5C64,
        0x4C45,
        0x3CA2,
        0x2C83,
        0x1CE0,
        0x0CC1,
        0xEF1F,
        0xFF3E,
        0xCF5D,
        0xDF7C,
        0xAF9B,
        0xBFBA,
        0x8FD9,
        0x9FF8,
        0x6E17,
        0x7E36,
        0x4E55,
        0x5E74,
        0x2E93,
        0x3EB2,
        0x0ED1,
        0x1EF0,
    )

    def __init__(self, port: str = "COM4", baudrate: int = 19200, timeout: float = 5):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial_conn = None

    def connect(self) -> None:
        try:
            self.serial_conn = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=self.timeout,
            )
            logger.info(f"Connected to {self.port}")
            self.wake_up()
        except serial.SerialException as e:
            logger.error(f"Error connecting to Davis Vantage Pro 2: {e}")
            raise

    def wake_up(self) -> None:
        self.serial_conn.write(b"\n")
        time.sleep(2)
        response = self.serial_conn.read(2)
        if response != b"\n\r":
            raise Exception(f"Failed to wake up station, response: {response!r}")
        logger.info("Station is awake")

    async def read(self) -> Dict[str, float]:
        try:
            if not self.serial_conn or not self.serial_conn.is_open:
                await asyncio.get_event_loop().run_in_executor(None, self.connect)
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, self._read_sync)
            return data
        except Exception as e:
            logger.error(f"Error reading data: {e}")
            return {}

    def _read_sync(self) -> Dict[str, float]:
        try:
            self.serial_conn.flush()
            self.serial_conn.write(b"LOOP 1\n")
            time.sleep(1)  # Ajusta si es necesario
            ack = self.serial_conn.read(1)
            if ack != b"\x06":
                return {}

            packet = self.serial_conn.read(99)
            if len(packet) != 99:
                return {}

            if not self.verify_crc(packet):
                return {}

            data = self._parse_loop_packet(packet)
            return data
        except Exception as e:
            logger.error(f"Error in synchronous read: {e}")
            return {}

    def calculate_crc(self, data: bytes) -> int:
        crc = 0
        for byte in array("B", data):
            crc = self.CRC_TABLE[(crc >> 8) ^ byte] ^ ((crc & 0xFF) << 8)
        return crc

    def verify_crc(self, data: bytes) -> bool:
        return self.calculate_crc(data) == 0

    def _parse_loop_packet(self, packet: bytes) -> Dict[str, float]:
        try:
            # Barometer (Bytes 7-8): inHg * 1000, little-endian, convertir a hPa
            pressure_raw = int.from_bytes(packet[7:9], byteorder="little", signed=False)
            pressure_inhg = pressure_raw / 1000  # inHg
            pressure_hpa = pressure_inhg * 33.8639  # Convertir a hPa

            # Outside Temperature (Bytes 12-13): décimas de °F, little-endian, convertir a °C
            temp_f = (
                int.from_bytes(packet[12:14], byteorder="little", signed=False) / 10
            )  # °F
            temp_c = (temp_f - 32) * 5 / 9  # Convertir a °C

            # Wind Speed (Byte 14): mph
            wind_speed = float(packet[14]) if packet[14] > 0 else 0.0

            # Wind Direction (Bytes 16-17): grados (0-359), little-endian
            wind_dir = int.from_bytes(packet[16:18], byteorder="little", signed=False)
            wind_direction = (
                float(wind_dir) if 0 <= wind_dir < 360 else 112.0
            )  # ESE por defecto

            # Outside Humidity (Byte 33): % (0-100)
            humidity = float(packet[33]) if 0 <= packet[33] <= 100 else 0.0

            # Rain Rate (Bytes 41-42): pulsos por hora * 100, little-endian, convertir a in/h
            rain_rate = (
                int.from_bytes(packet[41:43], byteorder="little", signed=False) / 100
            )  # in/h

            # UV Index (Byte 44): décimas de unidades, 0.0 sin sensor
            uv = 0.0  # Sin sensor UV, forzar a 0.0

            # Solar Radiation (Bytes 45-46): W/m², 0.0 sin sensor
            solar_radiation = 0.0  # Sin sensor solar, forzar a 0.0

            return {
                "Temperature": round(temp_c, 2),  # °C, redondeado a 2 decimales
                "Humidity": round(humidity, 2),  # %, redondeado a 2 decimales
                "Pressure": round(pressure_hpa, 2),  # hPa, redondeado a 2 decimales
                "WindSpeed": round(wind_speed, 2),  # mph, redondeado a 2 decimales
                "WindDirection": round(
                    wind_direction, 2
                ),  # grados, redondeado a 2 decimales
                "RainRate": round(rain_rate, 2),  # in/h, redondeado a 2 decimales
                "UV": round(uv, 2),  # Índice UV, redondeado a 2 decimales
                "SolarRadiation": round(
                    solar_radiation, 2
                ),  # W/m², redondeado a 2 decimales
            }
        except Exception as e:
            logger.error(f"Error parsing packet: {e}")
            return {
                "Temperature": 0.0,
                "Humidity": 0.0,
                "Pressure": 0.0,
                "WindSpeed": 0.0,
                "WindDirection": 0.0,
                "RainRate": 0.0,
                "UV": 0.0,
                "SolarRadiation": 0.0,
            }

    def close(self) -> None:
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            logger.info("Connection closed")

    async def __aenter__(self):
        await asyncio.get_event_loop().run_in_executor(None, self.connect)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.get_event_loop().run_in_executor(None, self.close)
