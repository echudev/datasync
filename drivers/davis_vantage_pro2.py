"""
Driver for Davis Vantage Pro 2 weather station.

This module provides an asynchronous interface to read real-time data using the LOOP command.
"""

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

    def __init__(self, port: str = "COM4", baudrate: int = 19200, timeout: float = 2):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial_conn = None

    def connect(self) -> None:
        """Establish a synchronous serial connection."""
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
        """Wake up the weather station synchronously."""
        self.serial_conn.write(b"\n")
        time.sleep(2)
        response = self.serial_conn.read(2)
        if response == b"\n\r":
            logger.info("Station is awake")
        else:
            raise Exception(f"Failed to wake up station, response: {response!r}")

    async def read(self) -> Dict[str, float]:
        """Read real-time data from the station using LOOP command."""
        try:
            if not self.serial_conn:
                await asyncio.get_event_loop().run_in_executor(None, self.connect)

            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, self._read_sync)
            return data

        except Exception as e:
            logger.error(f"Error reading data: {e}")
            return {}

    def _read_sync(self) -> Dict[str, float]:
        """Synchronous read implementation."""
        self.serial_conn.flush()
        self.serial_conn.write(b"LOOP 1\n")
        time.sleep(1)

        ack = self.serial_conn.read(1)
        if ack != b"\x06":
            logger.warning(f"Expected ACK (\x06), got: {ack!r}")
            return {}

        packet = self.serial_conn.read(99)
        if len(packet) != 99:
            logger.warning(f"Incomplete packet: {len(packet)} bytes")
            return {}

        if not self.verify_crc(packet):
            logger.warning("CRC check failed")
            return {}

        data = self._parse_loop_packet(packet)
        logger.debug(f"Data read: {data}")
        return data

    def calculate_crc(self, data: bytes) -> int:
        crc = 0
        for byte in array("B", data):
            crc = self.CRC_TABLE[(crc >> 8) ^ byte] ^ ((crc & 0xFF) << 8)
        return crc

    def verify_crc(self, data: bytes) -> bool:
        return self.calculate_crc(data) == 0

    def _parse_loop_packet(self, packet: bytes) -> Dict[str, float]:
        return {
            "Temperature": int.from_bytes(packet[12:14], byteorder="little") / 10,
            "Humidity": packet[33],
            "Pressure": int.from_bytes(packet[7:9], byteorder="little") / 1000,
            "WindSpeed": packet[14],
            "WindDirection": int.from_bytes(packet[16:18], byteorder="little"),
            "RainRate": int.from_bytes(packet[41:43], byteorder="little") / 100,
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
