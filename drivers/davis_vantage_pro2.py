# drivers/davis_vantage_pro2.py
import serial
import time
from datetime import datetime, timedelta
from array import array


class DavisVantagePro2:
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

    def __init__(self, port="COM4", baudrate=19200, timeout=2):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial_conn = None

    def connect(self):
        try:
            self.serial_conn = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=self.timeout,
            )
            print(f"Conectado al puerto {self.port}")
            self.wake_up()
        except serial.SerialException as e:
            print(f"Error al conectar: {e}")
            raise

    def get_station_time(self):
        """Obtiene la hora actual de la estación."""
        self.serial_conn.flushInput()
        print("Enviando GETTIME")
        self.serial_conn.write(b"GETTIME\n")
        time.sleep(3)  # Más tiempo para la respuesta
        response = self.serial_conn.read(8)
        print(f"Respuesta cruda a GETTIME: {response.hex()}")
        if len(response) == 8:
            if self.verify_crc(response):
                seconds = response[0]
                minutes = response[1]
                hours = response[2]
                day = response[3]
                month = response[4]
                year = 1900 + response[5]
                time_str = f"{year}-{month:02d}-{day:02d} {hours:02d}:{minutes:02d}:{seconds:02d}"
                print(f"Hora de la estación: {time_str}")
                return datetime(year, month, day, hours, minutes, seconds)
            else:
                print("CRC inválido para GETTIME")
        else:
            print(
                f"Respuesta de longitud incorrecta: {len(response)} bytes - {response.hex()}"
            )
        return None

    def get_last_hour(self):
        """Obtiene el último registro horario del datalogger usando DMPAFT."""
        self.serial_conn.flushInput()

        # Intentar obtener la hora de la estación
        station_time = self.get_station_time()
        if station_time:
            timestamp_dt = station_time - timedelta(hours=2)
        else:
            print("Usando hora local como respaldo")
            timestamp_dt = datetime.now() - timedelta(
                hours=24
            )  # Hace 24 horas como fallback

        # Enviar comando DMPAFT
        print("Enviando DMPAFT")
        self.serial_conn.write(b"DMPAFT\n")
        time.sleep(2)
        response = self.serial_conn.read(self.serial_conn.in_waiting or 10)
        print(f"Respuesta después de DMPAFT: {response}")
        if b"\x06" not in response:
            print("No se recibió ACK después de DMPAFT")
            return None

        # Enviar timestamp
        timestamp = timestamp_dt.strftime("%m%d%y%H%M")
        print(f"Enviando timestamp: {timestamp}")
        timestamp_bytes = timestamp.encode()
        crc = self.calculate_crc(timestamp_bytes)
        crc_bytes = crc.to_bytes(2, byteorder="big")
        self.serial_conn.write(timestamp_bytes + crc_bytes)
        time.sleep(3)
        response = self.serial_conn.read(self.serial_conn.in_waiting or 10)
        print(f"Respuesta después de timestamp: {response}")
        if b"\x06" not in response:
            print("No se recibió ACK después del timestamp")
            return None

        # Leer encabezado (6 bytes)
        header = b""
        while len(header) < 6:
            chunk = self.serial_conn.read(6 - len(header))
            header += chunk
            print(f"Bytes de encabezado leídos: {len(chunk)}, total: {len(header)}")

        if len(header) != 6 or not self.verify_crc(header):
            print(f"Encabezado inválido: {len(header)} bytes - {header.hex()}")
            self.serial_conn.write(b"\x15")  # NAK
            return None
        self.serial_conn.write(b"\x06")  # ACK

        num_pages = int.from_bytes(header[0:2], byteorder="little")
        print(f"Número de páginas: {num_pages}")
        if num_pages == 0:
            print("No hay datos nuevos después del timestamp")
            return None

        # Leer páginas y tomar el último registro
        last_record = None
        for i in range(num_pages):
            print(f"Intentando leer página {i + 1}")
            page_data = b""
            while len(page_data) < 269:
                chunk = self.serial_conn.read(269 - len(page_data))
                page_data += chunk
                print(f"Bytes leídos: {len(chunk)}, total: {len(page_data)}")

            if len(page_data) != 269 or not self.verify_crc(page_data):
                print(
                    f"Página {i + 1} inválida: {len(page_data)} bytes - {page_data.hex()}"
                )
                self.serial_conn.write(b"\x15")  # NAK
                continue

            print(f"Página {i + 1} válida")
            self.serial_conn.write(b"\x06")  # ACK
            records = [page_data[j : j + 52] for j in range(5, 5 + 52 * 5, 52)]
            for record in records:
                if int.from_bytes(record[0:2], byteorder="little") != 0xFFFF:
                    last_record = record

        if last_record:
            return self._parse_archive_record(last_record)
        print("No se obtuvo un registro válido")
        return None

    def wake_up(self):
        self.serial_conn.write(b"\n")
        time.sleep(2)
        response = self.serial_conn.read(2)
        if response == b"\n\r":
            print("Estación despierta")
        else:
            raise Exception("No se pudo despertar la estación")

    def calculate_crc(self, data):
        crc = 0
        for byte in array("B", data):
            crc = self.CRC_TABLE[(crc >> 8) ^ byte] ^ ((crc & 0xFF) << 8)
        return crc

    def verify_crc(self, data):
        crc = self.calculate_crc(data)
        print(f"CRC calculado sobre {len(data)} bytes: {crc} (hex: {hex(crc)})")
        return crc == 0

    def get_loop_data(self, interval_seconds=60, duration_seconds=None):
        start_time = time.time()

        while duration_seconds is None or (time.time() - start_time) < duration_seconds:
            self.serial_conn.flushInput()
            command = b"LOOP 1\n"
            print(f"Enviando: {command}")
            self.serial_conn.write(command)
            time.sleep(1)

            ack = self.serial_conn.read(1)
            if ack != b"\x06":
                print(f"Esperaba ACK (\x06), recibido: {ack}")
                time.sleep(interval_seconds)
                continue

            packet = b""
            while len(packet) < 99:
                chunk = self.serial_conn.read(99 - len(packet))
                packet += chunk
                print(f"Bytes leídos: {len(chunk)}, total: {len(packet)}")

            if len(packet) != 99:
                print(f"Paquete incompleto: {len(packet)} bytes - {packet}")
                time.sleep(interval_seconds)
                continue

            print(f"Paquete recibido: {packet.hex()}")
            if not self.verify_crc(packet):
                print("Error de CRC: el paquete no es válido")
                time.sleep(interval_seconds)
                continue

            data = self._parse_loop_packet(packet)
            yield data
            time.sleep(
                max(0, interval_seconds - (time.time() - start_time) % interval_seconds)
            )

    def _parse_loop_packet(self, packet):
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "barometer": int.from_bytes(packet[7:9], byteorder="little") / 1000,
            "temp_in": int.from_bytes(packet[9:11], byteorder="little") / 10,
            "temp_out": int.from_bytes(packet[12:14], byteorder="little") / 10,
            "wind_speed": packet[14],
            "wind_dir": int.from_bytes(packet[16:18], byteorder="little"),
            "humidity_out": packet[33],
            "rain_rate": int.from_bytes(packet[41:43], byteorder="little") / 100,
        }
        return data

    def _parse_archive_record(self, record):
        """Parsea un registro archivado de 52 bytes."""
        date_stamp = int.from_bytes(record[0:2], byteorder="little")
        time_stamp = int.from_bytes(record[2:4], byteorder="little")
        day = date_stamp & 0x1F
        month = (date_stamp >> 5) & 0x0F
        year = 2000 + (date_stamp >> 9)
        hour = time_stamp // 100
        minute = time_stamp % 100
        data = {
            "timestamp": f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}",
            "temp_out": int.from_bytes(record[4:6], byteorder="little") / 10,  # °F
            "wind_avg": record[14],  # mph
            "wind_dir": record[16],  # dirección promedio (0-15)
            "humidity_out": record[18],  # %
            "rain_total": int.from_bytes(record[20:22], byteorder="little") / 100,  # in
            "barometer": int.from_bytes(record[28:30], byteorder="little")
            / 1000,  # hPa
        }
        return data

    def close(self):
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            print("Conexión cerrada")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
