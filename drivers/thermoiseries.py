import socket
import time
from typing import Optional


class AirQualityAnalyzer:
    """Clase para interactuar con un analizador de calidad de aire vía TCP."""

    def __init__(self, host: str, port: int, timeout: int = 10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self) -> bool:
        """Establece la conexión con el analizador."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))
            print(f"Conectado a {self.host}:{self.port}")
            return True
        except ConnectionRefusedError:
            print(f"Conexión rechazada a {self.host}:{self.port}")
            return False
        except Exception as e:
            print(f"Error al conectar a {self.host}:{self.port}: {e}")
            return False

    def disconnect(self):
        """Cierra la conexión con el analizador."""
        if self.sock:
            self.sock.close()
            print("Conexión cerrada")

    def send_command(self, command: str) -> str:
        """Envía un comando al analizador y devuelve la respuesta."""
        if not self.sock:
            raise ConnectionError("No hay conexión activa")
        self.sock.sendall(f"{command}\r\n".encode("ascii"))
        time.sleep(1)  # Ajustable según el dispositivo
        return self.sock.recv(4096).decode("ascii")

    def get_total_records(self) -> int:
        """Obtiene el número total de registros del analizador."""
        response = self.send_command("no of lrec")
        print(f"Respuesta 'no of lrec': {response}")
        try:
            return int(response.split()[3])
        except (IndexError, ValueError) as e:
            raise ValueError(f"Error al interpretar 'no of lrec': {e}")

    def download_records(
        self, num_records: int, output_file: str, batch_size: int = 10
    ):
        """Descarga registros y los guarda en un archivo."""
        with open(output_file, "a") as file:
            for i in range(num_records, -1, -batch_size):
                count = min(batch_size, i + 1)  # Calcula cuántos registros pedir
                # Si quedan menos de batch_size registros, ajustamos el comando
                cmd = f"lrec {i} {count}" if i >= count else f"lrec {i} {i}"
                response = self.send_command(cmd)
                print(f"Respuesta '{cmd}': {response}")
                file.write(response)


def main():
    HOST = "192.168.22.15"
    PORT = 9880
    TIMEOUT = 10
    DIAS = 2
    OUTPUT_FILE = "data_output_nox.txt"
    download_records = DIAS * 24

    # Instanciar y usar la clase
    analyzer = AirQualityAnalyzer(HOST, PORT, TIMEOUT)
    if not analyzer.connect():
        return

    try:
        total_records = analyzer.get_total_records()
        print(f"Número total de registros: {total_records}")
        analyzer.download_records(download_records, OUTPUT_FILE)
        print(f"Datos guardados en {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        analyzer.disconnect()


if __name__ == "__main__":
    main()
