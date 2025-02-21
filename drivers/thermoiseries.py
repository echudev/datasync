import telnetlib
import time

HOST = "192.168.22.15"
PORT = 9880
# MODBUS_PORT = 502
TIMEOUT = 10

# Ingresa la cantida de días y el archivo donde vas a descargar los datos
DIAS = 2
output_file = "data_output_nox.txt"

# los equipos 48i y 42i almacenan 1 registro por hora
download_records = DIAS * 24 

try:
    # Conectar al dispositivo
    tn = telnetlib.Telnet(HOST, PORT, TIMEOUT)
    print(f"Conectado a {HOST} en el puerto {PORT}")
except ConnectionRefusedError:
    print(f"Conexión rechazada al intentar conectar a {HOST} en el puerto {PORT}")
    exit(1)
except Exception as e:
    print(f"Error al conectar a {HOST} en el puerto {PORT}: {e}")
    exit(1)

# Función para enviar un comando y obtener la respuesta
def send_command(command):
    tn.write(command.encode('ascii') + b"\r\n")
    time.sleep(1)  # Espera para que el dispositivo procese el comando
    return tn.read_very_eager().decode('ascii')

# Obtener el número total de registros largos
total_records_response = send_command("no of lrec")
print(f"Respuesta del comando 'no of lrec': {total_records_response}")

# Extraer el número total de registros de la respuesta
try:
    total_records = int(total_records_response.split()[3])
except (IndexError, ValueError):
    print("Error al interpretar la respuesta del comando 'no of lrec'.")
    tn.close()
    exit(1)

print(f"Número total de registros largos: {total_records}")

# descargo los datos del analizador y los guardo en data_output.txt
with open(output_file, "a") as file:
    for i in range(download_records, -1, -10):
        if i < 10:
            records_response = send_command(f"lrec {i} {i}")
            print(f"Respuesta del comando 'lrec {i} {i}': {records_response}")
            file.write(records_response)
            break
        records_response = send_command(f"lrec {i} 10")
        print(f"Respuesta del comando 'lrec {i} 10': {records_response}")
        file.write(records_response)

# Cerrar la conexión Telnet
tn.close()

print("Datos descargados y guardados en 'data_output.txt'")


if __name__ == "__main__":
    pass