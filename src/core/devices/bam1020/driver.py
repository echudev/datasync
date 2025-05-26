import serial
import time

# Configuración del puerto serial para el BAM1020
SERIAL_PORT = 'COM4'
BAUDRATE = 9600       # Verificar en el manual del equipo
BYTESIZE = serial.EIGHTBITS
PARITY = serial.PARITY_NONE 
STOPBITS = serial.STOPBITS_ONE
TIMEOUT = 2           # Tiempo de espera para lectura (segundos)

def last_hour_bam1020():
    ser = None
    try:
        # Inicializar conexión serial
        ser = serial.Serial(port=SERIAL_PORT,
                            baudrate=BAUDRATE,
                            bytesize=BYTESIZE,
                            parity=PARITY,
                            stopbits=STOPBITS,
                            timeout=TIMEOUT)

        # Paso 1: Establecer comunicación con 3 retornos de carro
        ser.write(b'\r\r\r')  # Tres carriage returns ASCII
        time.sleep(0.1)
        
        # Leer respuesta (asterisco *)
        response = ser.read_until(b'*')
        if b'*' not in response:
            raise Exception("No se recibió confirmación de comunicación (*)")
        
        # Paso 2: Solicitar menú CSV (archivo 6)
        ser.write(b'6')

        # Paso 3: Solicitar último registro (subarchivo 4)
        ser.write(b'4')
        
        # Esperar un momento para que el equipo prepare los datos
        time.sleep(0.5)
        
        # Leer todos los datos disponibles
        data = []
        while True:
            line = ser.readline()
            if not line:
                break
            decoded_line = line.decode('ascii').strip()
            data.append(decoded_line)
            print("Datos crudos recibidos:", decoded_line)  # Para depuración

        # Guardar en CSV
        with open('ultimos_datos.csv', 'w', newline='') as f:
            f.write('\n'.join(data))
        
        print("Datos guardados exitosamente en ultimos_datos.csv")

    except Exception as e:
        print("Error: {}".format(str(e)))
    finally:
        # Cerrar el puerto serial
        if ser and ser.is_open:
            ser.close()

if __name__ == "__main__":
    last_hour_bam1020()