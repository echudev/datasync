"""
- Obtener los datos horarios del .csv diario en la carpeta data
- Publicarlos en el servidor web.
"""

import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API URL from the environment variables
url = os.getenv('GOOGLE_POST_URL')

origen = 'CENTENARIO'

csv_path = 'data.csv'
with open(csv_path, 'r') as file:
    csv_data = file.read()

headers = {
    'Content-Type': 'text/csv',
}

params = {
    'origen': origen,
}

try:
    response = requests.post(url, headers=headers, params=params, data=csv_data)
    response.raise_for_status()
    print('Respuesta:', response.text)
except requests.exceptions.RequestException as e:
    print('Error:', e)