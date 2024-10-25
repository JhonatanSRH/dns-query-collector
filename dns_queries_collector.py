"""lumu scrip.py"""
import argparse
from datetime import datetime
from itertools import groupby
import re
import requests


LUMU_KEY = 'd39a0f19-7278-4a64-a255-b7646d1ace80'
COLLECTOR_ID = '5ab55d08-ae72-4017-a41c-d9d735360288'
LUMU_URL = 'https://api.lumu.io'

endpoint_data = {
    'url': f'https://portal.lumu.io/collectors/custom-collectors/show/{COLLECTOR_ID}',
    'user': 'lumutest@spam4.me',
    'password': 'uCNgrBby28RYEKv'
}

# Recibir el archivo mediante un parametro en linea de comandos
parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filepath", help="File Path", required=True)
args = parser.parse_args()

# Regex para la extraccion de datos
PARSE_REGEX = (
    r'(?P<date>\d{1,2}-\w{3}-\d{4})\s+(?P<time>\d{2}:\d{2}:\d{2}\.\d{3})\s+'+
    r'queries: info: client\s+(?P<hex_client>@0x[0-9a-f]+)\s+(?P<client_ip>[\d.]+)#\d+\s+'+
    r'\((?P<name>.+)\): query: .+ IN (?P<type>[A-Z0-9]+)\s+(?P<query>(\S+))\s+.+\n$')
def parse_record_data(record : str) -> dict | Exception:
    """Convierte el registro en diccionario de ser posible

    Args:
        record (str): registro a evaluar

    Raises:
        Exception: Expecion general que indica que el formato del registro 
        no concuerda con lo esperado

    Returns:
        dict | Exception: devuelve el diccionario o un error de formato en el registro
    """
    # Obtiene las concidencias del regex
    match = re.search(PARSE_REGEX, record)
    if match:
        # Convertir el resultado a diccionario
        dict_record = match.groupdict()
        timestamp = dict_record.get('date') + ' ' + dict_record.get('time')
        return {
            **dict_record,
            'timestamp': datetime.strptime(
                timestamp, "%d-%b-%Y %H:%M:%S.%f").strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            'hit': '+' in dict_record.get('query')
        }
    raise Exception('Bad format')

def partition_array(data: tuple, chunk_size=500) -> tuple:
    """Genera una particion de un array en subarrays del tamano del chunk

    Args:
        data (tuple):  array a particionar
        chunk_size (int, optional): tamaño de los subarrays. Defaults to 500.

    Returns:
        tuple: array con los subarrays particionados
    """
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

def consume_ws(data: tuple) -> tuple:
    """consume el endpoint para enviar los logs parseados

    Args:
        data (tuple): tupla de datos con la informacion parseada de cada log
        
    Returns:
        tuple: tupla de los objetos respuesta
    """
    return tuple(requests.post(
        f'{LUMU_URL}/collectors/{COLLECTOR_ID}/dns/queries?key={LUMU_KEY}',
        json=chunk_data,
        timeout=120) for chunk_data in partition_array(data))

def get_stats(group_data: object, key_name: str) -> list:
    """Calcula las estadisticas de las variables agrupadas

    Args:
        group_data (object): datos agrupados para analizar
        key_name (str): nombre de la clave que sirvio para la agrupacion

    Returns:
        list: lista de las estadisticas de los datos agrupados
    """
    stats = []
    for key_val, data in group_data:
        # Para cada valor se seleccionan los aciertos
        hit_data = tuple(filter(lambda x: x['hit'], data))
        # Calcula estadisticas
        stats.append(
            {
                key_name: key_val,
                'total': len(hit_data),
                'avg': f'{(len(hit_data) / len(file_records)):.2}%'
            })
    return stats

def print_table(data: list):
    """Pinta en consola los datos de una lista de diccionarios en formato tabla

    Args:
        data (list): lista de datos a pintar
    """
    if data:
        # Imprime un separador
        print("-" * 60)
        # Imprime las filas
        for row in data:
            print("{:<45} {:<6} {:<6}".format(*(str(value) for value in row.values())))

# Abre el archivo sin cargarlo en memoria
with open(args.filepath, 'r', encoding='utf-8') as file:
    # Extrae cada linea del archivo
    file_records = tuple(line for line in file)
    # Convierte cada linea en diccionario
    iter_records = tuple(map(parse_record_data, file_records))
    # Envia los logs convertidos
    consume_ws(iter_records)
    # Agrupa los clientes para sacar sus estadisticas
    # Complejidad O(nlog(n))
    client_stats = get_stats(groupby(
        sorted(iter_records, key=lambda x : x['client_ip']),
        key=lambda x : x['client_ip']), 'client_ip')
    # Agrupa los hosts para sacar sus estadisticas
    host_stats = get_stats(groupby(
        sorted(iter_records, key=lambda x : x['name']),
        key=lambda x : x['name']), 'name')
    # Muestra las estadisticas en el formato indicado
    print(f'Total records {len(file_records)}')
    print('\nClient IPs Rank')
    print_table(sorted(client_stats, key= lambda x : x['total'], reverse=True)[:5])
    print('\nHost Rank')
    print_table(sorted(host_stats, key= lambda x : x['total'], reverse=True)[:5])
