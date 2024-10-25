# Script Lumu

Prueba tecnica

## Descripción

Este script procesa logs de servidores dns.

## Aspectos necesarios para ejecutar el proyecto

Clonar el repositorio

```
git clone {url_del_repo}
```

Instalar las dependencias (Se recomienda trabajar dentro de un entorno virtual)

```
pip install -r requirements.txt
```

Dentro de la carpeta del proyecto ejecutar 

```
python dns_queries_collector.py -f <ruta y nombre del archivo>
```

Se debe remplazar '<ruta y nombre del archivo>' por la ruta del archivo que se desee analizar, he aqui un ejemplo

```
python dns_queries_collector.py -f ./queries
```
