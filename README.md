# Descripción General

Este programa realiza web scraping de un sitio web de películas y una API relacionada con eventos culturales, procesando la información extraída y almacenándola en una base de datos MongoDB. Incluye manejo de errores, soporte para múltiples hilos, y optimización mediante técnicas como "exponential backoff" en las peticiones.
Estructura del Programa

## Funciones Utilitarias

#### juntar: 
Combina elementos de una lista en una cadena estructurada.
    
#### log: 
Registra mensajes en un archivo log.txt para rastrear errores o actividades del programa.

### Manejo de Peticiones HTTP
#### peticion:
Realiza solicitudes HTTP con soporte para reintentos automáticos usando una estrategia de exponential backoff.

## Interacción con MongoDB
### amongo: 
Inserta datos en MongoDB evitando duplicados.
### mongo: 
Configura y conecta a la base de datos MongoDB.

## Extracción de Datos
### correpeli: 
Extrae información detallada sobre una película desde su página específica.
### correpag: 
Procesa todas las películas en una página de una categoría.
### correcat: 
Procesa todas las páginas de una categoría específica.
### pelis_ingesta: 
Inicia el proceso de extracción desde el sitio web de películas.
### apide y api_ingesta: 
Procesan datos obtenidos desde la API de eventos culturales.

## Transformación de Datos
### cleanitem: 
Limpia y normaliza los datos extraídos para garantizar consistencia antes de almacenarlos.
### aggregate:
Limpia la colección blog y saca el resultado en la colección blog_retocado

## Programa Principal
### main: 
Punto de entrada principal que coordina el flujo de extracción, transformación y almacenamiento de datos.

### Documentación de Funciones
#### 1. juntar(lista)

Une los elementos de una lista en una cadena separada por espacios, reemplazando dobles espacios por ": ".

Parámetros:
	lista (list): Lista de cadenas.
Retorno: Una cadena formateada.

#### 2. log(message)

Registra mensajes en un archivo de log (log.txt).

Parámetros:
	message (str): Mensaje a registrar.

### 3. peticion(url, params=None, max_retries=5, base_wait_time=10)

Realiza una solicitud HTTP con reintentos automáticos en caso de error.

Parámetros:
	url (str): URL objetivo.
	params (dict, opcional): Parámetros de la solicitud.
	max_retries (int): Máximo número de reintentos.
	base_wait_time (int): Tiempo base en segundos para "exponential backoff".
Retorno: Objeto Response de la solicitud o None si falla.

### 4. amongo(daba, film, filt={})

Inserta o actualiza un documento en MongoDB evitando duplicados.

Parámetros:
	daba (MongoDB Collection): Colección donde insertar.
	film (dict): Documento a insertar.
	filt (dict, opcional): Filtro para evitar duplicados.

### 5. correpeli(lin, nombre, cat)

Extrae datos de una película desde su página y los inserta en MongoDB.

Parámetros:
	lin (str): URL de la película.
	nombre (str): Título de la película.
	cat (str): Categoría de la película.

### 6. correpag(lin, cat)

Procesa todas las películas en una página específica de una categoría.

Parámetros:
	lin (str): URL de la página.
	cat (str): Categoría de las películas.

### 7. correcat(lin, cat_l)

Recorre todas las páginas de una categoría específica y procesa sus películas.

Parámetros:
	lin (str): URL de la categoría.
	cat_l (BeautifulSoup Tag): Elemento HTML que contiene el nombre de la categoría.

### 8. pelis_ingesta(url="https://www.blogdepelis.top/")

Inicia la extracción de datos desde el sitio web principal.

Parámetros:
	url (str, opcional): URL del sitio web.

### 9. cleanitem(item)

Limpia y normaliza datos extraídos de la API.

Parámetros:
	item (dict): Documento extraído de la API.
Retorno: Documento limpio y normalizado.

### 10. apide(b_url, opt)

Procesa datos obtenidos de la API.

Parámetros:
	b_url (str): URL base de la API.
	opt (dict): Parámetros para la solicitud.

### 11. api_ingesta(b_url="https://api.euskadi.eus/culture/events/v1.0/events/", opt={'_page': 1})

Inicia la extracción desde la API.

Parámetros:
	b_url (str, opcional): URL base de la API.
	opt (dict, opcional): Parámetros iniciales.

### 12. mongo(host='localhost', port=27017)

Conecta a MongoDB y configura las colecciones.

Parámetros:
	host (str, opcional): Dirección del servidor MongoDB.
	port (int, opcional): Puerto del servidor MongoDB.
Retorno: Cliente MongoDB, colección de películas (db_blog), colección de API (api_db).

### 13. main()

Controla el flujo principal del programa.
Tecnologías Utilizadas

Python Librerías Principales:
	requests: Para peticiones HTTP.
	BeautifulSoup: Para analizar HTML.
	threading: Para procesamiento concurrente.
	pymongo: Para interactuar con MongoDB.

## MongoDB:
Base de datos NoSQL para almacenamiento.

## Consideraciones de Diseño

### Resiliencia:
Manejo de errores HTTP con reintentos automáticos.
Registro de errores en un archivo de log.

### Optimización:
Evita duplicados en MongoDB.
Limpieza y normalización de datos antes de su almacenamiento.

### Modularidad:
Funciones independientes que facilitan su reutilización y mantenimiento.

## Mejoras Posibles

Implementar un sistema de cacheo para evitar repetir solicitudes HTTP.
Usar librerías asincrónicas como asyncio o aiohttp para manejar peticiones de manera más eficiente.
