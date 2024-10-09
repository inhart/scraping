




Ingesta de datos

Inhart de Orube
Index
Descripción General	3
Requisitos Previos	3
Estructura del Proyecto	4
Base de Datos MongoDB	4
Detalles de Implementación	4
 - MongoDB:	4
 - Librerías utilizadas:	4
 - 'requests':	4
 - 'BeautifulSoup':	4
 - 'pymongo':	5
 - 'datetime':	5
Funcionalidades y Estructura del Código	5
 1. peticion(url, params=None):	5
 2. log(message):	5
 3. amongo(daba, film):	5
 4. pelis_ingesta():	6
5. api_ingesta():	6
6. limpiar_coleccion(elem):	6
7. main():	6
Ejecución del Proyecto	7
1. Instalación de dependencias:	7
2. Ejecución del código:	7
Recursividad y Manejo de Errores	8
Posibles Mejoras	8
Conclusión	8



Descripción General

	El proyecto se centra en la ingesta y procesamiento de datos de películas y eventos culturales a través de scraping web y llamadas a APIs. El objetivo es recolectar información de diferentes fuentes y almacenarla en una base de datos MongoDB para facilitar su posterior análisis y consulta. El sistema está diseñado para interactuar con dos fuentes principales:


	1. Un blog de películas (scraping web).

	2. Una API pública que proporciona información sobre eventos audiovisuales y cinematográficos.

	El código está organizado para ejecutar estos procesos de ingesta y almacenarlos en dos colecciones separadas dentro de MongoDB: 'blog' y 'api'.

Requisitos Previos


	- Python 3.x: El proyecto está desarrollado en Python, por lo que se requiere una versión moderna del lenguaje.
	-MongoDB: El sistema debe tener un servidor MongoDB corriendo en 'localhost' en el puerto '27017'.
	- Bibliotecas Python: Las dependencias necesarias se encuentran en el archivo ‘requirements.txt’. Para instalarlas, ejecuta:
  	pip install -r requirements.txt

Estructura del Proyecto
	
	- ’iddmine.py’: Archivo principal que contiene el código para la ingesta de datos.
	
	- 'log.txt’: Archivo de log donde se registran errores y eventos durante la ejecución del programa.
	
Base de Datos MongoDB

	El proyecto utiliza una base de datos MongoDB llamada 'IDD' con dos colecciones principales:
  
	- 'blog': Para almacenar la información de películas obtenida mediante scraping web.

  	- 'api': Para almacenar la información de eventos audiovisuales obtenida desde la API.

Detalles de Implementación

	- MongoDB:

	El proyecto utiliza MongoDB para almacenar los datos recolectados. Se conecta a un servidor MongoDB local en 'localhost:27017'.
	
	- Librerías utilizadas:

	- 'requests':
		Para realizar peticiones HTTP tanto en el scraping web como en las llamadas a la API.
	- 'BeautifulSoup':
		Para parsear y extraer datos de las páginas HTML durante el scraping.
	- 'pymongo':
		Para la interacción con MongoDB.
	- 'datetime':
		Para registrar fechas y horas en los logs.
Funcionalidades y Estructura del Código

	1. peticion(url, params=None):
   - Realiza una solicitud 'GET' a la URL especificada.
   - Si la respuesta es exitosa ('status_code = 200'), retorna el contenido; de lo contrario, se registra el error en el archivo 'log.txt'.

	2. log(message):
   - Escribe mensajes de log en el archivo 'log.txt' para documentar errores y eventos.

	3. amongo(daba, film):
   - Inserta un documento ('film') en la colección MongoDB especificada ('daba').


	4. pelis_ingesta():
   - Realiza la conexión inicial con la página principal del blog para extraer las categorías de películas disponibles para navegar y extraer los datos de cada categoría.   
- Realiza scraping de una página de película específica para extraer datos como el título, año, categoría, reacciones (likes, dislikes, etc.), sinopsis y enlace.
- Navega por todas las páginas de una categoría específica del blog de películas y extrae la información de cada película.
- Inserta los datos extraídos en la colección 'blog'.

5. api_ingesta():
   	- Realiza solicitudes a la API de eventos culturales y almacena la información en la colección 'api' de MongoDB.
   	- Extrae información como el tipo de evento y el total de páginas, y recorre cada evento para almacenarlo en la base de datos.

6. limpiar_coleccion(elem):
	- Identifica duplicados en la colección especificada y los combina en un solo elemento, uniendo las categorías y consolidando los datos en un documento único. Al final, guarda los elementos resultantes en la colección 'blog'.

7. main():
	- Función principal que inicia el proceso de ingesta de datos y limpieza de la base de datos.


Ejecución del Proyecto

Para ejecutar el proyecto, sigue los siguientes pasos:

1. Instalación de dependencias:

   pip install -r requirements.txt

2. Ejecución del código:
  
   python iddmine.py
   
   Alternativamente, el archivo puede ejecutarse desde un editor de Python compatible.


Recursividad y Manejo de Errores
  	
	- El límite de recursividad se incrementa considerablemente para manejar las operaciones masivas y las peticiones múltiples.
  	
	- El sistema de logging registra cualquier error en 'log.txt' para facilitar la depuración.

Posibles Mejoras

	- Optimización del Scraping: Implementar un sistema de cacheo para evitar solicitar repetidamente la misma página y acelerar el procesamiento.
	
	- Paralelización: Utilizar threading o asyncio para paralelizar las peticiones HTTP y mejorar la eficiencia del scraping.

Conclusión

	Este proyecto de ingesta de datos es una herramienta potente para recolectar y centralizar información de diferentes fuentes en MongoDB. El código está diseñado para ser modular y escalable, facilitando la integración de nuevas fuentes de datos y la mejora de las funcionalidades existentes.