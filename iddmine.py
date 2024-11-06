from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time


###################################
#Funcion para estructurar el log  #
###################################

def log(message):
	with open('log.txt', 'a') as f:
		try:
			f.write(f'{datetime.now()}: {message}\n')
		except Exception() as e:
			print(f'error al guardar el log {e}')


#########################################
# Gestiona las peticiones a las paginas #
#########################################

def peticion(url, params=None, max_retries=3, base_wait_time=30):
	retries = 0

	while retries <= max_retries:
		try:
			response = requests.get(url, params=params, timeout=10)

			if response.status_code == 200:
				if retries > 0:
					log(f'Peticion exitosa para URL {url} en el intento {retries + 1}')
				return response
			else:
				log(f'HTTP error: {response.status_code} en {url}')
				break


		except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
			log(f'Error: {e} en intento {retries + 1} para URL: {url}')

		######################################################################
		# Exponential backoff: espera base_wait_time * 2^retries segundos    #
		######################################################################

		wait_time = base_wait_time * (2 ** retries)
		retries += 1
		time.sleep(wait_time)
	log(f'Fallo al conectar después de {max_retries} intentos: {url}')
	return None


############################################
#Funcion para insertar en la base de datos #
############################################
def amongo(daba, film):
	post = daba.insert_one(film)
	return post


#########################
#Setter de contadores	#
#########################
def incr():
	global j
	j += 1


########################
# Setter de contadores #
########################
def apide():
	global k
	k += 1


#################################################################################
## Esta funcion realiza la primera conexion a la pagina y extrae las categorias #
#################################################################################
def pelis_ingesta(url="https://www.blogdepelis.top/"):
	response = peticion(url)
	if response is not None:
		soup = BeautifulSoup(response.content, feat)
		##########################################
		# extraemos las categorias de la sopa    #
		##########################################
		cat = soup.find_all('a')

		#############################
		# recorremos las categorias  #
		#############################################################################################
		# como los links de categoria salen varias veces nos aseguramos de recorrerlas solo una vez #
		#############################################################################################
		cat_list = []
		for cate in cat:

			if "category" in cate['href']:
				if cate.get_text() not in cat_list:
					cat_list.append(cate.get_text())
					###############################
					# Este link es una categoria  #
					###############################
					newlink = cate['href']
					response2 = peticion(newlink)
					soup2 = BeautifulSoup(response2.content, feat)
					################################################################
					# extraemos la cantidad de paginas que tiene cada categoria    #
					################################################################
					pagen = soup2.find_all('a', 'page-numbers')
					pagen = int(pagen[-2].get_text())
					#######################
					# Y las recorremos    #
					#######################
					for i in range(1, pagen):
						#####################################
						# Para cada pagina en la categoria  #
						#####################################
						link1 = f'{newlink}/page/{i}'
						catgry = cate.get_text()
						###################################################
						# recorre las todas las paginas de la categoria   #
						###################################################
						response3 = peticion(link1)
						soup3 = BeautifulSoup(response3.content, feat)
						ases = soup3.find_all('div', 'latestPost-inner')
						for a in ases:
							####################################
							# Para cada pelicula de la pagina  #
							####################################
							nombre = a.find('a')['title']
							link2 = a.find('a')['href']
							#################################################################
							# Esta funcion recorre la pagina de la peli y extrae los datos  #
							#################################################################
							peli = peticion(link2)
							if peli is not None:
								soup4 = BeautifulSoup(peli.content, feat)
								scr = soup4.find_all('div', 'separator')
								react = soup4.find_all('span', "count-num")
								emo = []
								for num in react:
									num = num.get_text()
									emo.append(num)
								#############################################
								# adaptacion a la variabilidad de la pagina #
								#############################################
								if len(scr) == 0:
									scr = soup4.find_all('p')[1]
									sinopsis = scr.get_text()
								else:
									sinopsis = scr[-1].get_text().split('\n')[0]
								if f'(+18)' in nombre:
									a = -7
									b = -11
									c = -13
									age = True
								else:
									a = -1
									b = -5
									c = -7
									age = False
								film = {
									'_id': j,
									'titulo': nombre[:c],
									'year': nombre[b:a],
									'categoria': catgry,
									'like': emo[0],
									'dislike': emo[1],
									'love': emo[2],
									'shit': emo[3],
									'link': link2,
									'pegi': age,
									'sinopsis': sinopsis
								}
								#######################
								# Insertamos en mongo #
								#######################
								try:
									amongo(db_blog, film)
								except Exception as e:
									print(f'error al insertar {e}')
									log(f'error al insertar\n {e}')
								###########################
								# contador incrementar    #
								###########################
								incr()


######################################################
# Esta funcion hace una primera llamada a la api     #
# para obtener el total de paginas o eventos         #
# ya que vamos a seleccionar un evento por pagina    #
######################################################
def api_ingesta():
	base_url = "https://api.euskadi.eus/culture/events/v1.0/events/"
	opt = {
		'_elements': 1,
		'_page': 1,
		###########################################
		# tipo de evento "9 Audiovisuales y Cine" #
		###########################################
		'type': 9
	}
	response = peticion(base_url, params=opt)
	#########################################################
	#nos aseguramos de que la peticion se hizo exitosamente #
	#########################################################
	if response is not None:
		####################################
		#pasamos el json de la respuesta   #
		####################################
		data = response.json()
		######################################################
		# contamos los eventos que tendremos que atravesar   #
		######################################################
		tot = data['totalPages']
		for i in range(1, tot + 1):
			opt['_page'] = f'{i}'
			response = peticion(base_url, params=opt)
			if response is not None:
				data = response.json()
				item = data['items'][0]
				############################################
				# generamos un id unico para cada entrada  #
				############################################
				del item['id']
				#############################################
				# Limpiamos las entradas en euskera         #
				#############################################
				keyname = ['typeEu', 'nameEu', 'openingHoursEu',
						   'sourceNameEu', 'sourceUrlEu', 'priceEu',
						   'purchaseUrlEu', 'descriptionEu',
						   'municipalityEu', 'establishmentEu',
						   'urlEventEu', 'urlNameEu']
				for atom in keyname:
					try:
						del item[atom]
					except Exception as e:
						#log(f'error {e} al borrar la entrada {keyname}')
						continue

				item['_id'] = k
				###################################
				# insertamos en la base de datos  #
				###################################
				try:
					amongo(api_db, item)
				except Exception as e:
					print(f'error al insertar\n {e}')
					log(f'error al insertar\n {e}')
			#incrementamos el contador
			apide()


def limpiar_coleccion(elem):
	#############################################################################
	# localiza los elementos duplicados, los combina en un solo elemento,       #
	# uniendo los campos de categoria y guarda el elemento resultante en la db  #
	#############################################################################
	elem.aggregate(
		[
			{
				'$group': {
					'_id': {
						first: '$_id'
					},
					'titulo': {
						first: '$titulo',
					},
					'link': {
						first: '$link'
					},
					'categoria': {
						'$addToSet': '$categoria'
					},
					'love': {
						first: '$love'
					},
					'year': {
						first: '$year'
					},
					'like': {
						first: '$like'
					},
					'shit': {
						first: '$shit'
					},
					'dislike': {
						first: '$dislike'
					},
					'sinopsis': {
						first: '$sinopsis'
					}
				}
			}, {
			'$out': 'blog'
		}
		]
	)


def mongo(host='localhost', port=27017):
	#####################################################################################
	#   Consulta al usuario si quiere usar la direccion por defecto de la base de datos #
	#   En caso negativo, solicita meter ip y puerto									#
	#####################################################################################
	choice = input("¿Mongo usara la direccion y puerto por defecto? Y/N\n")
	if choice == 'N':
		dire = input("introduce IP base de datos\n")
		por = input("introduce puerto\n")
	else:
		dire = host
		por = port
	######################
	# Conecta a MongoDB  #
	######################
	mg = MongoClient(dire, por)
	###################################################################
	#  IDD ( Base de datos )---'blog' ( Coleccion )					  #
	#	|															  #
	#	|															  #
	#'api' ( Coleccion )											  #
	#																  #
	# crea la base de datos y las dos colecciones que usaremos        #
	###################################################################
	db = mg.idd
	db_blog = db['blog']
	api_db = db['api']
	return mg, db_blog, api_db


def main():
	########################
	# comienza el proceso  #
	########################
	ing = input('Quieres scrapear el blog o introducir una direccion alternativa? S/I\n')
	if ing == 'I':
		url = input("introduce la nueva url\n")
	else:
		url = "https://www.blogdepelis.top/"
	pelis_ingesta(url)
	api_ingesta()


#limpiar_coleccion(db_blog)


if __name__ == '__main__':
	#################################################
	#Inicializams las variables globales  			#
	#se que no hace falta iniciarlizarlas en python #
	#pero es una practica que conservo con cariño	#
	#################################################

	k = 0
	j = 0
	feat = 'html.parser'
	first = '$first'
	mg, db_blog, api_db = mongo()
	############################################################
	# Empieza la magia (No es ironico que empiece al final? :)#
	############################################################

	main()
	print("Programa Finalizado Correctamente")
