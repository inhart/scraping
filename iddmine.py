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
					log(f'Petición exitosa para URL {url} en el intento {retries + 1}')
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
## Esta función realiza la primera conexión a la página y extrae las categorías #
#################################################################################
def pelis_ingesta(url="https://www.blogdepelis.top/"):
	response = peticion(url)
	if response is not None:
		soup = BeautifulSoup(response.content, feat)
		##########################################
		# extraemos las categorías de la sopa    #
		##########################################
		cat = soup.find_all('a')

		#############################
		# recorremos las categorías #
		#############################################################################################
		# como los links de categoría salen varias veces nos aseguramos de recorrerlas solo una vez #
		#############################################################################################
		cat_list = []
		for cate in cat:
			if ("category" in cate['href']) and (cate.get_text() not in cat_list):
				cat_list.append(cate.get_text())
				###############################
				# Este link es una categoria  #
				###############################
				newlink = cate['href']
				response2 = peticion(newlink)
				soup2 = BeautifulSoup(response2.content, feat)
				################################################################
				# extraemos la cantidad de páginas que tiene cada categoría    #
				################################################################
				pagen = soup2.find_all('a', 'page-numbers')
				pagen = int(pagen[-2].get_text())
				#######################
				# Y las recorremos    #
				#######################
				for i in range(1, pagen-1):
					#####################################
					# Para cada página en la categoría  #
					#####################################
					link1 = f'{newlink}/page/{i}'
					catgry = cate.get_text()
					###################################################
					# recorre las todas las páginas de la categoría   #
					###################################################
					response3 = peticion(link1)
					soup3 = BeautifulSoup(response3.content, feat)
					ases = soup3.find_all('div', 'latestPost-inner')
					for a in ases:
						####################################
						# Para cada película de la página  #
						####################################
						nombre = a.find('a')['title']
						link2 = a.find('a')['href']
						#################################################################
						# Esta función recorre la página de la peli y extrae los datos  #
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
							# adaptación a la variabilidad de la pagina #
							#############################################
							if len(scr) == 0:
								scr = soup4.find_all('p')[1]
								sinopsis = scr.get_text()
							else:
								sinopsis = scr[-1].get_text().split('\n')[0]
							if '(+18)' in nombre:
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
								'year': int(nombre[b:a]),
								'categorize': catgry,
								'like': int(emo[0]),
								'dislike': int(emo[1]),
								'love': int(emo[2]),
								'shit': int(emo[3]),
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


#####################################################
# Esta función hace una primera llamada a la api	#
# para obtener el total de páginas o eventos		#
# por qué vamos a seleccionar un evento por pagina	#
#####################################################
def api_ingesta():
	base_url = "https://api.euskadi.eus/culture/events/v1.0/events/"
	opt = {
		'_elements': 1,
		'_page': 1,
		###########################################
		# tipo de evento "9 Audiovisuales y Cine" #
		###########################################

	}
	response = peticion(base_url, params=opt)
	#########################################################
	#nos aseguramos de que la petición se hizo exitosamente #
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
						   'urlEventEu', 'urlNameEu','companyEu']
				for atom in keyname:
					try:
						del item[atom]
					except:
						continue
				nc=int(item['provinceNoraCode'])
				if nc == 48:
					item['provinceNoraES'] = 'Bizkaia'
				if nc == 46:
					item['provinceNoraES'] = 'Araba'
				if nc == 20:
					item['provinceNoraES'] = 'Gipuzkoa'
				item['year']=int(item['startDate'][0:4])
				try:
					if '€' in item['priceEs']:
						item['priceEs']=float(item['priceEs'][:-2].replace(',','.'))
					else:
						item['priceEs']=0
				except:
					item['priceEs']=0
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
	# uniendo los campos de categoría y guarda el elemento resultante en la db  #
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
	#   Consulta al usuario si quiere usar la dirección por defecto de la base de datos #
	#   En caso negativo, solicita meter ip y puerto									#
	#####################################################################################
	choice = input("¿Mongo usara la dirección y puerto por defecto? Y/N\n")
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
	#  IDD (Base de datos)---'blog' (Colección)						  #
	#	|															  #
	#	|															  #
	#'api' (Colección)												  #
	#																  #
	# crea la base de datos y las dos colecciones que usaremos        #
	###################################################################
	db = mg.idd
	blog = db['blog']
	api = db['api']
	return mg, blog, api


def main():
	########################
	# comienza el proceso  #
	########################
	ing = input('Quieres scrapear el blog o introducir una dirección alternativa? Y/N\n')
	if ing == 'N':
		url = input("introduce la nueva url\n")
	else:
		url = "https://www.blogdepelis.top/"
	api_ingesta()
	pelis_ingesta(url)

if __name__ == '__main__':
	#################################################
	#Inicializams las variables globales  			#
	#sé que no hace falta inicializarlas en python, #
	#es una práctica que conservo con cariño		#
	#################################################

	k = 0
	j = 0
	feat = 'html.parser'
	first = '$first'
	mg, db_blog, api_db = mongo()
	############################################################
	# Empieza la magia (¿No es irónico que empiece al final? :)#
	############################################################

	main()
	print("Programa Finalizado Correctamente")
