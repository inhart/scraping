from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re
import threading

#############################################
#Funcion para unir en una cadena una lista  #
#############################################
def juntar(lista):
	tmp=''
	for item in lista:
		tmp+=item +" "
	while tmp[-1] == ' ':
		tmp=tmp[0:-2]
	tmp = tmp.replace('  ',': ')
	return tmp
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

def peticion(url, params=None, max_retries=5, base_wait_time=10):
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
def amongo(daba, film, filt={}):

	if daba.count_documents({}) != 0:
		if 'api' in daba.full_name:
			filt = {'nameEs': film['nameEs'], 'startDate': film['startDate']}
		else:
			filt = {'titulo': film['titulo'], 'year': film['year'], 'categoria': film['categoria']}
	################################################################
	# con esta funcion nos aseguramos de no introducir duplicados  #
	################################################################
	up = daba.update_one(filt, {'$set': film}, upsert=True)

	ok = int(up.raw_result['ok'])
	if ok == 1:
		return
	else:
		daba.insert_one(film)
		log(f'Error insertando {film}')

def aggregate():
	db_blog.aggregate([
		{
			'$group': { #Agrupa portodo menos categoria,
				'_id': {
					'titulo': '$titulo',
					'year': '$year',
					'shit': '$shit',
					'dislike': '$dislike',
					'like': '$like',
					'love': '$love',
					'sinopsis': '$sinopsis',
					'pegi': '$pegi',
					'vTotal': '$vTotal'
				},#las categorias iran encadenandose en un array
				'categorias': {
					'$addToSet': '$categoria'
				}
			}
		}, { # saca la coleccion sin duplicados
			'$project': {
				'_id': 0,
				'titulo': '$_id.titulo',
				'year': '$_id.year',
				'shit': '$_id.shit',
				'dislike': '$_id.dislike',
				'like': '$_id.like',
				'love': '$_id.love',
				'sinopsis': '$_id.sinopsis',
				'pegi': '$_id.pegi',
				'vTotal': '$_id.vTotal',
				'categoria': '$categorias'
			}
		}, {#lo guarda en una nueva coleccion
			'$merge': {
				'into': 'blog_retocado',
				'whenMatched': 'replace',
				'whenNotMatched': 'insert'
			}
		}
	])

def correpeli(lin, nombre, cat):
	#################################################################
	# Esta funcion recorre la página de la peli y extrae los datos  #
	#################################################################
	peli = peticion(lin)
	if peli is None:
		return
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

	tya = re.split('[(:*)]', nombre)
	while len(tya) < 3:
		tya.append('')

	titulo = juntar(tya[0:-2])
	year = tya[-2].split()[0] if tya[-2] != '' else tya[-2]
	age = tya[-1]

	for cant in emo:
		nv = int(str(cant).replace('k', '000').replace('.', ''))
		emo[emo.index(cant) - 1] = nv
	film = {
		'titulo': titulo,
		'year': int(year) if str(year) != '' else 2004,
		'categoria': cat,
		'like': int(emo[0]),
		'dislike': int(emo[1]),
		'love': int(emo[2]),
		'shit': int(emo[3]),
		'vTotal': int(emo[0]) + int(emo[1]) + int(emo[2]) + int(emo[3]),
		'link': lin,
		'pegi': False if age == '' else True,
		'sinopsis': sinopsis
	}
	#######################
	# Insertamos en mongo #
	#######################
	
	amongo(db_blog, film)
	print('1 ' + titulo)


def correpag(lin, cat):
	###################################################
	# recorre las todas las paginas de la categoría   #
	###################################################
	#Creamos una lista que contendra nuestros hilos   #
	###################################################
	threads = []
	response = peticion(lin)
	soup3 = BeautifulSoup(response.content, feat)
	ases = soup3.find_all('div', 'latestPost-inner')

	for a in ases:

		####################################
		# Para cada película de la pagina  #
		####################################
		nombre = a.find('a')['title']
		link = a.find('a')['href']
		#creamos un hilo por pelicula y continuamos
		x = threading.Thread(target=correpeli, args=(link, nombre, cat,), daemon=True)

		threads.append(x)
		x.start()

		if len(threads) >= 100:
			for thread in threads:
				if thread.is_alive():
					thread.join()
				else:
					threads.remove(thread)
		#correpeli(link, nombre, cat)

# print(db_blog.count_documents({}))
def correcat(lin, cat_l):
	response = peticion(lin)
	soup = BeautifulSoup(response.content, feat)
	################################################################
	# extraemos la cantidad de páginas que tiene cada categoría    #
	################################################################
	pagen = soup.find_all('a', 'page-numbers')
	try:
		pagen = int(pagen[-2].get_text())
	except IndexError:
		pagen = 2
	#######################
	# Y las recorremos    #
	#######################
	for i in range(1, pagen+1):
		#####################################
		# Para cada pagina en la categoría  #
		#####################################
		link1 = f'{lin}/page/{i}'
		catgry = cat_l.get_text()
		correpag(link1, catgry)


#################################################################################
## Esta función realiza la primera conexión a la página y extrae las categorías #
#################################################################################
def pelis_ingesta(url="https://www.blogdepelis.top/"):

	response = peticion(url)
	if response is None:
		return
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
			correcat(newlink, cate)
	#########################################################
	# Aggregacion que limpia la base de datos de duplicados #
	# unificando las peliculas con varias categorias		#
	#########################################################
	aggregate()

def cleanitem(item):
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
			   'urlEventEu', 'urlNameEu', 'companyEu']
	for atom in keyname:
		try:
			del item[atom]
		except KeyError:
			continue

	nc = int(item['provinceNoraCode'])
	if nc == 48:
		item['provinceNoraES'] = 'Bizkaia'
	if nc == 46:
		item['provinceNoraES'] = 'Araba'
	if nc == 20:
		item['provinceNoraES'] = 'Gipuzkoa'
	item['year'] = int(item['startDate'][0:4])
	precio = 0.0
	try:
		if 'http' in item['priceEs']:
			raise ValueError
		precio = re.findall('(\d+\.*\d+)', item['priceEs'])
		if len(precio) != 0:
			precio = precio[0].replace(',', '.')
		else:
			precio = 0.0
	except KeyError:
		precio = 0.0
	except ValueError:
		precio = 0.0
	finally:
		item['priceEs'] = float(precio)
	return item

def apide(b_url, opt):
	response = peticion(b_url, params=opt)
	if response is None:
		return
	data = response.json()
	for item in data['items']:
		item = cleanitem(item)

		###################################
		# insertamos en la base de datos  #
		###################################
		amongo(api_db, item)
		print("1 "+ item['nameEs'])



#####################################################
# Esta función hace una primera llamada a la api	#
# para obtener el total de páginas o eventos		#
# por qué vamos a seleccionar un evento por pagina	#
#####################################################
def api_ingesta(b_url = "https://api.euskadi.eus/culture/events/v1.0/events/",opt = {'_page': 1}):

	response = peticion(b_url, params=opt)
	####################################
	#pasamos el json de la respuesta   #
	####################################
	if response is None:
		return
	#########################################################
	#nos aseguramos de que la petición se hizo exitosamente #
	#########################################################
	data = response.json()
	######################################################
	# contamos los eventos que tendremos que atravesar   #
	######################################################
	ele = data['totalItems']

	opt = {
		'_elements': ele,
		'_page': 1,
		###########################################
		# tipo de evento "todos" #
		###########################################

	}
	apide(b_url,opt)


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
	#	|						|									  #
	#	|						|									  #
	#'api' (Colección)			blog_retocado									  #
	#																  #
	# crea la base de datos y las dos colecciones que usaremos        #
	###################################################################
	db = mg.idd

	blog = db['blog']

	api = db['api']
	return mg, blog, api

def exit_program():
	print("Exiting the program...")

	exit(0)

def main():
	########################
	# comienza el proceso  #
	########################
	url = "https://www.blogdepelis.top/"
	apiurl = "https://api.euskadi.eus/culture/events/v1.0/events/"

	pelis_ingesta(url)

	api_ingesta(apiurl)

	print("Programa Finalizado Correctamente")
	exit_program()

if __name__ == '__main__':
	#################################################
	#Inicializams las variables globales  			#
	#################################################

	i,f = 0 , 0
	
	feat = 'html.parser'
	first = '$first'
	mg, db_blog, api_db = mongo()
	############################################################
	# Empieza la magia (¿No es irónico que empiece al final? :)#
	############################################################

	main()

