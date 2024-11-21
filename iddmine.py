from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re

###################################
#Funcion para estructurar el log  #
###################################
def juntar(lista):
	tmp=''
	for item in lista:
		tmp+=item +" "
	while tmp[-1] == ' ':
		tmp=tmp[0:-2]
	tmp = tmp.replace('  ',': ')
	return tmp

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
def amongo(daba, film, filt={}):

	if daba.count_documents({}) !=0:
		if 'api' in daba.full_name:
			filt = {'nameEs': film['nameEs'],'startDate' : film['startDate']}
		else:
			filt = {'titulo': film['titulo'],'year': film['year'],'categoria': film['categoria']}

	try:
		daba.update_one(filt, {'$set' : film}, upsert=True)

	except:

		daba.insert_one(film)
		log(f'Error insertando {film}')


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

							tya = re.split('[\(:*)]',nombre)
							while len(tya) < 3:
								tya.append('')

							titulo = juntar(tya[0:-2])
							year = tya[-2].split()[0] if tya[-2] != '' else tya[-2]
							age = tya[-1]

							for cant in emo:
								nv = int(str(cant).replace('k','000').replace('.',''))
								emo[emo.index(cant)-1]=nv
							film = {
								'titulo': titulo,
								'year': int(year) if str(year) != '' else 2004,
								'categoria': catgry,
								'like': int(emo[0]),
								'dislike': int(emo[1]),
								'love': int(emo[2]),
								'shit': int(emo[3]),
								'vTotal':int(emo[0])+int(emo[1])+int(emo[2])+int(emo[3]),
								'link': link2,
								'pegi': False if age == '' else True,
								'sinopsis': sinopsis
							}
							#######################
							# Insertamos en mongo #
							#######################
							amongo(db_blog, film)
							print(db_blog.count_documents({}))


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
		ele = data['totalItems']
		tot = data['totalPages']
		opt = {
			'_elements': ele,
			'_page': 1,
			###########################################
			# tipo de evento "9 Audiovisuales y Cine" #
			###########################################

		}

		response = peticion(base_url, params=opt)
		if response is not None:
			data = response.json()
			for item in data['items']:
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

				###################################
				# insertamos en la base de datos  #
				###################################
				amongo(api_db, item)


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

def exit_program():
	print("Exiting the program...")
	mg.close()
	exit(0)

def main():
	########################
	# comienza el proceso  #
	########################
	url = "https://www.blogdepelis.top/"

	api_ingesta()

	pelis_ingesta(url)

	print("Programa Finalizado Correctamente")
	exit_program()

if __name__ == '__main__':
	#################################################
	#Inicializams las variables globales  			#
	#sé que no hace falta inicializarlas en python, #
	#es una práctica que conservo con cariño		#
	#################################################


	feat = 'html.parser'
	first = '$first'
	mg, db_blog, api_db = mongo()

	k = api_db.count_documents({})
	j = db_blog.count_documents({})
	############################################################
	# Empieza la magia (¿No es irónico que empiece al final? :)#
	############################################################

	main()

