from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime





def peticion(url, params=None):
	#Realiza la peticion de la pagina web y devuelve la respuesta en caso de respuesta exitosa
	# retorna none en caso de error
	response = requests.get(url, params=params)
	if response.status_code == 200:
		return response
	else:
		log('Request error: ' + str(response.status_code))
		return None



def log(message):
	#Funcion para estructurar el log
	with open('log.txt','a') as f:
		try:
			f.write(f'{datetime.now()} '+ f' {message}\n')
		except Exception() as e:
			print(f'error al guardar el log {e}')



def amongo(db, film):
	#Funcion para insertar en la base de datos
	post = db.insert_one(film)
	return post



def incr():
	#Setter de contadores
	global j
	j+=1



def apide():
	# Setter de contadores
	global k
	k+=1



def filmscrapy(enlace, name, cat, db):
	# Esta funcion recorre la pagina de la peli y extrae los datos
	peli = peticion(enlace)
	soup4 = BeautifulSoup(peli.content, feat)
	scr = soup4.find_all('div', 'separator')
	react = soup4.find_all('span', "count-num")
	emo = []
	for num in react:
		num = num.get_text()
		emo.append(num)
	if len(scr) == 0:
		scr = soup4.find_all('p')[1]
		comentario = scr.get_text()
	else:
		comentario = scr[-1].get_text().split('\n')[0]

	film = {
		'_id': j,
		'titulo': name[:-7],
		'year': name[-5:-1],
		'categoria': cat,
		'like': emo[0],
		'dislike': emo[1],
		'love': emo[2],
		'shit': emo[3],
		'link': enlace,
		'comentario': comentario
	}
	#insertamos
	try:
		amongo(db, film)
	except Exception as e:
		print(f'error al insertar {e}')
	#cntador
	incr()



def correpaginas(db, enlace, cate):
	# esta funcion recorre las todas las paginas de la categoria
	response3 = peticion(enlace)
	soup3 = BeautifulSoup(response3.content, feat)
	ases = soup3.find_all('div', 'latestPost-inner')
	for a in ases:
		# Para cada pelicula de la pagina
		nombre = a.find('a')['title']
		link = a.find('a')['href']
		filmscrapy(link, nombre, cate, db)




def pelis_ingesta(db):
	# Esta funcion realiza la primera conexion a la pagina y extrae las categorias
	url = "https://www.blogdepelis.top/"
	response = peticion(url)
	soup = BeautifulSoup(response.content, feat)
	# extraemos las categorias de la sopa
	cat = soup.find_all('a')
	for cate in cat:
		#recorremos las categorias
		if "category" in cate['href']:
			# Este link es una categoria
			newlink = cate['href']
			response2 = peticion(newlink)
			soup2 = BeautifulSoup(response2.content, feat)
			# extraemos la cantidad de paginas que tiene cada categoria
			pagen = soup2.find_all('a', 'page-numbers')
			pagen = int(pagen[-2].get_text())
			# Y las recorremos
			for i in range(1,pagen):
				# Para cada pagina en la categoria
				link = f'{newlink}/page/{i}'
				catgry = cate.get_text()
				correpaginas(db, link, catgry)



def api_ingesta(db):
	#Esta funcion hace una primera llamada a la api
	# para obtener el total de paginas o eventos
	# ya que vamos a seleccionar un evento por pagina
	base_url = "https://api.euskadi.eus/culture/events/v1.0/events/"
	opt = {
		'_elements' : 1,
		'_page' : 1,
		'type': 9
	}
	response = peticion(base_url, params=opt)
	#nos aseguramos de que la peticion se hizo exitosamente
	if response != None:
		#pasamos el json de la respuesta
		data = response.json()
		# contamos los eventos que tendremos que atravesar
		tot = data['totalPages']
		for i in range(1, tot + 1):
			opt['_page'] = f'{i}'
			response = peticion(base_url, params=opt)
			if response != None:
				data = response.json()
				item = data['items'][0]
				# generamos un id unico para cada entrada
				item['_id'] = k
				# insertamos en la base de datos
				try:
					amongo(db, item)
				except Exception as e:
					print(f'{e}')
			#incrementamos el contador
			apide()




def main():
	# Conecta a MongoDB
	mg = MongoClient("localhost", 27017)
	# crea la base de datos y las dos tablas
	db = mg.IDD
	pelidb = db['blog']
	apidb = db['api']
# comienza el proceso
	api_ingesta(apidb)
	pelis_ingesta(pelidb)


if __name__ == '__main__':
#inicializams las variables globales
	k=0
	j=0
	feat = 'html.parser'
	main()