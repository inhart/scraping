from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime

def peticion(url, params=None):
	response = requests.get(url, params=params)
	if response.status_code == 200:
		return response
	else:
		log('Request error: ' + str(response.status_code))
		return None

def log(message):
	with open('log.txt','a') as f:
		try:
			f.write(f'{datetime.now()} '+ f' {message}\n')
		except Exception() as e:
			print(f'error al guardar el log {e}')

def amongo(db, film):
	post = db.insert_one(film)
	return post

def incr():
	global j
	j+=1

def apide():
	global k
	k+=1



def filmscrapy(enlace, name, cat, db, feat):
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
	try:
		amongo(db, film)
	except Exception as e:

		print(f'error al insertar {e}')

	incr()
	#print(film['_id'])


def correpaginas(db, enlace, cate, feat='html.parser'):

	response3 = peticion(enlace)
	soup3 = BeautifulSoup(response3.content, feat)
	ases = soup3.find_all('div', 'latestPost-inner')
	for a in ases:
		# Para cada pelicula de la pagina
		nombre = a.find('a')['title']
		link = a.find('a')['href']
		filmscrapy(link, nombre, cate, db, feat)




def pelis_ingesta(db, feat):

	url = "https://www.blogdepelis.top/"
	response = peticion(url)
	soup = BeautifulSoup(response.content, feat)
	cat = soup.find_all('a')
	for cate in cat:
		if "category" in cate['href']:
			# Este link es una categoria
			newlink = cate['href']
			response2 = peticion(newlink)
			soup2 = BeautifulSoup(response2.content, feat)
			pagen = soup2.find_all('a', 'page-numbers')
			pagen = int(pagen[-2].get_text())
			for i in range(1,pagen):
				# Para cada pagina en la categoria
				link = f'{newlink}/page/{i}'
				catgry = cate.get_text()
				correpaginas(db, link, catgry, feat)

def api_ingesta(db):
	base_url = "https://api.euskadi.eus/culture/events/v1.0/events/"
	opt = {
		'_elements' : 1,
		'_page' : 1,
		'type': 9
	}
	response = peticion(base_url, params=opt)
	if response != None:
		data = response.json()

		tot = data['totalPages']
		for i in range(1, tot + 1):
			opt['_page'] = f'{i}'
			response = peticion(base_url, params=opt)
			if response != None:
				data = response.json()
				item = data['items'][0]
				item['_id'] = k
				try:
					amongo(db, item)
				except Exception as e:
					print(f'{e}')

			apide()




def main():
	# Conecta a MongoDB
	mg = MongoClient("localhost", 27017)
	feat = 'html.parser'
	pelidb = mg.IDD['blog']
	apidb = mg.IDD['api']

	api_ingesta(apidb)

	pelis_ingesta(pelidb, feat)


if __name__ == '__main__':

	k=0
	j=0

	main()