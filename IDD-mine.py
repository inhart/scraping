
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd


def log(message):
	with open('log.txt','a') as f:
		try:
			f.write(f'{datetime.now()} '+ f' {message}\n')
		except:
			print('error al guardar el log')

def amongo(db, film):
	post = db.insert_one(film)
	print(post)


def save_to_csv(pelicula):

	with open('peliculas.csv', 'a', encoding='utf-8') as f:
		for dato in pelicula:
			try:
				if pelicula.index(dato) == (len(pelicula)-1):
					f.write('"' + f'{dato}' + '"' + '\n')
				else:
					f.write(f'{dato},')
			except:
				print('error en el csv\n')
				print(dato)

def peticion(url):
	response = requests.get(url)
	if response.status_code == 200:
		return response
	else:
		log('Request error: ' + str(response.status_code))
		return None


def pelis_ingesta(db):
	j = 0
	pelicula = []
	url = "https://www.blogdepelis.top/"
	response = peticion(url)
	soup = BeautifulSoup(response.content, "html.parser")

	cat = soup.find_all('a')
	for cate in cat:
		if "category" in cate['href']:
			# This link is a category
			newlink = cate['href']
			response2 = peticion(newlink)
			soup2 = BeautifulSoup(response2.content, 'html.parser')
			pagen = soup2.find_all('a', 'page-numbers')
			pagen = int(pagen[-2].get_text())
			for i in range(1,pagen):
				# For each page in the category
				link3 = f'{newlink}/page/{i}'
				response3 = peticion(link3)
				soup3 = BeautifulSoup(response3.content,'html.parser')
				ases = soup3.find_all('div','latestPost-inner')
				for a in ases:
					# For each movie in the page
					nombre = a.find('a')['title']
					link4 = a.find('a')['href']
					peli = peticion(link4)
					soup4 = BeautifulSoup(peli.content,'html.parser')
					scr = soup4.find_all('div','separator')
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
						'titulo': nombre[:-6],
						'ano' : nombre[-5:-1],
						'categoria' : cate.get_text(),
						'like': emo[0],
						'dislike': emo[1],
						'love': emo[2],
						'shit': emo[3],
						'comentario' : comentario
					}
					#save_to_csv([titulo, ano, c, like, dislike, love, shit, comentario])
					print(film)
					amongo(db, film)
					j+=1


	#return pelicula





def api_ingesta():
	return " "


# Connect to MongoDB
mg = MongoClient("localhost", 27017)

apidb = mg.IDD['api']
pelidb = mg.IDD['blog']

pelis_ingesta(pelidb)
