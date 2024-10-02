from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup


def peticion(url):
	response = requests.get(url)
	if response.status_code == 200:
		return response


def pelis_ingesta():
	dic = {}

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
					scr = soup4.find_all('meta')
					for sc in scr:
						contenido = sc['content']
						if "description" in contenido:
							print(sc)
					print(scr)


def api_ingesta():
	return " "


# Connect to MongoDB
mg = MongoClient("localhost", 27017)

apidb = mg.IDD['api']
pelidb = mg.IDD['peliculas']


peliculas = pelis_ingesta()



