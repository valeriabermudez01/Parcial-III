import json
import boto3
import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd

s3 = boto3.client('s3')

date = datetime.datetime.now()
year = date.year
month = date.month
day = date.day
url = "headlines/raw/periodico=El tiempo/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/El tiempo.txt"
url1 = "headlines/raw/periodico=Publimetro/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/Publimetro.txt"
response1 = s3.get_object(Bucket="parcial3bigdatavalsanti",Key=url)
data1 = response1['Body'].read()
print("hola", data1)
response = s3.get_object(Bucket="parcial3bigdatavalsanti",Key=url1)
data = response['Body'].read()
print("hola", data)

e = requests.get('https://www.eltiempo.com').text
#print(e)

soup = BeautifulSoup(data1,'html.parser')
link= list()
titulo= list()
categoria=list()
categoria1=list()
for a in soup.find_all('a',class_="title page-link",href=True):
    categoria.append(str(a['href']))
    link.append("https://www.eltiempo.com"+str(a['href']))

for a in soup.find_all('a',class_="title page-link"):
    titulo.append(str(a.text))

for a in range(len(categoria)):
    categoria1.append(categoria[a].split(sep='/')[1])

print(len(titulo))
for a in range(len(titulo)):
    print(titulo[a])
    print(link[a])
    print(categoria1[a])

dict1 = {'titulo': titulo, 'categoria':categoria1,'link':link}

soup = BeautifulSoup(data,'html.parser')

cat_publi = soup.find_all('h4' , class_ = 'primary-font__PrimaryFontStyles-o56yd5-0 jqfNWj header-block')

categorias_publimetro = list()
for f in cat_publi:
    categorias_publimetro.append(f.text)
print(categorias_publimetro)

tit_publi = soup.find_all('h2' , class_ = 'primary-font__PrimaryFontStyles-o56yd5-0 jqfNWj sm-promo-headline')

titulares_publimetro = list()
for titular in tit_publi:
  tit = titular.find_all('a')
  for t in tit:
    titulares_publimetro.append(t.text)
print(titulares_publimetro)

links_publi = soup.find_all('h2' , class_ = 'primary-font__PrimaryFontStyles-o56yd5-0 jqfNWj sm-promo-headline')

enlaces_publimetro = list()
for e in links_publi:
  links = e.find_all('a')
  for l in links:
    enlaces_publimetro.append(l.get('href'))
print(enlaces_publimetro)

categorias_publimetro1 = list()

for a in range(len(titulares_publimetro)):
    if (a<len(categorias_publimetro)):
        categorias_publimetro1.append(categorias_publimetro[a])
    else:
        categorias_publimetro1.append("Noticias")

dict= {'titulo':titulares_publimetro, 'categoria':categorias_publimetro1,'link':enlaces_publimetro}

df = pd.DataFrame(dict) 
df.to_csv('/tmp/publimetro.csv', index = False)

df1 = pd.DataFrame(dict1) 
df1.to_csv('/tmp/tiempo.csv', index = False)
url11 = "headlines/final/periodico=El Tiempo/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/Eltiempo.csv"
s3.upload_file("/tmp/tiempo.csv","parcial3bigdatavalsanti",url11)
url12 = "headlines/final/periodico=Publimetro/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/publimetro.csv"
s3.upload_file("/tmp/publimetro.csv","parcial3bigdatavalsanti",url12)

