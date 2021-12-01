import requests
import json
import boto3
import time

def descargarPagina(name, url, localtime, bucketname, s3):	
	headers = {'User-Agent': 'Mozilla'}
	r = requests.get(url, headers=headers)
	filepath="/tmp/"+name+".txt"
	f = open(filepath,"w")
	print("Saving file from "+name)
	f.write(r.text)
	f.close()
	path = 'headlines/raw/periodico='+name+'/year='+str(localtime.tm_year)+'/month='+str(localtime.tm_mon)+'/day='+str(localtime.tm_mday)+'/'+name+'.txt'
	s3.meta.client.upload_file(filepath, bucketname, path)
	
bucket="parcial3bigdatavalsanti"

localtime=time.localtime()
s3 = boto3.resource('s3')
    
descargarPagina('El tiempo', 'https://www.eltiempo.com/' , localtime, bucket, s3)
descargarPagina('Publimetro', 'https://www.publimetro.co/' , localtime, bucket, s3)