import boto3
import json
import zlib
from datetime import datetime
import os
import gc

def define_source_destiny(dataset, bucket, key, s3):
	obj = s3.Object(bucket, key)
	dataset_spec = json.loads(obj.get()['Body'].read().decode('utf-8'))
	return dataset_spec[ dataset ]

# la funcion debe obtener el path, obtener el format del date, reemplazarlo en el path utilizando la fecha presente en el nombre del archivo

def uncompress_chunk(s3, bucket_origen, key_origen, bucket_destino, path_destino, dec):
	i = 0
	for chunk in s3.get_object(Bucket=bucket_origen, Key=key_origen)["Body"].iter_chunks(10 * 1024 * 1024):
		print(str(datetime.now()) + ": Downloaded chunk: " + str(i))
		data = dec.decompress(chunk)
		print(len(chunk), len(data))
		try:
			print(str(datetime.now()) +": decompressed chunk: " + str(i))
			response = s3.put_object(
				Body=data,
				Bucket=bucket_destino,
				Key=path_destino + "/" + key_origen.split("/")[-1].replace(".dat.gz", "-") + str(i) + ".dat",
			)
			del chunk
			del data
			gc.collect()
		except Exception as ex:
		   return str(ex)
		i += 1
	return "File uncompressed and uploaded"


def copy_file(s3, bucket_origen, key_origen, bucket_destino, path_destino):
	copy_source = {
	'Bucket': bucket_origen,
	'Key': key_origen
	}
 	try:
		s3.meta.client.copy(copy_source, bucket_destino, path_destino + key_origen.split("/")[-1])
		return "SUCCESS"
	except Exception as ex:
		return str(ex)


def delete_file(s3, bucket_origen, key_origen):
	obj = s3.Object(bucket_origen, key_origen)
	try:
		obj.delete()
		return "Success deleting {}/{}".format(bucket_origen, key_origen)
	except Exception as ex:
		return str(ex)


def move_file(s3, bucket_origen, key_origen, bucket_destino, path_destino):
	result_copy = copy_file(s3, bucket_origen, key_origen, bucket_destino, path_destino)
	if result_copy != "SUCCESS":
		return result_copy
	result_delete = delete_file(s3, bucket_origen, key_origen)
	if result_delete != "SUCCESS"
		return result_delete


def lambda_handler(event, context):
	# Obtiene nombre del dataset basado en el nombre del archivo que ha sido subido a s3
	# test_folder/maeterm_20211004.dat.gz
	# "test_folder/maeterm_20211004.dat.gz".split("/")[-1] = "maeterm_20211004.dat.gz" - "maeterm_20211004.dat.gz".split("_")[0] = maeterm
	dataset_name = event["Records"][0]["s3"]["object"]["key"].split("/")[-1].split("_")[0].lower()
	# Obtiene el bucket de origen utilizando la data del evento que ha sido subido a s3
	bucket_origen = event["Records"][0]["s3"]["bucket"]["name"]
	# Obtiene el key del objeto de origen
	# test_folder/maeterm_20211004.dat.gz
	key_origen = event["Records"][0]["s3"]["object"]["key"]
	# Debe obtenerse desde variable de entorno
	artifact_bucket = os.environ["BUCKET_ARTIFACTORY"]
	# Debe obtenerse desde variable de entorno
	config_key = "s3_sync_test/nombramiento.json"
	# obtejo que permite descomprimir
	dec = zlib.decompressobj(32+zlib.MAX_WBITS)
	s3 = boto3.client('s3')
	s3_r = boto3.resource('s3')
	# Obtiene la informacion relevante del dataset del objeto que ha sido subido a s3
	dataset_spec = define_source_destiny(dataset_name, artifact_bucket, config_key, s3_r)
	# Obtiene la fecha desde el nombre del archivo que ha sido subido a s3
	# Ex: maeterm_20210101.dat.gz => 20210101
	file_date = event["Records"][0]["s3"]["object"]["key"].split("/")[-1].split("_")[1].split(".")[0] # OBTENER FECHA DESDE EL NOMBRE DEL ARCHIVO
	dt_format = ""
	# definicion de date_format, TEMPORAL, debería venir definido en el json de configuracion
	if len(file_date) == 12: # DEFINE DATE FORMAT EN BASE A LA CANTIDAD DE CARACTERES DE LA FECHA QUE VIENE EN EL ARCHIVO
		dt_format = "%Y%m%d%H%M"
	elif len(file_date) == 8:
		dt_format = "%Y%m%d"
	print(dt_format)
	# utiliza la fecha obtenida desde el nombre del archivo para asignarle un date format
	date_time_obj	 = datetime.strptime(file_date, dt_format)
	year, month, day = str(date_time_obj.year), str(date_time_obj.month), str(date_time_obj.day)
	path_destino	 = dataset_spec["path_s3_destino"].replace("yyyy-mm-dd", "{}-{}-{}".format(year, month, day))
	print(path_destino)
	print(dataset_spec["bucket_destino"])
	##### TERMINAR #####
	if dataset_spec["extract_gunzip"] == "True":
		result = uncompress_chunk(s3, bucket_origen, key_origen, dataset_spec["bucket_destino"], path_destino, dec)
		if result != "SUCCESS":
			return "Error descomprimiendo o subiendo chunk:" + result
		result = move_file(s3_r, bucket_origen, key_origen, dataset_spec["bucket_destino"], path_destino)
		if result != "SUCCESS":
			return "Error moviendo archivo" + result
	else:
		result = move_file(s3_r, bucket_origen, key_origen, dataset_spec["bucket_destino"], path_destino)
		if result != "SUCCESS":
			return "Error moviendo archivo" + result