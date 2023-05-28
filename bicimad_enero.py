import findspark
import pyspark
from pyspark.sql import SparkSession
import json
import csv


findspark.init()
spark = SparkSession.builder.getOrCreate()


#ENERO 2023
enero2019 = spark.read.json('201901.json')
enero2020 = spark.read.json('202001.json')

print('Archivo tipo json, enero 2019')
enero2019.show()
print('Archivo tipo csv, enero 2022')
enero2022.show()


enero2019 = spark.read.json('201901.json')
enero2020 = spark.read.json('202001.json')
enero2021 = spark.read.json('202101.json')
enero2022 = spark.read.csv('202201.csv', sep=';', header = True)
enero2023 = spark.read.csv('202301.csv', sep=';', header = True)


#NÚMERO DE DESPLAZAMIENTOS

print('Número de desplazamientos Enero 2019: ' + str(enero2019.count()))
print('Número de desplazamientos Enero 2020: ' + str(enero2020.count()))
print('Número de desplazamientos Enero 2021: ' + str(enero2021.count()))
print('Número de desplazamientos Enero 2022: ' + str(enero2022.count()//2))
print('Número de desplazamientos Enero 2023: ' + str(enero2023.count()//2))


#DESENGANCHES

unlock2019 = enero2019.groupBy('idunplug_station').count()
unlock2020 = enero2020.groupBy('idunplug_station').count()
unlock2021 = enero2021.groupBy('idunplug_station').count()
unlock2022 = enero2022.groupBy('station_unlock').count()
unlock2023 = enero2023.groupBy('station_unlock').count()

print ('ENERO 2019')
unlock2019.filter(unlock2019['count']>3400).show()
print ('ENERO 2020')
unlock2020.filter(unlock2020['count']>3000).show()
print ('ENERO 2021')
unlock2021.filter(unlock2021['count']>1100).show()
print ('ENERO 2022')
unlock2022.filter(unlock2022['count']>2300).show()
print ('ENERO 2023')
unlock2023.filter(unlock2023['count']>2500).show()


#ENGANCHES

lock2019 = enero2019.groupBy('idplug_station').count()
lock2020 = enero2020.groupBy('idplug_station').count()
lock2021 = enero2021.groupBy('idplug_station').count()
lock2022 = enero2022.groupBy('station_lock').count()
lock2023 = enero2023.groupBy('station_lock').count()

print ('ENERO 2019')
lock2019.filter(lock2019['count']>3300).show()
print ('ENERO 2020')
lock2020.filter(lock2020['count']>3000).show()
print ('ENERO 2021')
lock2021.filter(lock2021['count']>1100).show()
print ('ENERO 2022')
lock2022.filter(lock2022['count']>2300).show()
print ('ENERO 2023')
lock2023.filter(lock2023['count']>2500).show()



#ESTACIONES DONDE MENOS SE DESENGANCHA

print ('ENERO 2019')
unlock2019.filter(unlock2019['count']<350).show()
print ('ENERO 2020')
unlock2020.filter(unlock2020['count']<400).show()
print ('ENERO 2021')
unlock2021.filter(unlock2021['count']<60).show()
print ('ENERO 2022')
unlock2022.filter(unlock2022['count']<20).show()
print ('ENERO 2023')
unlock2023.filter(unlock2023['count']<200).show()

#ESTACIONES DONDE MENOS SE ENGANCHA

print ('ENERO 2019')
lock2019.filter(lock2019['count']<350).show()
print ('ENERO 2020')
lock2020.filter(lock2020['count']<400).show()
print ('ENERO 2021')
lock2021.filter(lock2021['count']<60).show()
print ('ENERO 2022')
lock2022.filter(lock2022['count']<200).show()
print ('ENERO 2023')
lock2023.filter(lock2023['count']<200).show()

#ARCHIVOS JSON
#TIPOS DE USUSARIOS

print('TIPO DE USUARIO QUE LO UTILIZA')
print ('ENERO 2019')
enero2019.groupBy('user_type').count().show()
print ('ENERO 2020')
enero2020.groupBy('user_type').count().show()
print ('ENERO 2021')
enero2021.groupBy('user_type').count().show()


#TIEMPO DE UTILIZACIÓN
print('TIEMPO EN SEGUDNOS UTILIZACIÓN TOTAL')
print ('ENERO 2019')
enero2019.groupBy('user_type').sum('travel_time').show()
print ('ENERO 2020')
enero2020.groupBy('user_type').sum('travel_time').show()
print ('ENERO 2021')
enero2021.groupBy('user_type').sum('travel_time').show()


#RANGO DE EDAD (2019-2021)

print('RANGO DE EDAD UTILIZADO')
print ('ENERO 2019')
enero2019.groupBy('ageRange').count().show()
print ('ENERO 2020')
enero2020.groupBy('ageRange').count().show()
print ('ENERO 2021')
enero2021.groupBy('ageRange').count().show()


#ARCHIVOS CSV
#DÍAS CON MÁS DESPLAZAMIENTOS (2022 Y 2023)

print('FECHA MÁS UTILIZADAS DE LOS ÚLTIMOS 2 AÑOS')

fecha2022 = enero2022.groupBy('fecha').count()
fecha2023 = enero2023.groupBy('fecha').count()

print ('ENERO 2022')
fecha2022.filter(fecha2022['count']>10000).show()
print ('ENERO 2023')
fecha2023.filter(fecha2023['count']>12000).show()

#MESES COVID

febrero2020 = spark.read.json('202002.json')
marzo2020 = spark.read.json('202003.json')
abril2020 = spark.read.json('202004.json')
mayo2020 = spark.read.json('202005.json')
junio2020 = spark.read.json('202006.json')

print('Número de desplazamientos Febrero 2020: ' + str(febrero2020.count()))
print('Número de desplazamientos Marzo 2020: ' + str(marzo2020.count()))
print('Número de desplazamientos Abril 2020: ' + str(abril2020.count()))
print('Número de desplazamientos Mayo 2020: ' + str(mayo2020.count()))
print('Número de desplazamientos Junio 2020: ' + str(junio2020.count()))


















