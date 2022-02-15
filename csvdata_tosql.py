import csv
import mysql.connector
import logging as log

class ParseCsvToSql():
    ''' This class creates a DB, Table in mysql. Parses data in csv file and inserts all the data to DB '''
    log.basicConfig(filename='log.txt', level=log.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    def __init__(self,file,user,password):
        self.file=file
        self.user = user
        self.password = password


    def csvParser(self):
        '''Parses data to list from csv file'''

        try:
            f=open(self.file,'r')
            r=csv.reader(f,delimiter=';')
            data=list(r)
            log.info('File Parsed Successfully')
        except Exception as e:
            print('File not parsed:',e)
            log.exception('File not parsed',e)
        return data

    def parseDataToSql(self,data):
        '''Create a table in SQL and stores all the parsed data in table'''
        try:
            con=mysql.connector.connect(host='localhost',user=self.user,password=self.password)
            if con.is_connected():
                log.info('Connected to mySql')
            cursor=con.cursor() #To execute SQL queries and hold results
            cursor.execute("create database if not exists Challenge3")
            log.info('Database Challenge3 Created')
            cursor.execute("use Challenge3")
            cursor.execute("Create table if not exists carbon_nanotubes(Chiral_indice_n int(5),Chiral_indice_m int(5),Initial_atomic_coordinate_u varchar(10),Initial_atomic_coordinate_v varchar(10),Initial_atomic_coordinate_w varchar(10),Calculated_atomic_coordinates_u varchar(10),Calculated_atomic_coordinates_v varchar(10),Calculated_atomic_coordinates_w varchar(10))")
            log.info('Table carbon_nanotubes Created')
            sql='insert into carbon_nanotubes(Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u,Initial_atomic_coordinate_v,Initial_atomic_coordinate_w,Calculated_atomic_coordinates_u,Calculated_atomic_coordinates_v,Calculated_atomic_coordinates_w) values(%s, %s, %s, %s, %s, %s, %s, %s)'
            cursor.executemany(sql,data[1:])
            con.commit()
            log.info('Data inserted to Table Successfully')
        except mysql.connector.DatabaseError as e:
            print('Issue in Parsing data to sql',e)
            log.exception('Issue in Parsing data to sql',e)
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()
            log.info('Connection closed')

parser=ParseCsvToSql('carbon_nanotubes.csv','root','1234')
data=parser.csvParser()
parser.parseDataToSql(data)





