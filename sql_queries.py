import csvdata_tosql
import mysql

class SqlQueries():

    def __init__(self):
        self.con = mysql.connector.connect(host='localhost',user='root', password='1234',database='challenge3')
        self.cursor = self.con.cursor()

    #Query1 - Select query
    def queriesExecute(self):
        #Query1 - Select query
        self.cursor.execute("select distinct Chiral_indice_n from carbon_nanotubes")
        query1_res=self.cursor.fetchall()
        self.cursor.execute("select * from carbon_nanotubes")
        query2_res = self.cursor.fetchall()

        #Query2 - Where clause
        self.cursor.execute("select Initial_atomic_coordinate_u from carbon_nanotubes where Chiral_indice_n=2")
        query3_res=self.cursor.fetchall()

        #Where clause with AND OR NOT
        self.cursor.execute("select * from carbon_nanotubes where Chiral_indice_n=10 and Chiral_indice_m=4")
        query4_res=self.cursor.fetchall()

        self.cursor.execute("select * from carbon_nanotubes where Chiral_indice_n=10 or Chiral_indice_m=4")
        query5_res=self.cursor.fetchall()

        self.cursor.execute("select * from carbon_nanotubes where NOT Chiral_indice_n=10")
        query6_res=self.cursor.fetchall()

        #Order By
        self.cursor.execute("select Chiral_indice_n, Chiral_indice_m, Initial_atomic_coordinate_u from carbon_nanotubes order by Chiral_indice_n ")
        query7_res = self.cursor.fetchall()

        self.cursor.execute(
            "select Chiral_indice_n, Chiral_indice_m, Initial_atomic_coordinate_u from carbon_nanotubes order by Chiral_indice_n desc")
        query8_res = self.cursor.fetchall()

        #MIN() and MAX()
        self.cursor.execute("select min(Chiral_indice_n),max(Chiral_indice_n) from carbon_nanotubes")
        query9_res=self.cursor.fetchall()

        #Count() avg() and sum()
        self.cursor.execute("select count(Chiral_indice_n), avg(Chiral_indice_n), sum(Chiral_indice_n) from carbon_nanotubes")
        query10_res = self.cursor.fetchall()

        #Like operator
        self.cursor.execute("select Initial_atomic_coordinate_u from carbon_nanotubes where Initial_atomic_coordinate_u like '0,9%'")
        query11_res = self.cursor.fetchall()

        #In and Not In Operators
        self.cursor.execute("select Chiral_indice_n from carbon_nanotubes where Chiral_indice_n in (2,6)")
        query12_res = self.cursor.fetchall()

        self.cursor.execute("select Chiral_indice_n from carbon_nanotubes where Chiral_indice_n not in (4,9,10)")
        query13_res = self.cursor.fetchall()

        #Group by
        self.cursor.execute("select * from carbon_nanotubes group by Chiral_indice_n")
        query14_res = self.cursor.fetchall()
        print(query14_res)

queries=SqlQueries().queriesExecute()
