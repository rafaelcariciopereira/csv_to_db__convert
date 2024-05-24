import mysql.connector
from mysql.connector import errorcode
from connectDB import connectDB


# Retorna o resultado da consulta
def consulta():
    try :
        conn = connectDB()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM vendas ; " )

        for row in cursor.fetchall():
            print(row)

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(err)
consulta()