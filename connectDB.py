import mysql.connector
from mysql.connector import errorcode


# Conecta ao DB e informa resultado da conexão
def connectDB():
    config = {
  'host':'<DATABASE_HOST>.mysql.database.azure.com',
  'user':'<USERNAME>',
  'password':'<PASSWORD>',
  'database':'DATABASE_NAME',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': 'DigiCertGlobalRootCA.crt.pem'
}

    try:
        conn = mysql.connector.connect(**config)
        print("Connection established")
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None
