import traceback
import pandas as pd
from util.db_generator import generate_db

def extraer_stores():
    try:
        con_db = generate_db('oltp')
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        stores = pd.read_sql("SELECT * FROM store", ses_db)
        return stores
    finally:
        con_db.stop()

def extraer_tabla(tabla):
    try:
        con_db = generate_db('oltp')
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        query = f"SELECT * FROM {tabla}"
        stores = pd.read_sql(query, ses_db)
        return stores
    finally:
        con_db.stop()