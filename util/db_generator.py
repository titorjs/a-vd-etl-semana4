import traceback
import pandas as pd
from util.db_connection import Db_Connection

def generate_db(db):
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'tarea'
        pwd = '1q2w3e4r'
        con_db = Db_Connection(type,host,port, user,pwd,db)
        return con_db
    except:
        traceback.print_exc()