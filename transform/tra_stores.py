import traceback
import pandas as pd
from util.db_generator import generate_db

def transfrom_stores():
    try:
        con_db = generate_db("staging")
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT s.store_id, concat("SAKILA Store",s.store_id) AS name, \
                    ifnull(ci.city, concat("City",s.store_id)) AS city, \
                    ifnull(co.country, concat("Country",s.store_id)) AS country \
                    FROM ext_store AS s \
                    LEFT JOIN ext_address AS a ON (s.address_id = a.address_id) \
                    LEFT JOIN ext_city AS ci ON (a.city_id = ci.city_id) \
                    LEFT JOIN ext_country AS co ON (ci.country_id = co.country_id)'''
        
        stores_tra = pd.read_sql(sql_stat, ses_db)
        return stores_tra  
    finally:
        con_db.stop()

def transfrom_inventory():
    try:
        con_db = generate_db("staging")
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT inv.store_id, inv.film_id, dat.date_id, flm.rental_rate AS rental_price, flm.replacement_cost AS rental_cost
                      FROM ext_inventory inv
                      LEFT JOIN ext_film flm ON inv.film_id = flm.film_id
                      LEFT JOIN ext_date dat ON DATE(inv.last_update) = dat.date;'''
        
        stores_tra = pd.read_sql(sql_stat, ses_db)
        return stores_tra  
    finally:
        con_db.stop()
        
def transfrom_films():
    try:
        con_db = generate_db("staging")
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT film_id, title, release_year, length, rating \
                      FROM ext_film;'''
        
        films_tra = pd.read_sql(sql_stat, ses_db)
        films_tra['length'] = films_tra['length'].apply(categorizar_duracion)
        return films_tra  
    finally:
        con_db.stop()
        
def categorizar_duracion(minutos):
    if minutos < 60:
        return '< 1h'
    elif minutos < 90:
        return '< 1.5h'
    elif minutos < 120:
        return '< 2h'
    else:
        return '> 2h'