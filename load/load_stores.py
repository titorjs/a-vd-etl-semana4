from sqlalchemy import text
import traceback
import pandas as pd
from util.db_generator import generate_db

def load_stores():
    try:
        con_sta_db = generate_db("staging")
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT store_id, name, city, country \
                      FROM tra_store'''
                      
        stores_tra = pd.read_sql(sql_stat, ses_sta_db)
        
        con_sor_db = generate_db("sor")
        ses_sor_db = con_sor_db.start()
        
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        
        dim_sto_dict = {
            "store_bk":[],
            "name":[],
            "city":[],
            "country":[],
        }
        
        if not stores_tra.empty:
            for bk,nam,cit,cou \
                in zip(stores_tra['store_id'], stores_tra['name'], stores_tra['city'], stores_tra['country']):
                dim_sto_dict['store_bk'].append(bk)
                dim_sto_dict['name'].append(nam)
                dim_sto_dict['country'].append(cou)
                dim_sto_dict['city'].append(cit)
        
        if dim_sto_dict['store_bk']:
            df_dim_store = pd.DataFrame(dim_sto_dict)
            df_dim_store.to_sql("dim_store", ses_sor_db, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        con_sor_db.stop()
        con_sta_db.stop()

def load_films():
    try:
        con_sta_db = generate_db("staging")
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT film_id, title, release_year, length, rating \
                      FROM tra_film;'''
                      
        film_tra = pd.read_sql(sql_stat, ses_sta_db)
        
        con_sor_db = generate_db("sor")
        ses_sor_db = con_sor_db.start()
        
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        
        dim_fil_dict = {
            "film_bk":[],
            "title":[],
            "release_year":[],
            "length":[],
            "rating":[],
        }
        
        if not film_tra.empty:
            for bk,tit,rey,le, rat \
                in zip(film_tra['film_id'], film_tra['title'], film_tra['release_year'], film_tra['length'], film_tra['rating']):
                dim_fil_dict['film_bk'].append(bk)
                dim_fil_dict['title'].append(tit)
                dim_fil_dict['release_year'].append(rey)
                dim_fil_dict['length'].append(le)
                dim_fil_dict['rating'].append(rat)
                
        if dim_fil_dict['film_bk']:
            df_dim_store = pd.DataFrame(dim_fil_dict)
            df_dim_store.to_sql("dim_film", ses_sor_db, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        con_sor_db.stop()
        con_sta_db.stop()

def load_dates():
    try:
        con_sta_db = generate_db("staging")
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT date_id, date, month, year FROM ext_date;'''
                      
        film_tra = pd.read_sql(sql_stat, ses_sta_db)
        
        con_sor_db = generate_db("sor")
        ses_sor_db = con_sor_db.start()
        
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        
        dim_fil_dict = {
            "id":[],
            "date_bk":[],
            "date_month":[],
            "date_year":[],
        }
        
        if not film_tra.empty:
            for did,bk,dm,dy \
                in zip(film_tra['date_id'], film_tra['date'], film_tra['month'], film_tra['year']):
                dim_fil_dict['id'].append(did)
                dim_fil_dict['date_bk'].append(bk)
                dim_fil_dict['date_month'].append(dm)
                dim_fil_dict['date_year'].append(dy)
                
        if dim_fil_dict['id']:
            df_dim_store = pd.DataFrame(dim_fil_dict)
            df_dim_store.to_sql("dim_date", ses_sor_db, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        con_sor_db.stop()
        con_sta_db.stop()

def load_inventory():
    try:
        con_sta_db = generate_db("staging")
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stat = '''SELECT store_id, film_id, date_id, rental_price, rental_cost FROM fact_inventory;'''
                      
        film_tra = pd.read_sql(sql_stat, ses_sta_db)
        
        con_sor_db = generate_db("sor")
        ses_sor_db = con_sor_db.start()
        
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        
        dim_fil_dict = {
            "store_id":[],
            "film_id":[],
            "date_id":[],
            "rental_price":[],
            "rental_cost":[],
        }
        
        if not film_tra.empty:
            for did,bk,dm,dy, rc \
                in zip(film_tra['store_id'], film_tra['film_id'], film_tra['date_id'], film_tra['rental_price'], film_tra['rental_cost']):
                dim_fil_dict['store_id'].append(did)
                dim_fil_dict['film_id'].append(bk)
                dim_fil_dict['date_id'].append(dm)
                dim_fil_dict['rental_price'].append(dy)
                dim_fil_dict['rental_cost'].append(rc)
                
        if dim_fil_dict['store_id']:
            df_dim_store = pd.DataFrame(dim_fil_dict)
            df_dim_store.to_sql("fact_inventory", ses_sor_db, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        con_sor_db.stop()
        con_sta_db.stop()

def update_fact_inventory():
    try:
        # Conexión a la base de datos SOR
        con_db_sor = generate_db("staging")  # Conexión a la base de datos staging
        engine_sor = con_db_sor.start()
        if engine_sor == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif engine_sor == -2:
            raise Exception("Error conectándose a la base de datos SOR")

        # Actualizar film_id en fact_inventory
        update_film_query = """
		UPDATE staging.fact_inventory fi
        JOIN sor.dim_film df ON fi.film_id = df.film_bk
        SET fi.film_id = df.id;
        """
        # Actualizar store_id en fact_inventory
        update_store_query = """
        UPDATE staging.fact_inventory fi
        JOIN sor.dim_store ds ON fi.store_id = ds.store_bk
        SET fi.store_id = ds.id;
        """

        # Ejecutar las consultas
        with engine_sor.connect() as connection_sor:
            connection_sor.execute(text(update_film_query))
            connection_sor.execute(text(update_store_query))
            connection_sor.commit()

        print("Actualización de film_id y store_id en fact_inventory completada.")
    
    except Exception as e:
        print("Error al actualizar fact_inventory:", str(e))
        traceback.print_exc()

    finally:
        con_db_sor.stop()