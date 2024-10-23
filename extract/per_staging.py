import traceback
from sqlalchemy import text
import pandas as pd
from util.db_generator import generate_db

def persistir_stagin(df_per, tab_name):
    try:        
        con_db = generate_db("staging")
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        df_per.to_sql(tab_name, ses_db, if_exists='replace', index=False)
    finally:
        con_db.stop()

def persistir_stagin_address():
    try:
        # Crear conexión a la base de datos "staging"
        con_db_staging = generate_db("staging")
        engine_staging = con_db_staging.start()
        if engine_staging == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif engine_staging == -2:
            raise Exception("Error tratando de conectarse a la base de datos staging")

        # Crear conexión a la base de datos "oltp"
        con_db_oltp = generate_db("oltp")
        engine_oltp = con_db_oltp.start()
        if engine_oltp == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif engine_oltp == -2:
            raise Exception("Error tratando de conectarse a la base de datos oltp")

        # Abrir conexión a la base de datos staging
        with engine_staging.connect() as connection_staging:
            # Borrar datos previos en la tabla "address" de "staging"
            delete_query = "DELETE FROM staging.ext_address"
            connection_staging.execute(text(delete_query))

            # Insertar los datos desde "oltp.address" a "staging.address"
            insert_query = """
            INSERT INTO staging.ext_address (address_id, address, address2, district, city_id, postal_code, phone, location, last_update)
            SELECT address_id, address, address2, district, city_id, postal_code, phone, location, last_update
            FROM oltp.address;
            """
            connection_staging.execute(text(insert_query))

        print("Datos persistidos correctamente en staging.address")
    
    except Exception as e:
        print("Error al persistir datos en staging.address:", str(e))
        traceback.print_exc()
    
    finally:
        # Cerrar conexiones a las bases de datos
        con_db_staging.stop()
        con_db_oltp.stop()