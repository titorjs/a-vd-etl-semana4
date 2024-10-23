# this file is a kind of python startup module used for manual unit testing
import traceback
import pandas as pd
from extract.ext_fromcsv import extraer_csv
from extract.ext_fromdb import extraer_tabla
from extract.per_staging import persistir_stagin, persistir_stagin_address
from transform.tra_stores import transfrom_stores, transfrom_films, transfrom_inventory
from load.load_stores import load_stores, load_films, load_dates, load_inventory, update_fact_inventory

try:
    print("!!!!!!!!INICIANDO PROCESO DE EXTRACCIÓN!!!!!!!!")
    print("----- extrayendo desde base")
    print("Extrayendo Adress y persistiendo en staging")
    persistir_stagin_address()
    
    print("Extrayendo City")
    cities = extraer_tabla("city")
    print("Persistiendo data Cities en staging")
    persistir_stagin(cities, "ext_city")
    
    print("Extrayendo Country")
    countries = extraer_tabla("country")
    print("Persistiendo data Countries en staging")
    persistir_stagin(countries, "ext_country")
    
    print("Extrayendo Films")
    films = extraer_tabla("film")
    print("Persistiendo data Countries en staging")
    persistir_stagin(films, "ext_film")
    
    print("Extrayendo Inventory")
    inventory = extraer_tabla("inventory")
    print("Persistiendo data Countries en staging")
    persistir_stagin(inventory, "ext_inventory")
    
    print("Extrayendo Stores")
    store = extraer_tabla("store")
    print("Persistiendo data Stores en staging")
    persistir_stagin(store, "ext_store")
    
    print("----- extrayendo desde csv")
    print("Extrayendo Dates")
    dates = extraer_csv("./csv/dates.csv")
    print("Persistiendo data Stores en staging")
    persistir_stagin(dates, "ext_date")
    
    print("!!!!!!!!INICIANDO PROCESO DE TRANSFORMACIÓN!!!!!!!!")
    print("Transformando información de Stores")
    tra_stores = transfrom_stores()
    print("Persistiendo en sataging datos transformados de Stores")
    persistir_stagin(tra_stores, "tra_store")
    print("Transformando información Films")
    tra_films = transfrom_films()
    print("Persistiendo en sataging datos transformados de Films")
    persistir_stagin(tra_films, "tra_film")
    print("Transformando información Inventory")
    tra_inventory = transfrom_inventory()
    print("Persistiendo en sataging datos transformados de Inventory")
    persistir_stagin(tra_inventory, "fact_inventory")
    
    
    print("!!!!!!!!INICIANDO PROCESO DE CARGA!!!!!!!!")
    print("Carga de Stores")
    load_stores()
    print("Carga de Films")
    load_films()
    print("Carga de Dates")
    load_dates()
    print("Carga de Inventory")
    update_fact_inventory()
    load_inventory()
    
except:
    traceback.print_exc()
finally:
    None