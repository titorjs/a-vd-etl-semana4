import traceback
import pandas as pd

def extraer_csv (filename):
    try:
        data = pd.read_csv(filename)
        return data
    except:
        traceback.print_exc()
    finally:
        pass