from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, text
import pandas as pd
import os

# Obtener configuraciones de entorno
DATABASE_URL = os.getenv('DATABASE_URL')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Verificar que todas las variables necesarias están definidas
if not all([DATABASE_URL, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
    raise ValueError("Faltan variables de entorno necesarias para la conexión a la base de datos")

# Configurar la conexión a la base de datos PostgreSQL
engine = create_engine(DATABASE_URL)

app = FastAPI()

# Endpoint para obtener los nombres de los productos, sus precios unitarios y regiones
@app.get("/productos")
def obtener_productos():
    try:
        query = """
        SELECT p.product_name, pr.unit_price, r.region_name
        FROM price_tb as pr
        JOIN productos_tb as p ON pr.id_producto = p.id_producto
        JOIN region_tb as r ON pr.id_region = r.id_region
        GROUP BY p.product_name, r.region_name, pr.unit_price
        ORDER BY pr.unit_price DESC;
        """
        productos_df = pd.read_sql_query(text(query), con=engine)
        return productos_df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener productos: {str(e)}")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
