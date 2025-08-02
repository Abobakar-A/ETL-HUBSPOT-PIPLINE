from airflow.decorators import dag, task
from datetime import datetime, timezone
import requests
import json
import logging
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

@dag(
    dag_id='hubspot_products_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['hubspot', 'products']
)
def hubspot_products_pipeline():
    @task
    def extract_products():
        logging.info("Starting extraction of products from HubSpot.")
        
        conn = BaseHook.get_connection('hubspot_api_connection')
        access_token = conn.password
        base_url = conn.host
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        query = {
            'properties': ['name', 'price', 'hs_sku', 'description', 'hs_lastmodifieddate', 'hs_object_id', 'hs_created_at'],
            'limit': 100
        }
        
        url = f"https://{base_url}/crm/v3/objects/products/search"
        response = requests.post(url, headers=headers, json=query)
        response.raise_for_status()
        
        products_data = response.json().get('results', [])
        
        with open('/usr/local/airflow/include/raw_products.json', 'w') as f:
            json.dump(products_data, f)
        
        logging.info(f"Extracted {len(products_data)} products and saved to raw_products.json.")
        
        return products_data

    @task
    def transform_products(products_data: list):
        logging.info("Starting transformation of product data.")
        if not products_data:
            logging.warning("No products to transform.")
            return []
        
        products = [
            {
                "hs_object_id": p['id'],
                "name": p['properties'].get('name'),
                "description": p['properties'].get('description'),
                "price": float(p['properties'].get('price', 0)) if p['properties'].get('price') else 0,
                "hs_sku": p['properties'].get('hs_sku'),
                "hs_created_at": p['properties'].get('hs_created_at'),
                "hs_lastmodifieddate": p['properties'].get('hs_lastmodifieddate')
            } for p in products_data
        ]
        
        with open('/usr/local/airflow/include/transformed_products.json', 'w') as f:
            json.dump(products, f)
            
        logging.info(f"Successfully transformed {len(products)} products and saved to transformed_products.json.")
        
        return products

    @task
    def load_products(transformed_products_list: list):
        logging.info("Starting load of products to PostgreSQL.")
        
        if not transformed_products_list:
            logging.warning("No transformed products to load. Exiting.")
            return

        postgres_hook = PostgresHook(postgres_conn_id='hubspot_db_connection')

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS hubspot_products (
            hs_object_id VARCHAR(255) PRIMARY KEY,
            name TEXT,
            description TEXT,
            price NUMERIC,
            hs_sku VARCHAR(255),
            hs_created_at TIMESTAMP,
            hs_lastmodifieddate TIMESTAMP
        );
        """
        postgres_hook.run(create_table_sql)
        logging.info("Ensured 'hubspot_products' table exists.")

        insert_values = []
        for product in transformed_products_list:
            insert_values.append((
                product.get('hs_object_id'),
                product.get('name'),
                product.get('description'),
                product.get('price'),
                product.get('hs_sku'),
                product.get('hs_created_at'),
                product.get('hs_lastmodifieddate')
            ))
            
        insert_sql = """
            INSERT INTO hubspot_products (hs_object_id, name, description, price, hs_sku, hs_created_at, hs_lastmodifieddate)
            VALUES %s
            ON CONFLICT (hs_object_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                price = EXCLUDED.price,
                hs_sku = EXCLUDED.hs_sku,
                hs_lastmodifieddate = EXCLUDED.hs_lastmodifieddate;
        """
        
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            execute_values(cursor, insert_sql, insert_values)
            conn.commit()
            logging.info(f"Successfully loaded {len(transformed_products_list)} products into the database.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to load products: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    products_data = extract_products()
    transformed_data = transform_products(products_data)
    load_products(transformed_data)

hubspot_products_pipeline()