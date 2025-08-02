from airflow.decorators import dag, task
from datetime import datetime, timezone
import requests
import json
import logging
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

@dag(
    dag_id='hubspot_contacts_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['hubspot', 'contacts']
)
def hubspot_contacts_pipeline():
    @task
    def extract_contacts():
        logging.info("Starting extraction of contacts from HubSpot.")
        
        conn = BaseHook.get_connection('hubspot_api_connection')
        access_token = conn.password
        base_url = conn.host
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        query = {
            'properties': ['firstname', 'lastname', 'email', 'phone', 'lastmodifieddate', 'createdate', 'hs_object_id'],
            'limit': 100
        }
        
        url = f"https://{base_url}/crm/v3/objects/contacts/search"
        response = requests.post(url, headers=headers, json=query)
        response.raise_for_status()
        
        contacts_data = response.json().get('results', [])
        
        with open('/usr/local/airflow/include/raw_contacts.json', 'w') as f:
            json.dump(contacts_data, f)
        
        logging.info(f"Extracted {len(contacts_data)} contacts and saved to raw_contacts.json.")
        
        return contacts_data

    @task
    def transform_contacts(contacts_data: list):
        logging.info("Starting transformation of contact data.")
        if not contacts_data:
            logging.warning("No contacts to transform.")
            return []
        
        contacts = [
            {
                "hs_object_id": c['id'],
                "firstname": c['properties'].get('firstname'),
                "lastname": c['properties'].get('lastname'),
                "email": c['properties'].get('email'),
                "phone": c['properties'].get('phone'),
                "createdate": c['properties'].get('createdate'),
                "lastmodifieddate": c['properties'].get('lastmodifieddate')
            } for c in contacts_data
        ]
        
        with open('/usr/local/airflow/include/transformed_contacts.json', 'w') as f:
            json.dump(contacts, f)
            
        logging.info(f"Successfully transformed {len(contacts)} contacts and saved to transformed_contacts.json.")
        
        return contacts

    @task
    def load_contacts(transformed_contacts_list: list):
        logging.info("Starting load of contacts to PostgreSQL.")
        
        if not transformed_contacts_list:
            logging.warning("No transformed contacts to load. Exiting.")
            return

        postgres_hook = PostgresHook(postgres_conn_id='hubspot_db_connection')

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS hubspot_contacts (
            hs_object_id VARCHAR(255) PRIMARY KEY,
            firstname TEXT,
            lastname TEXT,
            email TEXT,
            phone TEXT,
            createdate TIMESTAMP,
            lastmodifieddate TIMESTAMP
        );
        """
        postgres_hook.run(create_table_sql)
        logging.info("Ensured 'hubspot_contacts' table exists.")

        insert_values = []
        for contact in transformed_contacts_list:
            insert_values.append((
                contact.get('hs_object_id'),
                contact.get('firstname'),
                contact.get('lastname'),
                contact.get('email'),
                contact.get('phone'),
                contact.get('createdate'),
                contact.get('lastmodifieddate')
            ))
            
        insert_sql = """
            INSERT INTO hubspot_contacts (hs_object_id, firstname, lastname, email, phone, createdate, lastmodifieddate)
            VALUES %s
            ON CONFLICT (hs_object_id) DO UPDATE SET
                firstname = EXCLUDED.firstname,
                lastname = EXCLUDED.lastname,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                lastmodifieddate = EXCLUDED.lastmodifieddate;
        """
        
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            execute_values(cursor, insert_sql, insert_values)
            conn.commit()
            logging.info(f"Successfully loaded {len(transformed_contacts_list)} contacts into the database.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to load contacts: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    contacts_data = extract_contacts()
    transformed_data = transform_contacts(contacts_data)
    load_contacts(transformed_data)

hubspot_contacts_pipeline()