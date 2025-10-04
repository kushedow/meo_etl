import os
import psycopg2
from dotenv import load_dotenv


def get_pg_connection():

    load_dotenv("../meo.env")
    return psycopg2.connect(
        host=os.getenv('PG_HOST'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASS'),
        dbname=os.getenv('PG_DBNAME')
    )


