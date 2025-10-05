import os
import logging
import asyncio
import datetime
from pprint import pprint

from psycopg2.extras import execute_values
from dotenv import load_dotenv
from fast_bitrix24 import Bitrix, BitrixAsync

from dwh_connection import get_pg_connection

FIELDS: dict = {

    "id": "id",
    "title": "title",
    "entity_id": "parentId2",
    "contact_id": "contactId",
    "invoice_number": "accountNumber",
    "status": "stageId",
    "created_at": "begindate",
    "paid_at": "ufCrmSmartInvoicePaidDate",
    "amount": "opportunity",
    "invoice_link": "ufCrmSmartInvoiceLink"
}

# Загружаем токены доступа
load_dotenv("../meo.env")
webhook: str = f"{os.getenv("HOST")}/rest/{os.getenv("USER_ID")}/{os.getenv("TOKEN")}/"

bx: BitrixAsync = Bitrix(webhook, ssl=False)
logging.getLogger('fast_bitrix24').addHandler(logging.StreamHandler())


def rename_keys(record_raw: dict) -> dict:
    """
    Перекладывает битриксовые ключи на понятные человеку и постгресу
    :param rec: Словарь со старыми ключами
    :return: Словарь с новыми ключами
    """
    record = {}
    for good_key, bad_key in FIELDS.items():
        record[good_key] = record_raw.get(bad_key) if record_raw.get(bad_key) != "" else None
        # record["etl_timestamp"] = datetime.datetime.now().isoformat()
    return record


def bulk_upsert(records, table_name, conflict_column="id"):
    if not records:
        return

    connection = get_pg_connection()

    columns = list(FIELDS.keys())
    columns_str = ', '.join(columns)

    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES %s
        ON CONFLICT ({conflict_column}) 
        DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_column])}
    """

    values = [tuple(record.values()) for record in records]

    with connection.cursor() as cursor:
        execute_values(cursor, query, values)
    connection.commit()


async def main():
    print("Начинаем выгрузку оплат")

    deals_raw = await bx.get_all(
        'crm.item.list',
        {
            "entityTypeId": 31,
            "select": list(FIELDS.values()),
            "filter": {
                "stageID": "DT31_2:P",
                ">createdTime": "2025-01-01",
            },
        })

    pprint(deals_raw)

    deals: list = list(map(rename_keys, deals_raw))

    pprint(deals)
    bulk_upsert(deals, "bit_crm_invoices")

    print("Выгрузка завершена")


asyncio.run(main())
