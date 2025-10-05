import os
import logging
import asyncio
import datetime

from psycopg2.extras import execute_values
from dotenv import load_dotenv
from fast_bitrix24 import Bitrix, BitrixAsync

from dwh_connection import get_pg_connection

FIELDS: dict = {
    "id": "ID",
    "lead_id": "LEAD_ID",
    "title": "TITLE",
    "deal_type": "TYPE_ID",
    "status_id": "STAGE_ID",
    "manager_id": "ASSIGNED_BY_ID",
    "product_id": "UF_CRM_65FBDEEB66334",
    "tariff_id": "UF_CRM_1590157930",
    "academic_year": "UF_CRM_65F9CCE1B0771",
    "student_grade": "UF_CRM_1579011156",
    "amount_total": "OPPORTUNITY",
    "student_country": "UF_CRM_63931D41D4B7C",
    "student_location_full": "UF_CRM_1583234012",
    "student_is_attached": "UF_CRM_1679320196398",
    "is_gosfin": "UF_CRM_1678443683",
    "is_matcap": "UF_CRM_1660815928880",
    "parent_deal_id": "UF_CRM_1678820045816",
    "renewal_wave": "UF_CRM_1684927408",
    "is_self_service": "UF_CRM_64E22B7EBEF33",
    "termination_reasons": "UF_CRM_1726687398",
    "termination_subreason": "UF_CRM_1755595137",
    "signed_at": "UF_CRM_1579182960",
    "offered_at": "UF_CRM_6494E0BEEEAAB",
    "terminated_at": "UF_CRM_1693818065",
    "created_at": "DATE_CREATE",
    "etl_timestamp": "",
}


# Загружаем токены доступа
load_dotenv("../meo.env")
webhook: str = f"{os.getenv("HOST")}/rest/{os.getenv("USER_ID")}/{os.getenv("TOKEN")}/"

bx:BitrixAsync = Bitrix(webhook, ssl=False)
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

    print("Начинаем выгрузку сделок")

    deals_raw = await bx.get_all(
        'crm.deal.list',
        {
            "select": list(FIELDS.values()),
            # "filter": {">DATE_CREATE": "2025-10-03"},
    })

    deals: list = list(map(rename_keys,deals_raw))
    bulk_upsert(deals, "bit_crm_deals")

    print("Выгрузка завершена")

asyncio.run(main())
