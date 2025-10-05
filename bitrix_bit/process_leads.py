import os
import logging
import asyncio
import datetime

from psycopg2.extras import execute_values
from dotenv import load_dotenv
from fast_bitrix24 import Bitrix, BitrixAsync

from bitrix_bit.dwh_connection import get_pg_connection

FIELDS: dict = {
    "id": "ID",
    "title": "TITLE",
    # "phone": "PHONE",
    # "email": "EMAIL",
    "client_full_name": "UF_CRM_5F2915B4DB466",
    "client_relation_type": "UF_CRM_1681762948",
    "student_full_name": "UF_CRM_5F2915B501F0E",
    "status_id": "STATUS_ID",
    "manager_id": "ASSIGNED_BY_ID",
    "product_id": "UF_CRM_1710956921",
    "tariff_id": "UF_CRM_5F29568AB1DF4",
    "academic_year": "UF_CRM_1710868000",
    "student_grade": "UF_CRM_1671530135656",
    "amount_total": "OPPORTUNITY",
    "source_id": "SOURCE_ID",
    "utm_source": "UTM_SOURCE",
    "utm_medium": "UTM_MEDIUM",
    "utm_campaign": "UTM_CAMPAIGN",
    "utm_content": "UTM_CONTENT",
    "utm_term": "UTM_TERM",
    "discount_loyalty": "UF_CRM_1683884441",
    # "ndz_count": "UF_CRM_1754512458374",
    "created_at": "DATE_CREATE",
    "offered_at": "UF_CRM_1687463753"

}


load_dotenv("../meo.env")
webhook: str = f"{os.getenv("HOST")}/rest/{os.getenv("USER_ID")}/{os.getenv("TOKEN")}/"

bx: BitrixAsync = Bitrix(webhook, ssl=False, operating_time_limit = 200)

# Включаем логирование
logging.getLogger('fast_bitrix24').addHandler(logging.StreamHandler())



def rename_keys(record_raw: dict) -> dict:
    """
    Перекладывает битриксовые ключи на понятные человеку и постгрес
    :param rec: Словарь со старыми ключами
    :return: Словарь с новыми ключами
    """
    record = {}
    for good_key, bad_key in FIELDS.items():
        record[good_key] = record_raw.get(bad_key) if record_raw.get(bad_key) != "" else None
        # record["etl_timestamp"] = datetime.datetime.now().isoformat()
    return record


def bulk_upsert(records, table_name, conflict_column="id"):

    """
    Выполняет массовую upsert-операцию (вставка или обновление) записей в PostgreSQL.
    Использует PostgreSQL INSERT ... ON CONFLICT для эффективной обработки дубликатов.
    Подходит для больших объемов данных (тысячи записей) за счет пакетной вставки.
    """

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
    print("Начинаем выгрузку лидов")

    max_concurrent_requests = 1
    with bx.slow(max_concurrent_requests):
        leads_raw = await bx.get_all(
            'crm.lead.list',
            {
                "select": list(FIELDS.values()),
                "filter": {">DATE_CREATE": "2025-01-01"},
            })

    leads: list = list(map(rename_keys, leads_raw))
    result = bulk_upsert(leads, "bit_crm_leads")

    print("Выгрузка завершена")


if __name__ == "__main__":
    asyncio.run(main())
