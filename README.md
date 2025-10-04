# Выгрузка данных в читаемые таблицы DWH

Это набор ETL скриптов, который выгружает в таблицы DWH (meo_mart) лиды, сделки, контакты и историю из Битрикса Бита.

### Настройка:

Создайте переменные окружния

```
HOST=<Адрес Битрикса>
USER_ID=<ID пользователя Битрикса>
TOKEN=<Токен Вебхука Битрикса>

PG_HOST=<Хост БД>
PG_USER=<Пользователь БД>
PG_PASS=<Пароль БД>
PG_DBNAME=<Название БД>
```



### Использует:

https://pypi.org/project/fast-bitrix24/

https://pypi.org/project/pydantic/

https://pypi.org/project/python-dotenv/
