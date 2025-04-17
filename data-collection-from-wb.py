import configparser
from pgdb import PGDatabase
import pandas as pd
import requests as rq
import datetime as dt
import time as tm
import numpy as np
from collections import defaultdict

config = configparser.ConfigParser()
config.read("config.ini")

TIME_FORMAT = config["Time_Format"]["TIME_FORMAT"]
print(TIME_FORMAT)
WB_GET_ALL_ORDERS = config["Method_API"]["WB_GET_ALL_ORDERS"]
WB_GET_ALL_SALES = config["Method_API"]["WB_GET_ALL_SALES"]
token_WB = config["Token"]["token_WB"]
DATABASE_CREDS = config["Database"]

database = PGDatabase(
    host=DATABASE_CREDS["HOST"],
    database=DATABASE_CREDS["DATABASE"],
    user=DATABASE_CREDS["USER"],
    password=DATABASE_CREDS["PASSWORD"],
)

# Основной словарь, содержащий данные по товарам
# Ключ - nmId (идентификатор товара (SKU)), значение - вложенный словарь с характеристиками

dict_sku = {}
start_date = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
params = {
    "dateFrom": start_date,  # Параметр запроса для получения данных с указанной даты
    "flag": 1,
}
# Запрос заказов с сервера
for _ in range(4):
    r = rq.get(
        url=WB_GET_ALL_ORDERS, headers={"Authorization": token_WB}, params=params
    )
    if r.status_code == 200:
        lst_all_orders = r.json()
        break
    elif (
        r.status_code == 429
    ):  # Ошибка 429 означает превышение лимита запросов, ждём перед повтором
        tm.sleep(1.1)
    else:
        print(f"Ошибка в запросе {WB_GET_ALL_ORDERS}")

# Запрос продаж с сервера
for i in range(4):
    r = rq.get(url=WB_GET_ALL_SALES, headers={"Authorization": token_WB}, params=params)
    if r.status_code == 200:
        lst_all_sales = r.json()
        break
    elif r.status_code == 429:
        tm.sleep(1.1)
    else:
        print(f"Ошибка в запросе {WB_GET_ALL_SALES}")


df_raw_all_orders = pd.DataFrame(
    lst_all_orders
)  # Создание DataFrame с данными о заказах
df_raw_all_sales = pd.DataFrame(
    lst_all_sales
)  # Создание DataFrame с данными о продажах

# Объединение информации о товарах из заказов и продаж, удаление дубликатов
df_info_sku = pd.concat([df_raw_all_orders, df_raw_all_sales])
df_info_sku = df_info_sku.loc[
    :,
    ["barcode", "nmId", "category", "subject", "brand", "techSize", "supplierArticle"],
]
df_info_sku = df_info_sku.drop_duplicates()

# Выбор конкретных столбцов из заказов
df_all_orders = df_raw_all_orders.loc[
    :,
    [
        "lastChangeDate",
        "barcode",
        "isCancel",
        "cancelDate",
        "orderType",
        "srid",
        "countryName",
        "oblastOkrugName",
        "regionName",
    ],
]

# Выбор конкретных столбцов из продаж
df_all_sales = df_raw_all_sales.loc[
    :,
    [
        "lastChangeDate",
        "warehouseName",
        "countryName",
        "oblastOkrugName",
        "regionName",
        "barcode",
        "totalPrice",
        "spp",
        "forPay",
        "finishedPrice",
        "saleID",
        "orderType",
        "srid",
    ],
]

df_all_orders.rename(
    columns={
        "lastChangeDate": "lastChangeDate_order",
        "countryName": "countryName_order",
        "oblastOkrugName": "oblastOkrugName_order",
        "regionName": "regionName_order",
    },
    inplace=True,
)
df_all_sales.rename(
    columns={
        "lastChangeDate": "lastChangeDate_sale",
        "countryName": "countryName_sale",
        "oblastOkrugName": "oblastOkrugName_sale",
        "regionName": "regionName_sale",
    },
    inplace=True,
)
df_all_sales["success_sale"] = df_all_sales["saleID"].apply(
    lambda x: 1 if x[0] == "S" else 0
)
df_all_sales["success_return"] = df_all_sales["saleID"].apply(
    lambda x: 1 if x[0] == "R" else 0
)
df_all_sales = df_all_sales.merge(df_info_sku)
df_sales = (
    df_all_sales.groupby(["barcode"])[["totalPrice", "success_sale", "success_return"]]
    .agg(
        {
            "totalPrice": "sum",
            # 'totalPrice': 'mean',
            "success_sale": "sum",
            "success_return": "sum",
        }
    )
    .reset_index()
)

df_all_orders["success_order"] = df_all_orders["isCancel"].apply(
    lambda x: 0 if x else 1
)
df_all_orders["success_cancel"] = df_all_orders["isCancel"].apply(
    lambda x: 1 if x else 0
)
df_all_orders = df_all_orders.merge(df_info_sku)
df_orders = (
    df_all_orders.groupby(["barcode"])[["success_order", "success_cancel"]]
    .agg(
        {
            # 'totalPrice': 'mean',
            "success_order": "sum",
            "success_cancel": "sum",
        }
    )
    .reset_index()
)

df_orders_and_sales = df_sales.merge(df_orders, how="outer")

df_sales_by_oblast = (
    df_all_sales.groupby(["barcode", "oblastOkrugName_sale"])[
        ["totalPrice", "success_sale", "success_return"]
    ]
    .agg(
        {
            "totalPrice": "sum",
            # 'totalPrice': 'mean',
            "success_sale": "sum",
            "success_return": "sum",
        }
    )
    .reset_index()
)

df_orders_by_oblast = (
    df_all_orders.groupby(["barcode", "oblastOkrugName_order"])[
        ["success_order", "success_cancel"]
    ]
    .agg(
        {
            # 'totalPrice': 'mean',
            "success_order": "sum",
            "success_cancel": "sum",
        }
    )
    .reset_index()
)

df_orders_and_sales_by_oblast = df_sales_by_oblast.merge(
    df_orders_by_oblast, how="outer"
)


df_orders_by_oblast["date"] = start_date
df_orders_and_sales["date"] = start_date

df_orders_by_oblast["barcode"] = df_orders_by_oblast["barcode"].astype(str)
df_orders_and_sales["barcode"] = df_orders_and_sales["barcode"].astype(str)
df_info_sku["barcode"] = df_info_sku["barcode"].astype(str)
df_info_sku["nmId"] = df_info_sku["nmId"].astype(str)

df_orders_and_sales.fillna("NULL", inplace=True)


for i, row in df_orders_by_oblast.iterrows():
    query = f"insert into orders_by_oblast values ('{row['barcode']}', '{row['oblastOkrugName_order']}', {row['success_order']}, {row['success_cancel']}, '{row['date']}')"
    database.post(query)

for i, row in df_info_sku.iterrows():
    query = f"insert into info_sku values ('{row['barcode']}', {row['nmId']}, '{row['category']}', '{row['subject']}', '{row['brand']}', '{row['techSize']}', '{row['supplierArticle']}')"
    database.post(query)

for i, row in df_orders_and_sales.iterrows():
    query = f"insert into orders_and_sales values ('{row['barcode']}', {row['totalPrice']}, {row['success_sale']}, {row['success_return']}, {row['success_order']}, {row['success_cancel']}, '{row['date']}')"
    database.post(query)
