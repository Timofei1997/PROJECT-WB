Как я построил ETL-пайплайн для анализа продаж Wildberries с API, PostgreSQL и Metabase
Привет, Хабр!
Хочу поделиться реальным кейсом, который реализовал на работе: системой автоматизированного сбора и визуализации данных с маркетплейса Wildberries.
Я написал Python-скрипт, который:
•	забирает данные через WB API;
•	обрабатывает заказы и продажи;
•	сохраняет их в PostgreSQL;
•	визуализирует в Metabase;
•	ежедневно автоматически обновляется через cron.
В статье покажу, как всё устроено внутри — с примерами кода и архитектурой.
________________________________________
📦 Зачем всё это?
Бизнесу нужно ежедневно отслеживать:
•	продажи и возвраты;
•	географию заказов;
•	остатки и ходовые позиции.
Раньше всё это считалось вручную. Теперь данные автоматически обновляются каждый день и доступны в BI-системе в виде удобных дашбордов.
________________________________________
⚙️ Технологии
•	Python — для ETL-скрипта
•	requests, pandas, psycopg2 — работа с API, трансформация, загрузка в БД
•	PostgreSQL — хранилище
•	Metabase — визуализация
•	cron + Linux-сервер — автоматизация
________________________________________
🔐 Получение данных с Wildberries API
Для получения заказов и продаж используется официальный WB API:
ini
КопироватьРедактировать
[Method_API]
WB_GET_ALL_ORDERS = https://statistics-api.wildberries.ru/api/v1/supplier/orders
WB_GET_ALL_SALES = https://statistics-api.wildberries.ru/api/v1/supplier/sales
Пример запроса на Python:
python
КопироватьРедактировать
r = rq.get(url=WB_GET_ALL_SALES, headers={"Authorization": token_WB}, params=params)
if r.status_code == 200:
    lst_all_sales = r.json()
Реализована обработка ошибки 429 (превышение лимита запросов):
python
КопироватьРедактировать
for _ in range(4):
    r = rq.get(...)
    if r.status_code == 429:
        tm.sleep(1.1)
________________________________________
🧹 Обработка и агрегация данных
С помощью pandas:
•	очищаю и объединяю данные заказов и продаж;
•	добавляю флаги успешных заказов, продаж и возвратов;
•	группирую по SKU и округам.
Пример расчёта флагов:
python
КопироватьРедактировать
df_all_sales["success_sale"] = df_all_sales["saleID"].apply(lambda x: 1 if x[0] == "S" else 0)
df_all_orders["success_order"] = df_all_orders["isCancel"].apply(lambda x: 0 if x else 1)
И агрегации:
python
КопироватьРедактировать
df_sales = df_all_sales.groupby("barcode")[["totalPrice", "success_sale", "success_return"]].sum().reset_index()
df_orders = df_all_orders.groupby("barcode")[["success_order", "success_cancel"]].sum().reset_index()
df_orders_and_sales = df_sales.merge(df_orders, how="outer")
________________________________________
🗃️ Загрузка в PostgreSQL
Для подключения к БД использую psycopg2. Обёртка:
python
КопироватьРедактировать
class PGDatabase:
    def __init__(...):
        self.connection = psycopg2.connect(...)
        self.cursor = self.connection.cursor()
        self.connection.autocommit = True

    def post(self, query, args=()):
        try:
            self.cursor.execute(query, args)
        except Exception as err:
            print(repr(err))
Данные вставляются в несколько таблиц:
python
КопироватьРедактировать
for i, row in df_orders_and_sales.iterrows():
    query = f"INSERT INTO orders_and_sales VALUES (...)"
    database.post(query)
________________________________________
📊 Визуализация в Metabase
Metabase подключён к PostgreSQL. Настроены дашборды:
•	Продажи и возвраты по SKU;
•	География заказов по федеральным округам;
•	Топ-бренды и категории;
•	Динамика заказов по дням.
Интерфейс Metabase позволяет фильтровать по дате, бренду, региону.
________________________________________
🖥️ Размещение на сервере и автообновление
Я разместил проект на своём Linux-сервере и настроил автоматическое обновление через cron.
Файл crontab:
bash
КопироватьРедактировать
0 7 * * * /usr/bin/python3 /home/user/scripts/wb_etl.py >> /home/user/logs/wb_etl.log 2>&1
Скрипт запускается ежедневно в 07:00, данные обновляются, и дашборды в Metabase всегда актуальны.
________________________________________
✅ Результат
•	Полностью автоматизированный ETL-процесс;
•	PostgreSQL как единое хранилище;
•	Удобные визуализации в Metabase;
•	Стабильное ежедневное обновление без ручного участия.
________________________________________
🚀 Планы на развитие
•	Добавить логирование и мониторинг ошибок;
•	Настроить Telegram-уведомления в случае сбоя;
•	Перевести пайплайн в Airflow;
•	Сделать версионирование и хранение истории данных.
________________________________________
📌 Заключение
Этот проект стал отличным примером построения сквозной аналитической цепочки: от получения данных с внешнего API до визуализации в BI-инструменте.
Если интересно — могу выложить шаблон проекта на GitHub.
Спасибо за внимание! 🙌

