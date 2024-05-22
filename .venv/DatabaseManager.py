import os
import asyncpg
from dotenv import load_dotenv
import openpyxl
import psycopg2

load_dotenv("parcer.env")

# Получение значений переменных среды
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

class AsyncDatabaseManager:
    def __init__(self):
        self.pool = None

    async def connect_to_database(self):
        try:
            self.pool = await asyncpg.create_pool(
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                host=DB_HOST,
                port=DB_PORT
            )
            print("Соединение с базой данных установлено")
        except asyncpg.exceptions.PostgresError as e:
            print("Не удалось подключиться к базе данных")
            print(e)

    async def create_table(self):
        try:
            async with self.pool.acquire() as connection:
                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS adidas_products (
                        product_id TEXT PRIMARY KEY,
                        brand TEXT,
                        category TEXT,
                        model_name TEXT,
                        color TEXT,
                        price TEXT,
                        discount TEXT,
                        image_url TEXT
                    )
                """)
            print("Таблица успешно создана")
        except asyncpg.exceptions.PostgresError as e:
            print("Ошибка создания таблицы")
            print(e)

    async def insert_data(self, data):
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    for item in data:
                        print(item['product_id'], item['brand'], item['category'], item['model_name'], item['color'], item['price'],
                            item['discount'], item['image_url'], sep = ':')
                        await connection.execute("""
                            INSERT INTO adidas_products (product_id, brand, category, model_name, color, price, discount, image_url)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (product_id) DO UPDATE
                            SET brand = COALESCE(EXCLUDED.brand, adidas_products.brand),
                                category = COALESCE(EXCLUDED.category, adidas_products.category),
                                model_name = COALESCE(EXCLUDED.model_name, adidas_products.model_name),
                                color = COALESCE(EXCLUDED.color, adidas_products.color),
                                price = COALESCE(EXCLUDED.price, adidas_products.price),
                                discount = COALESCE(EXCLUDED.discount, adidas_products.discount),
                                image_url = COALESCE(EXCLUDED.image_url, adidas_products.image_url)
                        """,
                            item['product_id'], item['brand'], item['category'], item['model_name'], item['color'], str(item['price']),
                            str(item['discount']), item['image_url']
                        )
            print('Данные успешно вставлены в базу данных')
        except asyncpg.exceptions.PostgresError as e:
            print("Ошибка вставки данных в базу данных")
            print(e)

    async def export_to_excel(self, filename):
        async with self.pool.acquire() as connection:
            try:
                records = await connection.fetch("SELECT * FROM adidas_products")
                workbook = openpyxl.Workbook()
                sheet = workbook.active
                sheet.append(
                    ['Product ID', 'Brand', 'Category', 'Model Name', 'Color', 'Price', 'Discount', 'Image URL',
                     'Sizes'])
                for record in records:
                    # Преобразование объекта asyncpg.Record в список
                    record_list = list(record.values())
                    sheet.append(record_list)
                workbook.save(filename)
                print(f"Данные успешно экспортированы в файл {filename}")
            except asyncpg.exceptions.PostgresError as e:
                print("Ошибка экспорта данных в Excel")
                print(e)
    async def close_connection(self):
        await self.pool.close()
        print("Соединение с базой данных закрыто")

