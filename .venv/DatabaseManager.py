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
                        image_side_url TEXT, 
                        image_top_url TEXT, 
                        image_34_url TEXT, 
                        gender TEXT
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
                        await connection.execute("""
                            INSERT INTO adidas_products (product_id, brand, category, model_name, color, price, discount, image_side_url, image_top_url, image_34_url, gender)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            ON CONFLICT (product_id) DO UPDATE
                            SET brand = COALESCE(EXCLUDED.brand, adidas_products.brand),
                                category = COALESCE(EXCLUDED.category, adidas_products.category),
                                model_name = COALESCE(EXCLUDED.model_name, adidas_products.model_name),
                                color = COALESCE(EXCLUDED.color, adidas_products.color),
                                price = COALESCE(EXCLUDED.price, adidas_products.price),
                                discount = COALESCE(EXCLUDED.discount, adidas_products.discount),
                                image_side_url = COALESCE(EXCLUDED.image_side_url, adidas_products.image_side_url),
                                image_top_url = COALESCE(EXCLUDED.image_top_url, adidas_products.image_top_url),
                                image_34_url = COALESCE(EXCLUDED.image_34_url, adidas_products.image_34_url),
                                gender = COALESCE(EXCLUDED.gender, adidas_products.gender)
                        """,
                            item['product_id'], item['brand'], item['category'], item['model_name'], item['color'], str(item['price']),
                            str(item['discount']), item['image_side_url'], item['image_top_url'], item['image_34_url'], item['gender']
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
                    ['Product ID', 'Brand', 'Category', 'Model Name', 'Color', 'Price', 'Discount', 'Image Side URL', 'Image Tor URL', 'Image 3/4 URL',
                     'Gender'])
                for record in records:
                    # Преобразование объекта asyncpg.Record в список
                    record_list = list(record.values())
                    sheet.append(record_list)
                workbook.save(filename)
                print(f"Данные успешно экспортированы в файл {filename}")
            except asyncpg.exceptions.PostgresError as e:
                print("Ошибка экспорта данных в Excel")
                print(e)

    async def get_products_without_brand(self):
        try:
            async with self.pool.acquire() as connection:
                product_ids = await connection.fetch("SELECT product_id FROM adidas_products WHERE brand IS NULL")
                products = {
                    product_id['product_id']: {
                        'brand': None,
                        'category': None,
                        'model_name': None,
                        'color': None,
                        'price': None,
                        'discount': None,
                        'product_id': product_id['product_id'],
                        'image_side_url': None,
                        'image_top_url': None,
                        'image_34_url': None,
                        'gender': None
                    }
                    for product_id in product_ids
                }
                return products
        except asyncpg.exceptions.PostgresError as e:
            print("Ошибка получения продуктов без бренда")
            print(e)
            return {}

    async def close_connection(self):
        await self.pool.close()
        print("Соединение с базой данных закрыто")

