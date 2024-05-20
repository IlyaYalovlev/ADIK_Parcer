"""
Version 3: Refactored for OOP standards and added asynchronous functionality.

This version of the code introduces a more organized structure using object-oriented programming (OOP) principles
and leverages asynchronous programming for improved performance.

Changes Made:
- Refactored the code to use classes for DatabaseManager and AdidasScraper.
- Introduced asynchronous functionality using asyncio and aiohttp.
- Added type hints for better code readability and maintainability.
"""


import os
import random
import asyncio
import aiohttp
import openpyxl
import psycopg2
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from dotenv import load_dotenv

load_dotenv("parcer.env")


class DatabaseManager:
    def __init__(self):
        self.conn = self.connect_to_database()

    def connect_to_database(self):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            print("Connected to the database")
            return conn
        except psycopg2.Error as e:
            print("Unable to connect to the database")
            print(e)
            return None

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS adidas_products (
                product_id TEXT PRIMARY KEY,
                model_name TEXT,
                color TEXT,
                price TEXT,
                discount TEXT,
                image_url TEXT,
                sizes TEXT
            );
        """)
        self.conn.commit()
        cursor.close()

    def insert_data(self, data):
        cursor = self.conn.cursor()
        for item in data:
            cursor.execute("""
                INSERT INTO adidas_products (product_id, model_name, color, price, discount, image_url, sizes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET model_name = COALESCE(EXCLUDED.model_name, adidas_products.model_name),
                    color = COALESCE(EXCLUDED.color, adidas_products.color),
                    price = COALESCE(EXCLUDED.price, adidas_products.price),
                    discount = COALESCE(EXCLUDED.discount, adidas_products.discount),
                    image_url = COALESCE(EXCLUDED.image_url, adidas_products.image_url),
                    sizes = COALESCE(EXCLUDED.sizes, adidas_products.sizes)
            """, (
                item['product_id'], item['model_name'], item['color'], item['price'], item['discount'],
                item['image_url'],
                item['sizes']))
        self.conn.commit()
        cursor.close()
        print('Data inserted into the database')

    def export_to_excel(self, filename):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM adidas_products")
        records = cursor.fetchall()
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.append(['Product ID', 'Model Name', 'Color', 'Price', 'Discount', 'Image URL', 'Sizes'])
        for record in records:
            sheet.append(record)
        workbook.save(filename)
        cursor.close()
        print(f"Data exported to {filename} successfully")

    def close_connection(self):
        self.conn.close()


class AdidasScraper:
    def __init__(self):
        self.ua = UserAgent()

    async def fetch_with_delay(self, session, url):
        await asyncio.sleep(random.uniform(2, 5))  # Добавляем случайную задержку
        return await self.fetch(session, url)

    async def fetch(self, session: aiohttp.ClientSession, url: str, retry_count: int = 3):
        headers = {
            "User-Agent": self.ua.random,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Referer": "https://www.adidas.com/us",
            "DNT": "1",
            "TE": "Trailers",
        }

        for attempt in range(retry_count):
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 403:
                        print(f"403 Forbidden for {url}, retrying in {2 ** attempt} seconds...")
                        await asyncio.sleep(2 ** attempt)
                    else:
                        response.raise_for_status()
            except aiohttp.ClientResponseError as e:
                print(f"ClientResponseError: {e}, retrying in {2 ** attempt} seconds...")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"Exception: {e}, retrying in {2 ** attempt} seconds...")
                await asyncio.sleep(2 ** attempt)

        raise aiohttp.ClientResponseError(
            request_info=None,
            history=(),
            status=403,
            message='Forbidden',
            headers=None
        )

    async def pages(self, url: str):
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, url)
            soup = BeautifulSoup(html, 'html.parser')
            span = soup.find("span", {"class": "gl-body gl-body--s gl-no-margin-bottom",
                                      "data-auto-id": "pagination-pages-container"})
            text = span.get_text(strip=True)
            number = int(text.split("of")[1].strip('"'))
            return number

    async def scrape_product_ids_and_URLs(self, url: str):
        async with aiohttp.ClientSession() as session:
            html = await self.fetch_with_delay(session, url)
            soup = BeautifulSoup(html, 'html.parser')
            product_cards = soup.find_all('div', class_='grid-item')

            products = {}
            for container in product_cards:
                product_id = container.get('data-grid-id')
                image_url = container.find('img')['src']
                print(f"Product ID: {product_id}, Image URL: {image_url}")

                products[product_id] = {
                    'model_name': None,
                    'color': None,
                    'price': None,
                    'sizes': None,
                    'discount': None,
                    'product_id': product_id,
                    'image_url': image_url
                }
            return products

    async def fetch_adidas_data(self, session: aiohttp.ClientSession, product_id: str, retry_count: int = 5):
        url = f"https://www.adidas.com/api/search/product/{product_id}?sitePath=us"
        headers = {
            "User-Agent": self.ua.random,
            "Accept": "application/json",
        }

        for attempt in range(retry_count):
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        print(f"Adidas data for {product_id} fetched successfully")
                        return await response.json()
                    elif response.status == 403:
                        print(f"403 Forbidden for {url}, retrying in {2 ** attempt} seconds...")
                        await asyncio.sleep(2 ** attempt)
                    else:
                        response.raise_for_status()
            except aiohttp.ClientResponseError as e:
                print(f"ClientResponseError: {e}, retrying in {2 ** attempt} seconds...")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"Exception: {e}, retrying in {2 ** attempt} seconds...")
                await asyncio.sleep(2 ** attempt)

        print(f"Failed to fetch Adidas data for {product_id} after {retry_count} attempts")
        return None

    async def parse_adidas_json(self, json_data):
        print("Parsing Adidas JSON data:", json_data)
        product_info = {
            "product_id": json_data["id"],
            "model_name": json_data["name"],
            "color": json_data["color"],
            "price": json_data["price"],
            "discount": json_data.get("salePrice", ""),
            "image_url": json_data.get("image", {}).get("url", ""),
            "sizes": ", ".join(json_data.get("sizes", []))
        }
        return product_info

    async def scrape_adidas(self, product_data):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for product_id in product_data.keys():
                task = asyncio.ensure_future(self.fetch_adidas_data_with_delay(session, product_id))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            updated_data = {}
            for response in responses:
                if response and "id" in response:
                    product_id = response["id"]
                    updated_data[product_id] = await self.parse_adidas_json(response)
            return updated_data

    async def fetch_adidas_data_with_delay(self, session, product_id):
        await asyncio.sleep(random.uniform(0.3, 1))
        return await self.fetch_adidas_data(session, product_id)


async def main():
    db_manager = DatabaseManager()
    db_manager.create_table()

    scraper = AdidasScraper()
    base_url = "https://www.adidas.com/us/men-shoes"

    # Получаем данные с первой страницы
    first_page_url = base_url
    first_page_data = await scraper.scrape_product_ids_and_URLs(first_page_url)
    print("Type of first_page_data:", type(first_page_data))  # Добавим эту строку для отладки
    detailed_data = await scraper.scrape_adidas(first_page_data)
    for key, value in detailed_data.items():
        for key1, value1 in value.items():
            if key1 != "image_url":
                first_page_data[key][key1] = detailed_data[key][key1]
    db_manager.insert_data(first_page_data.values())

    # Параллельно обрабатываем остальные страницы
    num_pages = await scraper.pages(base_url)
    urls = [f"{base_url}?start={i * 48}" for i in range(1, num_pages)]  # Начинаем с 1, так как первую страницу уже обработали
    tasks = []

    for url in urls:
        tasks.append(scraper.scrape_product_ids_and_URLs(url))

    all_pages_data = await asyncio.gather(*tasks)

    for data in all_pages_data:
        detailed_data = await scraper.scrape_adidas(data)
        for key, value in detailed_data.items():
            for key1, value1 in value.items():
                if key1 != "image_url":
                    data[key][key1] = detailed_data[key][key1]
        db_manager.insert_data(data.values())

    db_manager.export_to_excel('adidas_products_final.xlsx')
    db_manager.close_connection()

if __name__ == "__main__":
    asyncio.run(main())

