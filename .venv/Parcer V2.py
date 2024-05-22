"""
Version 3: Refactored for OOP standards and added asynchronous functionality.

This version of the code introduces a more organized structure using object-oriented programming (OOP) principles
and leverages asynchronous programming for improved performance.

Changes Made:
- Refactored the code to use classes for DatabaseManager and AdidasScraper.
- Introduced asynchronous functionality using asyncio and aiohttp.
- Added type hints for better code readability and maintainability.
"""


import asyncpg
import os
import random
import asyncio
import aiohttp
import openpyxl
import psycopg2
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from dotenv import load_dotenv
from DatabaseManager import AsyncDatabaseManager


load_dotenv("parcer.env")

# Получение значений переменных среды
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


class AdidasScraper:
    def __init__(self):
        self.ua = UserAgent()

    async def fetch_with_delay(self, session, url):
        await asyncio.sleep(random.uniform(1, 34))
        return await self.fetch(session, url)

    async def fetch(self, session: aiohttp.ClientSession, url: str, retry_count: int = 2):
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
                        print(f"403 Forbidden для {url}, повторная попытка через {2 ** attempt} секунды...")
                        await asyncio.sleep(2 ** attempt)
                    else:
                        print(f"Неудачный запрос для {url}, статус: {response.status}")
                        return None
            except aiohttp.ClientResponseError as e:
                print(f"Ошибка клиентского ответа: {e}, повторная попытка через {2 ** attempt} секунды...")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"Исключение: {e}, повторная попытка через {2 ** attempt} секунды...")
                await asyncio.sleep(2 ** attempt)

        print(f"Не удалось выполнить запрос для {url} после {retry_count} попыток")
        return None

    async def pages(self, url: str):
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, url)
            soup = BeautifulSoup(html, 'html.parser')
            span = soup.find("span", {"class": "gl-body gl-body--s gl-no-margin-bottom",
                                      "data-auto-id": "pagination-pages-container"})
            text = span.get_text(strip=True)
            number = int(text.split("of")[1].strip())
            return number

    async def scrape_product_ids_and_URLs(self, url: str):
        async with aiohttp.ClientSession() as session:
            html = await self.fetch_with_delay(session, url)
            if html is not None:
                soup = BeautifulSoup(html, 'html.parser')
                product_cards = soup.find_all('div', class_='grid-item')

                products = {}
                for container in product_cards:
                    product_id = container.get('data-grid-id')
                    image_url = container.find('img')['src']
                    print(f"Product ID: {product_id}, Image URL: {image_url}")

                    products[product_id] = {
                        'brand': None,
                        'category': None,
                        'model_name': None,
                        'color': None,
                        'price': None,
                        'discount': None,
                        'product_id': product_id,
                        'image_url': image_url
                    }
                return products
            else:
                print(f"Пустой HTML-код для URL: {url}")
                # Отправляем повторный запрос в случае пустого HTML-кода
                print(f"Повторный запрос для URL: {url}")
                html_retry = await self.fetch_with_delay(session, url)
                if html_retry is not None:
                    soup_retry = BeautifulSoup(html_retry, 'html.parser')
                    product_cards_retry = soup_retry.find_all('div', class_='grid-item')

                    products_retry = {}
                    for container_retry in product_cards_retry:
                        product_id_retry = container_retry.get('data-grid-id')
                        image_url_retry = container_retry.find('img')['src']
                        print(f"Product ID: {product_id_retry}, Image URL: {image_url_retry}")

                        products_retry[product_id_retry] = {
                            'brand': None,
                            'category': None,
                            'model_name': None,
                            'color': None,
                            'price': None,
                            'discount': None,
                            'product_id': product_id_retry,
                            'image_url': image_url_retry
                        }
                    return products_retry
                else:
                    print(f"Повторный запрос не вернул данные для URL: {url}")
                    return {}

    async def fetch_adidas_data(self, session: aiohttp.ClientSession, product_id: str, retry_count: int = 2):
        await asyncio.sleep(random.uniform(0.5, 2))
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
                        print(f"403 Forbidden for {url}, retrying in {attempt + 1} second...")
                        await asyncio.sleep(attempt + 1)
                    else:
                        response.raise_for_status()
            except aiohttp.ClientResponseError as e:
                print(f"ClientResponseError: {e}, retrying in {attempt + 1} second...")
                await asyncio.sleep(attempt + 1)
            except Exception as e:
                print(f"Exception: {e}, retrying in {attempt + 1} second...")
                await asyncio.sleep(attempt + 1)

        print(f"Failed to fetch Adidas data for {product_id} after {retry_count} attempts")
        return None

    async def parse_adidas_json(self, json_data):
        print("Parsing Adidas JSON data:", json_data)
        product_info = {
            "brand": json_data["brand"],
            "category": json_data["category"],
            "product_id": json_data["id"],
            "model_name": json_data["name"],
            "color": json_data["color"],
            "price": json_data["price"],
            "discount": json_data.get("salePrice", ""),
            "image_url": json_data.get("image", {}).get("url", "")
        }
        return product_info

    async def scrape_adidas(self, product_data):
        async with aiohttp.ClientSession() as session:
            tasks = []
            await asyncio.sleep(random.uniform(0.5, 2))
            for product_id in product_data.keys():
                task = asyncio.create_task(self.fetch_adidas_data_with_delay(session, product_id))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            updated_data = {}
            for response in responses:
                if response and "id" in response:
                    product_id = response["id"]
                    updated_data[product_id] = await self.parse_adidas_json(response)
            return updated_data

    async def fetch_adidas_data_with_delay(self, session, product_id):
        await asyncio.sleep(random.uniform(1, 40))
        return await self.fetch_adidas_data(session, product_id)

    async def process_page(self, db_manager: AsyncDatabaseManager, url: str):
        product_data = await self.scrape_product_ids_and_URLs(url)
        detailed_data = await self.scrape_adidas(product_data)
        for key, value in detailed_data.items():
            for key1, value1 in value.items():
                if key1 != "image_url":
                    product_data[key][key1] = detailed_data[key][key1]

        await db_manager.insert_data(product_data.values())

        await asyncio.sleep(random.uniform(0.5, 2))


async def main():
    db_manager = AsyncDatabaseManager()
    await db_manager.connect_to_database()
    await db_manager.create_table()

    scraper = AdidasScraper()
    base_url = "https://www.adidas.com/us/men-shoes"

    first_page_url = base_url
    await scraper.process_page(db_manager, first_page_url)

    num_pages = await scraper.pages(base_url)
    urls = [f"{base_url}?start={i * 48}" for i in range(1, num_pages)]
    tasks = [scraper.process_page(db_manager, url) for url in urls]

    await asyncio.gather(*tasks)

    await db_manager.export_to_excel('adidas_products_final.xlsx')
    await db_manager.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
