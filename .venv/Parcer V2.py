import os
import time
import random
import requests
from bs4 import BeautifulSoup
import openpyxl
import psycopg2
from dotenv import load_dotenv
from fake_useragent import UserAgent

load_dotenv("parcer.env")

# Получение значений переменных среды
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

ua = UserAgent()

def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connected to the database")
        return conn
    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e)
        return None


def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""          
            -- Создание таблицы
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
    conn.commit()
    cursor.close()


def insert_data(conn, data):
    cursor = conn.cursor()
    for item in data:
        cursor.execute("""
            INSERT INTO adidas_products (product_id, model_name, color, price, discount, image_url, sizes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_id) DO UPDATE
            SET model_name = COALESCE(adidas_products.model_name, EXCLUDED.model_name),
                color = COALESCE(adidas_products.color, EXCLUDED.color),
                price = COALESCE(adidas_products.price, EXCLUDED.price),
                discount = COALESCE(adidas_products.discount, EXCLUDED.discount),
                image_url = COALESCE(adidas_products.image_url, EXCLUDED.image_url),
                sizes = COALESCE(adidas_products.sizes, EXCLUDED.sizes)
        """, (item['product_id'], item['model_name'], item['color'], item['price'], item['discount'], item['image_url'],
              item['sizes']))
    conn.commit()
    cursor.close()
    print('Записали в бд')


def scrape_adidas():
    scraped_data = []
    conn = connect_to_database()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT product_id FROM adidas_products")
        product_ids = cursor.fetchall()
        cursor.close()
        conn.close()
        for product_id in product_ids:
            product_data = fetch_adidas_data(product_id[0])
            if product_data:
                parsed_data = parse_adidas_json(product_data)
                scraped_data.append(parsed_data)
                time.sleep(random.uniform(1, 3))  # Добавляем случайную задержку перед каждым запросом, чтобы избежать блокировки
    return scraped_data


def scrape_product_ids_and_URLs():
    url = "https://www.adidas.com/us/men-shoes"

    headers = {
        "User-Agent": ua.random,
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

    try:
        session = requests.Session()
        session.headers.update(headers)

        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return []
    except requests.exceptions.RequestException as err:
        print(f"Other error occurred: {err}")
        return []

    print("Successfully retrieved the webpage")

    soup = BeautifulSoup(response.content, 'html.parser')
    product_cards = soup.find_all('div', class_='grid-item')

    products = []
    for container in product_cards:
        product_id = container.get('data-grid-id')  # Получаем значение атрибута data-grid-id
        image_url = container.find('img')['src']

        print(f"Product ID: {product_id}, Image URL: {image_url}")

        products.append({
            'model_name' : None,
            'color': None,
            'price': None,
            'sizes': None,
            'discount': None,
            'product_id': product_id,
            'image_url': image_url
        })
    return products


def fetch_adidas_data(product_id):
    url = f"https://www.adidas.com/api/search/product/{product_id}?sitePath=us"
    headers = {
        "User-Agent": ua.random,
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Adidas data fetched successfully")
        return response.json()
    else:
        print("Failed to fetch Adidas data:", response.status_code)
        return None


def parse_adidas_json(json_data):
    print("Parsing Adidas JSON data:", json_data)
    product_info = {}
    product_info["product_id"] = json_data["id"]
    product_info["model_name"] = json_data["name"]
    product_info["color"] = json_data["color"]
    product_info["price"] = json_data["price"]
    product_info["discount"] = json_data.get("salePrice", "")  # Обратите внимание, что salePrice может отсутствовать в ответе
    product_info["image_url"] = json_data.get("image", {}).get("url", "")  # Здесь можно добавить логику для получения URL изображения
    product_info["sizes"] = ", ".join(json_data.get("sizes", []))  # Преобразование списка размеров в строку
    return product_info



def export_to_excel(conn, tag):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM adidas_products")
    records = cursor.fetchall()
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(['Product ID', 'Model Name', 'Color', 'Price', 'Discount', 'Image URL', 'Sizes'])
    for record in records:
        sheet.append(record)
    workbook.save(tag)
    cursor.close()
    print("Data exported to Excel successfully")


if __name__ == "__main__":
    conn = connect_to_database()
    export_to_excel(conn, 'adidas_products.xlsx')
    if conn:
        create_table(conn)
        data = scrape_product_ids_and_URLs()
        insert_data(conn, data)
        export_to_excel(conn, 'adidas_products1.xlsx')
        data = scrape_adidas()
        insert_data(conn, data)
        export_to_excel(conn, 'adidas_products2.xlsx')
        conn.close()
