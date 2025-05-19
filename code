import asyncio
from playwright.async_api import async_playwright, Playwright
from bs4 import BeautifulSoup
import json
import time
import logging
import random
import pandas as pd
from typing import Dict, List, Optional

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Список User-Agent для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Из твоих заголовков
]

# Куки из Request Headers для Twix (замени __cf_bm на свежую)
COOKIES = [
    {"name": "__Secure-ab-group", "value": "59", "domain": ".ozon.ru", "path": "/"},
    {"name": "__Secure-user-id", "value": "0", "domain": ".ozon.ru", "path": "/"},
    {"name": "xcid", "value": "5076d374a6d123713481cc3fca89dddf", "domain": ".ozon.ru", "path": "/"},
    {"name": "__Secure-ext_xcid", "value": "5076d374a6d123713481cc3fca89dddf", "domain": ".ozon.ru", "path": "/"},
    {"name": "guest", "value": "true", "domain": ".ozon.ru", "path": "/"},
    {"name": "is_cookies_accepted", "value": "1", "domain": "www.ozon.ru", "path": "/"},
    {"name": "__Secure-ETC", "value": "139e9e5385a1b41fe3c5427928cb7d52", "domain": ".ozon.ru", "path": "/"},
    {"name": "__Secure-access-token",
     "value": "8.0._M7lbt0kQcWvJgwhqNmFUw.59.AQMggvqxB-UpaFMTaQk14f1DZ6MSKixYeZzZHApaw74CjIyqZw4oFLlzzslMrQaKhYrPv5iaOLXFJgIa01DNqQLL_STTuKFn5M90eo0gZb7j..20250519221325.D4ZMGfe9PIvB2kl0W8j1GN4zwESU0fVT_PIf0rC07Os.145616f87ae4d771d",
     "domain": ".ozon.ru", "path": "/"},
    {"name": "__Secure-refresh-token",
     "value": "8.0._M7lbt0kQcWvJgwhqNmFUw.59.AQMggvqxB-UpaFMTaQk14f1DZ6MSKixYeZzZHApaw74CjIyqZw4oFLlzzslMrQaKhYrPv5iaOLXFJgIa01DNqQLL_STTuKFn5M90eo0gZb7j..20250519221325.GawNur9MYyMmx9RQZBkxqfV_l8kY1vFTOUbH1kA6tnY.1ebcf13243314a26a",
     "domain": ".ozon.ru", "path": "/"},
    {"name": "ADDRESSBOOKBAR_WEB_CLARIFICATION", "value": "1747685606", "domain": ".ozon.ru", "path": "/"},
    {"name": "rfuid",
     "value": "LTE5NTAyNjU0NzAsMTI0LjA0MzQ3NTI3NTE2MDc0LDEwMjgyMzcyMjMsLTEsLTE5MDAwNDk4MTUsVzNzaWJtRnRaU0k2SWxCRVJpQldhV1YzWlhJaUxDSmtaWE5qY21sd2RHbHZiaUk2SWxCdmNuUmhZbXhsSUVSdlkzVnRaVzUwSUVadmNtMWhkQ0lzSW0xcGJXVlVlWEJsY3lJNlczc2lkSGx3WlNJNkltRndjR3hwWTJGMGFXOXVMM0JrWmlJc0luTjFabVpwZUdWeklqb2ljR1JtSW4wc2V5SjBlWEJsSWpvaWRHVjRkQzl3WkdZaUxDSnpkV1ptYVhobGN5STZJbkJrWmlKOVhYMHNleUp1WVcxbElqb2lRMmh5YjIxbElGQkVSaUJXYVdWM1pYSWlMQ0prWlhOamNtbHdkR2x2YmlJNklsQnZjblJoWW14bElFUnZZM1Z0Wlc1MElFWnZjbTFoZENJc0ltMXBiV1ZVZVhCbGN5STZXM3NpZEhsd1pTSTZJbUZ3Y0d4cFkyRjBhVzl1TDNCa1ppSXNJbk4xWm1acGVHVnpJam9pY0dSbUluMHNleUowZVhCbElqb2lkR1Y0ZEM5d1pHWWlMQ0p6ZFdabWFYaGxjeUk2SW5Ca1ppSjlYWDBzZXlKdVlXMWxJam9pUTJoeWIyMXBkVzBnVUVSR0lGWnBaWGRsY2lJc0ltUmxjMk55YVhCMGFXOXVJam9pVUc5eWRHRmliR1VnUkc5amRXMWxiblFnUm05eWJXRjBJaXdpYldsdFpWUjVjR1Z6SWpwYmV5SjBlWEJsSWpvaVlYQndiR2xqWVhScGIyNHZjR1JtSWl3aWMzVm1abWw0WlhNaU9pSndaR1lpZlN4N0luUjVjR1VpT2lKMFpYaDBMM0JrWmlJc0luTjFabVpwZUdWeklqb2ljR1JtSW4xZGZTeDdJbTVoYldVaU9pSk5hV055YjNOdlpuUWdSV1JuWlNCUVJFWWdWbWxsZDJWeUlpd2laR1Z6WTNKcGNIUnBiMjRpT2lKUWIzSjBZV0pzWlNCRWIyTjFiV1Z1ZENCR2IzSnRZWFFpTENKdGFXMWxWSGx3WlhNaU9sdDdJblI1Y0dVaU9pSmhjSEJzYVdOaGRHbHZiaTl3WkdZaUxDSnpkV1ptYVhobGN5STZJbkJrWmlKOUxIc2lkSGx3WlNJNkluUmxlSFF2Y0dSbUlpd2ljM1ZtWm1sNFpYTWlPaUp3WkdZaWZWMTlMSHNpYm1GdFpTSTZJbGRsWWt0cGRDQmlkV2xzZEMxcGJpQlFSRVlpTENKa1pYTmpjbWx3ZEdsdmJpSTZJbEJ2Y25SaFlteGxJRVJ2WTNWdFpXNTBJRVp2Y20xaGRDSXNJbTFwYldWVWVYQmxjeUk2VzNzaWRIbHdaU0k2SW1Gd2NHeHBZMkYwYVc9dUwzQmtaaUlzSW5OMVptWnBlR1Z6SWpvaWNHUm1JbjBzZXlKMGVYQmxJam9pZEdWNGRDOXdaR1lpTENKemRXWm1hWGhsY3lJNkluQmtaaUo5WFgxZCxXeUp5ZFMxU1ZTSmQsMCwxLDAsMjQsMjM3NDE1OTMwLDgsMjI3MTI2NTIwLDAsMSwwLC00OTEyNzU1MjMsUjI5dloyeGxJRWx1WXk0Z1RtVjBjMk5oY0dVZ1IyVmphMjhnVjJsdU16SWdOUzR3SUNoWGFXNWtiM2R6SUU1VUlERXdMakE3SUZkcGJqWTBPeUI0TmpRcElFRndjR3hsVjJWaVMybDBMelV6Tnk0ek5pQW9TMGhVVFV3c0lHeHBhMlVnUjJWamEyOHBJRU5vY205dFpTOHhNell1TUM0d0xqQWdVMkZtWVhKcEx6VXpOeTR6TmlBeU1EQXpNREV3TnlCTmIzcHBiR3hoLGV5SmphSEp2YldVaU9uc2lZWEJ3SWpwN0ltbHpTVzV6ZEdGc2JHVmtJanBtWVd4elpTd2lTVzV6ZEdGc2JGTjBZWFJsSWpwN0lrUkpVMEZDVEVWRUlqb2laR2x6WVdKc1pXUWlMQ0pKVGxOVVFVeE1SVVFpT2lKcGJuTjBZV3hzWldRaUxDSk9UMVJmU1U1VFZFRk1URVZFSWpvaWJtOTBYMmx1YzNSaGJHeGxaQ0o5TENKU2RXNXVhVzVuVTNSaGRHVWlPbnNpUTBGT1RrOVVYMUpWVGlJNkltTmhibTV2ZEY5eWRXNGlMQ0pTUlVGRVdWOVVUMTlTVlU0aU9pSnlaV0ZrZVY5MGIxOXlkVzRpTENKU1ZVNU9TVTVISWpvaWNuVnVibWx1WnlKOWZYMTksNjUsLTEyODU1NTEzLDEsMSwtMSwxNjk5OTU0ODg3LDE2OTk5NTQ4ODcsMzM2MDA3OTMzLDg=; abt_data=7.d0jpqTk-ySpH0TQlZDculAUX81olsMxRtTCEWTqXw6ope1-iSsG7HNVZt7KslCJqAJhCPbPUT86P0VkHnmpUzDP-SD5HgmwhJRw5KG-a2L6UQ2rvORjkGCAO9S7GdsvzaacrtqKEG-UCk6hLdHCh0doRMRN7zKfIJZ7hDtFCUUBiOtyeBWONeCAyAwQWxDgd_XMtk8_jQW_Zw8sAm5Kl6vTxiZjibaT_MENH8AIAK-kLgopaXpkb-AGUo8q6PfwNU5Kxlw1KPKi4YpEHWKVHjKkRMg1oIih6UK94Ju0XoYxS8vWX1cdlT3gAG8HbYQilURqKr2OQZv1tSuGzifRLz7H4gQufCt_InbCtlzEQJSiVIzh32CiWtWGDk2sYbBBIOJadf0sHvTaBmbTuLsOPiOByFdYAQvB_SpPidBqziPBk1qJ4wYQJnFBusyxhiFWEK_d-SUhVGYp_-Qyek_pkuV0bUzLXWlwfuWfm_11ygGJ_bMEYJn-Zy7HeLgg9n9fEkUTfYq_Sj2ITCC8l_-PHwi5A4od_7y8WQW2567o-5Biu5tWP-LJDcBJZuXHG1TkxZLqB6hDbIhQkxDt483MM",
     "domain": ".ozon.ru", "path": "/"},
    {"name": "__cf_bm", "value": "your_fresh_cf_bm_value", "domain": ".ozon.ru", "path": "/"},
]


async def fetch_page(url: str, playwright: Playwright) -> Optional[str]:
    """Получение HTML страницы через Playwright Async API без прокси."""
    browser = None
    try:
        print(f"Запуск браузера для {url} без прокси")
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                f"--user-agent={random.choice(USER_AGENTS)}",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        context = await browser.new_context()

        # Устанавливаем куки
        await context.add_cookies(COOKIES)

        page = await context.new_page()
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(3)  # Ожидание загрузки JavaScript

        html = await page.content()
        if "cf-browser-verification" in html.lower():
            logger.error(f"Cloudflare защита на {url} без прокси")
            print(f"Cloudflare заблокировал запрос. Обновите __cf_bm или попробуйте другой IP.")
            return None

        print(f"Успешно загружена страница {url} без прокси")
        return html
    except Exception as e:
        logger.error(f"Ошибка при загрузке {url} без прокси: {e}")
        print(f"Ошибка: {e}")
        return None
    finally:
        if browser:
            await browser.close()


def parse_product(html: str, url: str, product_id: str) -> Optional[Dict]:
    """Парсинг данных карточки товара."""
    start_time = time.time()
    soup = BeautifulSoup(html, 'html.parser')

    try:
        # Название товара
        title_elem = soup.find('h1', class_=lambda x: x and 'title' in x.lower())
        title = title_elem.text.strip() if title_elem else "Не найдено"

        # Категория (из хлебных крошек)
        category_elem = soup.find('a', class_=lambda x: x and 'breadcrumb' in x.lower())
        category = category_elem.text.strip() if category_elem else "Не найдено"

        # Цена без карты
        price_elem = soup.find('span', class_=lambda x: x and 'price' in x.lower() and 'discount' not in x.lower())
        price_without_card = float(price_elem.text.replace('₽', '').replace(' ', '').strip()) if price_elem else 0.0

        # Цена с картой (зелёная цена или скидка)
        discount_elem = soup.find('span', class_=lambda x: x and ('discount' in x.lower() or 'green' in x.lower()))
        price_with_card = float(
            discount_elem.text.replace('₽', '').replace(' ', '').strip()) if discount_elem else price_without_card

        # Рассчитываем скидку
        discount_percent = (
                    (price_without_card - price_with_card) / price_without_card * 100) if price_without_card > 0 else 0

        parse_time = time.time() - start_time

        return {
            "product_id": product_id,
            "title": title,
            "category": category,
            "price_without_card": price_without_card,
            "price_with_card": price_with_card,
            "discount_percent": round(discount_percent, 2),
            "currency": "RUB",
            "url": url,
            "parse_time_seconds": round(parse_time, 2)
        }
    except Exception as e:
        logger.error(f"Ошибка при парсинге {url}: {e}")
        return None


def save_excel(data: List[Dict], filename: str):
    """Экспорт результатов в Excel."""
    df = pd.DataFrame(data)
    with pd.ExcelWriter(f'{filename}.xlsx') as writer:
        df.to_excel(writer, index=False)
        for column in df:
            col_idx = df.columns.get_loc(column)
            writer.sheets['Sheet1'].set_column(col_idx, col_idx, 20)
    print(f'Файл {filename}.xlsx успешно сохранён')


async def main():
    # Список карточек для парсинга
    products = [
        {
            "url": "https://www.ozon.ru/product/konfety-shokoladnye-batonchiki-twix-minis-184-g-pechene-shokolad-karamel-137734033/",
            "product_id": "137734033"
        },
        {
            "url": "https://www.ozon.ru/product/konfety-shokoladnye-batonchiki-mars-minis-182-g-shokolad-nuga-karamel-1858087256/",
            "product_id": "1858087256"
        }
    ]

    results: List[Dict] = []

    async with async_playwright() as playwright:
        for product in products:
            logger.info(f"Парсинг {product['url']}")
            try:
                html = await fetch_page(product['url'], playwright)
                if html:
                    product_data = parse_product(html, product['url'], product['product_id'])
                    if product_data:
                        results.append(product_data)
                        print(f"Успешно спарсены данные для {product['url']}")
                    else:
                        logger.warning(f"Не удалось спарсить данные для {product['url']}")
                else:
                    logger.warning(f"Не удалось загрузить страницу {product['url']}")
            except Exception as e:
                logger.error(f"Не удалось обработать {product['url']}: {e}")
            await asyncio.sleep(6)  # Задержка для обхода защиты

    # Сохранение результатов
    if results:
        with open('ozon_products.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        save_excel(results, 'ozon_products')
        logger.info(f"Результат сохранён в ozon_products.json и ozon_products.xlsx")
        print(f"Парсинг завершён. Проверьте ozon_products.json и ozon_products.xlsx")
    else:
        logger.warning("Нет данных для сохранения")


if __name__ == "__main__":
    asyncio.run(main())
