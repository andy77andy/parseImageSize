import logging
import random
from typing import Any

import aiohttp
import asyncio
import gspread
from PIL import Image
from io import BytesIO

from oauth2client.service_account import ServiceAccountCredentials

from parseImageSize.utils import time_counter

scopes = {
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
}

creds = ServiceAccountCredentials.from_json_keyfile_name('secret.py', scopes=scopes)
client = gspread.authorize(creds)
workbook1 = client.open("Parser_ImageSize1")
sheet = workbook1.sheet1
urls = sheet.col_values(1)[2:1002]
batch_size = 150


async def fetch_image_size(url: str) -> Any:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    """fetching image size"""
                    image_data = await response.read()
                    image = Image.open(BytesIO(image_data))
                    width, height = image.size
                    return f"{width} X {height}"
                elif response.status == 429:
                    """ If rate limit exceeded or server error, apply exponential backoff """
                    logging.error("Rate limit exceeded or server error. Retrying...")
                    await asyncio.sleep(2 ** 2 + random.random())
                    return await fetch_image_size(url)
    except Exception as e:
        logging.error(f"Error getting image size from URL {url}: {e}")
        return "Uncorrected url"


async def create_async_task(links: list) -> Any:
    async with aiohttp.ClientSession():
        tasks = [fetch_image_size(link) for link in links]
        results = await asyncio.gather(*tasks)
    return results


async def save_data_to_sheet(data: list) -> None:
    for row, size in enumerate(data, start=2):
        while True:
            """Keep retrying until successful"""
            try:
                sheet.update_cell(row, 3, size)
                break
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    print("Rate limit exceeded or server error. Retrying...")
                    await asyncio.sleep(3)
                else:
                    logging.error(f"Error occurred while updating cell: {e}")
                    break


@time_counter
def main():
    for cut in range(0, len(urls), batch_size):
        """perform fetching/adding data per cuts to improve performance regarding google limits"""
        data = asyncio.run(create_async_task(urls[cut: cut + batch_size]))
        asyncio.run(save_data_to_sheet(data))


if __name__ == '__main__':
    main()
