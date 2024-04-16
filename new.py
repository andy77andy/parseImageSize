import logging
import time
from typing import Any

import aiohttp
import asyncio
import gspread
from PIL import Image
from io import BytesIO
from oauth2client.service_account import ServiceAccountCredentials

from ReadGoSheets import scopes  # Assuming this imports your scopes
from utiles import time_counter

creds = ServiceAccountCredentials.from_json_keyfile_name(('D:\\pythonProject\\test\\parseImagesSize\\secret.py'), scopes=scopes)
file = gspread.authorize(creds)
workbook = file.open("Parser_ImageSize1")
sheet = workbook.sheet1
urls = sheet.col_values(1)[:5000]
batch_size = 200


async def fetch_image_size(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status in (429, 443):
                    print("Rate limit exceeded. Waiting for 60 seconds.")
                    await asyncio.sleep(60)
                    return await fetch_image_size(url)  # Retry
                if response.status == 200:
                    image_data = await response.read()
                    image = Image.open(BytesIO(image_data))
                    width, height = image.size
                    return width, height
    except Exception as e:
        print(f"Error getting image size from URL {url}: {e}")
        return None, None


async def create_async_task(links: list) -> Any:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_image_size(url) for url in urls]
        results = await asyncio.gather(*tasks)
        print("Results:", results)
    return results


async def save_sizes_to_sheet(sizes, sheet):
    for row, (width, height) in enumerate(sizes, start=2):
        if width and height:
            size = f"{width}x{height}"
            print(f'{row}:{size}')
        else:
            size = f"Failed to get image size."
            print(size)
        try:
            sheet.update_cell(row, 2, size)
        except Exception as e:
            print(f"Error writing size to cell at row {row}: {e}")
            if "429" or "443" in str(e):
                print("Rate limit exceeded. Waiting for 20 seconds.")
                await asyncio.sleep(20)
                continue


# @time_counter
async def main():

    sizes = await create_async_task(urls)
    await save_sizes_to_sheet(sizes, sheet)


if __name__ == '__main__':
    start = time.perf_counter()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    duration = time.perf_counter() - start
    print(duration)


# async def fetch_urls(url, limit):
#     try:
#         async with aiohttp.ClientSession() as session:
#             async with session.get(url) as response:
#                 if response.status == 429:
#                     print("Rate limit exceeded. Waiting for 60 seconds.")
#                     await asyncio.sleep(60)
#                     return await fetch_image_size(url)  # Retry
#                 image_data = await response.read()
#                 image = Image.open(BytesIO(image_data))
#                 width, height = image.size
#                 return width, height
#     except Exception as e:
#         print(f"Error getting image size from URL {url}: {e}")
#         return None, None



#
# async def fetch_all_sizes(urls, batch_size=300):
#     all_sizes = []  # List to store all image sizes
#
#     for start_index in range(2, len(urls), batch_size):
#         end_index = min(start_index + batch_size, len(urls))
#         batch_urls = urls[start_index:end_index]
#
#         tasks = []
#         for url in batch_urls:
#             tasks.append(fetch_image_size(url))
#
#         results = await asyncio.gather(*tasks)
#
#         for index, (width, height) in enumerate(results):
#             if width and height:
#                 size = f"{width}x{height}"
#             else:
#                 size = f"Failed to get image size."
#             all_sizes.append({start_index + index: size})
#
#         if len(all_sizes) >= 5000:
#             break
#
#     return all_sizes
#
# async def write_sizes_to_sheet(sizes, sheet):
#     for item in sizes:
#         key, value = list(item.items())[0]
#         row = key
#         size = value
#         update_cell(sheet, row, 2, size)
#
# async def main():
#     total_urls = len(urls)  # Total number of URLs
#     sizes = await fetch_all_sizes(urls)
#     await write_sizes_to_sheet(sizes, sheet)
#
# if __name__ == '__main__':
#     start = time.perf_counter()
#     asyncio.run(main())
#     duration = time.perf_counter() - start
#     print(duration)