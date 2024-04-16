import time

import aiohttp
import asyncio

import gspread
from PIL import Image
from io import BytesIO

from oauth2client.service_account import ServiceAccountCredentials

from ReadGoSheets import scopes


# async def fetch_image_size(url):
#     try:
#         # headers = {
#         #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
#
#         async with aiohttp.ClientSession() as session:
#             # async with session.get(url, headers=headers) as response:
#             async with session.get(url) as response:
#                 image_data = await response.read()
#                 image = Image.open(BytesIO(image_data))
#                 width, height = image.size
#                 return width, height
#     except Exception as e:
#         print(f"Error getting image size from URL {url}: {e}")
#
#         return None, None

#
def update_cell(sheet, row, col, value):
    return sheet.update_cell(row, col, value)


creds = ServiceAccountCredentials.from_json_keyfile_name(('D:\\pythonProject\\test\\parseImagesSize\\secret.py'), scopes=scopes)
file = gspread.authorize(creds)
workbook = file.open("Parser_ImageSize1")
sheet = workbook.sheet1
urls = sheet.col_values(1)
batch_size = 200


# async def main(batch_size=300):
#     total_urls = len(urls)  # Total number of URLs
#     all_sizes = []  # List to store all image sizes
#
#     # while len(all_sizes) != 5000:
#     for start_index in range(2, total_urls, batch_size):
#         end_index = min(start_index + batch_size, total_urls)
#         batch_urls = urls[start_index:end_index]
#
#         tasks = []
#         for url in batch_urls:
#             tasks.append(fetch_image_size(url))
#
#         results = await asyncio.gather(*tasks)
#
#         for width, height in results:
#             if width and height:
#                 size = f"{width}x{height}"
#             else:
#                 size = "Failed to get image size."
#             all_sizes.append({start_index: size})
#             print(size)
#             print(len(all_sizes))
#         if len(all_sizes) >= 4999:
#             break
#     print(all_sizes)


    #     await asyncio.sleep(5)  # Adjust the sleep interval as needed
    #
    # # Update Google Sheet with all sizes
    # for i, size in enumerate(all_sizes, start=1):
    #     await asyncio.to_thread(update_cell, sheet, i, 2, size)
#
# if __name__ == '__main__':
#     start = time.perf_counter()
#     asyncio.run(main())
#     duration = time.perf_counter() - start
#     print(duration)
# Example usage:
# async def main(batch_size=50):
#     start_index = 1  # Starting index for the loop
#     end_index = start_index + batch_size  # Calculate the end index for the first batch
#     total_urls = len(urls)  # Total number of URLs
#
#     while start_index < total_urls:
#         # Iterate over the URLs within the current batch
#         for i in range(start_index, min(end_index, total_urls)):
#             all_images = []
#             url = urls[i]
#             width, height = await fetch_image_size(url)
#             if width and height:
#                 size = f"{width}x{height}"
#                 all_images.append(size)
#                 # await asyncio.to_thread(update_cell, sheet, i + 1, 2, size)
#             else:
#                 all_images.append("Failed to get image size.")
#                 # print("Failed to get image size.")
#
#         # Update start and end indices for the next batch
#         start_index = end_index
#         end_index = min(start_index + batch_size, total_urls)
#         await asyncio.sleep(20)
#
# # asyncio.run(main())
#

async def fetch_image_size(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 429:
                    print("Rate limit exceeded. Waiting for 60 seconds.")
                    await asyncio.sleep(60)
                    return await fetch_image_size(url)  # Retry
                image_data = await response.read()
                image = Image.open(BytesIO(image_data))
                width, height = image.size
                return width, height
    except Exception as e:
        print(f"Error getting image size from URL {url}: {e}")
        return None, None


async def fetch_all_sizes(urls, batch_size=300):
    all_sizes = []  # List to store all image sizes

    for start_index in range(2, len(urls), batch_size):
        end_index = min(start_index + batch_size, len(urls))
        batch_urls = urls[start_index:end_index]

        tasks = []
        for url in batch_urls:
            tasks.append(fetch_image_size(url))

        results = await asyncio.gather(*tasks)

        for index, (width, height) in enumerate(results):
            if width and height:
                size = f"{width}x{height}"
            else:
                size = f"Failed to get image size."
            all_sizes.append({start_index + index: size})

        if len(all_sizes) >= 5000:
            break
    return all_sizes


async def write_sizes_to_sheet(sizes, sheet):
    for item in sizes:
        key, value = list(item.items())[0]
        row = key
        size = value
        try:
            update_cell(sheet, row, 2, size)
        except Exception as e:
            print(f"Error writing size to cell at row {row}: {e}")
            if "429" in str(e):
                print("Rate limit exceeded. Waiting for 20 seconds.")
                await asyncio.sleep(20)
                continue

async def main():
    total_urls = len(urls)  # Total number of URLs
    sizes = await fetch_all_sizes(urls)
    await write_sizes_to_sheet(sizes, sheet)

if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(main())
    duration = time.perf_counter() - start
    print(duration)