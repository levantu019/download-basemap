from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import math
import os
import sys
import numpy as np
from urllib.request import urlopen
import time

BASE_URL = 'https://api.maptiler.com/tiles/satellite-v2/'
BASE_URL_2 = 'https://khms0.googleapis.com/kh?v=931&hl=en-US&x=104338&y=57945&z=17'
KEY = 'gkBXqlWRZj1cthPIxJwI'
IMAGE_TYPE = 'jpg'
DEFAULT_EXTENT = [21.7196447229999876, 107.7430949790000909, 104.8154006840000534, 20.1230816180000716]
# url_ex: https://api.maptiler.com/tiles/satellite-v2/6/50/28.jpg?key=gkBXqlWRZj1cthPIxJwI


# return (x_tile, y_tile) from coordinate
def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return (xtile, ytile)


# 
def create_url(z, x, y, type):
    url = BASE_URL + str(z) + '/' + str(x) + '/' + str(y) + '.' + type + '?key=' + KEY

    return url


# extent = [N, E, W, S]
def create_urls(extent, zoom, base_folder):
    urls = []
    N, E, W, S = extent

    bottom_left = deg2num(S, W, zoom)
    top_right = deg2num(N, E, zoom)
    for x_tile in range(bottom_left[0], top_right[0] + 1):
        for y_tile in range(top_right[1], bottom_left[1] + 1):
            directory = os.path.join(base_folder, str(zoom), str(x_tile))
            os.makedirs(directory, exist_ok=True)
            filename = os.path.join(directory, str(y_tile)) + '.' + IMAGE_TYPE
            urls.append({'url': create_url(zoom, x_tile, y_tile, IMAGE_TYPE), 'filename': filename})

    return urls     


# 
def download_image(url, file):
    with urlopen(url) as f:
        if f.status == 429:
            retry_after = int(f.headers["Retry-After"])
            print("Was blocking, time: %d", retry_after)
            sys.exit()
        image_data = f.read()

    if not image_data:
        raise Exception(f"Error: could not download the image from {url}")

    with open(file, 'wb') as image_file:
        image_file.write(image_data)
        print(f'{file} was downloaded...')   


# 
def get_urls_downloaded(zoom, base_folder):
    urls = []

    directory_zoom = os.path.join(base_folder, str(zoom))
    x_tiles = os.listdir(directory_zoom)
    for x_tile in x_tiles:
        directory_x = os.path.join(directory_zoom, x_tile)
        y_tiles = os.listdir(directory_x)
        for y_tile in y_tiles:
            y_tile_2 = y_tile.split('.')
            urls.append({'url': create_url(zoom, x_tile, y_tile_2[0], y_tile_2[1]), 'filename': os.path.join(directory_x, y_tile)})

    return urls


def consume(threads):
    done, _ = wait(threads, return_when=FIRST_COMPLETED)
    for t in done:
        del threads[t]
    print("Start sleep in 10s")
    time.sleep(10)


#  
def main(extent, min_zoom, max_zoom, base_folder):
    with ThreadPoolExecutor() as executor:
        for zoom in range(min_zoom, max_zoom + 1):
            images = create_urls(extent, zoom, base_folder)
            for img in images:
                executor.submit(download_image, img['url'], img['filename'])


# 
if __name__ == '__main__':
    min_zoom = 16
    max_zoom = 16
    extent = DEFAULT_EXTENT
    base_folder = 'xyz'
    # main(extent, min_zoom, max_zoom, base_folder)
    # print(len(create_urls(extent, 16, base_folder)))

    urls_downloaded = np.array(get_urls_downloaded(16, base_folder))
    urls_all = np.array(create_urls(extent, 16, base_folder))

    urls_pos = np.logical_not(np.isin(urls_all, urls_downloaded))
    urls = urls_all[urls_pos]
    # urls = urls[:100]
    print(len(urls))
    
    count = 0
    threads = {}

    with ThreadPoolExecutor(100) as executor:
        # for url in urls:
        #     executor.submit(download_image, url['url'], url['filename'])  

        for idx, url in enumerate(urls): 
            count += 1
            threads[executor.submit(download_image, url['url'], url['filename'])] = idx

            if count == 500:
                consume(threads)
                count = 0
                threads = {}

    consume(threads, 0)


