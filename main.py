import math
import time
import asyncio
from os import getpid

import aiohttp
import numpy as np

DEFAULT_URL = "https://en.wikipedia.org/w/api.php"
DEFAULT_PARAMS = {
    "action": "query",
    "format": "json",
    "prop": "links",
    "pllimit": "max"
}

COUNTER = 0


async def get_all_page_links(titles: list):
    global COUNTER
    COUNTER += 1
    print(COUNTER)
    links = {}
    params = {**DEFAULT_PARAMS, "titles": "|".join(titles)}
    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=DEFAULT_URL, params=params) as response:
                data = await response.json()
        for k, v in data['query']['pages'].items():
            if k == "-1":
                # print(f"There is no page {v['title']}")
                pass
            else:
                current_links = v.get("links")
                if current_links:
                    if v['title'] not in links:
                        links[v['title']] = []
                    links[v['title']].extend([x['title'] for x in current_links])
        if "batchcomplete" in data:
            break
        params = {**params, "plcontinue": data["continue"]["plcontinue"]}
    return links


def get_pages_links(titles: list, lst):
    print(getpid())
    loop = asyncio.get_event_loop()
    tasks = [get_all_page_links(url) for url in np.array_split(titles, math.ceil(len(titles) / 50))]
    y = loop.run_until_complete(asyncio.gather(*tasks))
    for i in y:
        lst.update(i)


def main(src_title: str, dst_title: str):
    start = time.time()
    invalid = []
    counter = 0
    titles = [src_title]
    while True:
        counter += 1
        print(f"Trying with {counter} hops.")
        titles = list(filter(lambda x: x not in invalid, set(titles)))
        end = time.time()
        print(f"\nTime is {(end - start) / 60}.")
        array_split = np.array_split(titles, math.ceil(len(titles) / 2000))
        for koko in array_split:
            global COUNTER
            COUNTER = 0
            loop = asyncio.get_event_loop()
            tasks = [get_all_page_links(url) for url in np.array_split(koko, math.ceil(len(koko) / 50))]
            y = loop.run_until_complete(asyncio.gather(*tasks))
            for batch in y:
                for x, y in batch.items():
                    if dst_title in y:
                        print(f"Found in {x} after {counter}")
                        end = time.time()
                        print(f"\nFinished in {(end - start) / 60}.")
                        exit()
                    else:
                        invalid.append(x)
                        titles.extend(y)


if __name__ == '__main__':
    main("Israel", "Banana")
