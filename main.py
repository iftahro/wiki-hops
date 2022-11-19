import asyncio
import math
import multiprocessing
import random
import time
from functools import partial
from json import JSONDecodeError
from multiprocessing import Process
from os import getpid

import aiohttp
import numpy as np
import requests
from aiohttp import ContentTypeError

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
    # finished, unfinished = loop.run_until_complete(asyncio.wait(tasks))
    # print(unfinished)
    # for i, t in enumerate(finished):
    #     lst[titles[i]] = t.result()
    # lst.update(dict(zip(titles, y)))
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
        lst = {}
        # manager = multiprocessing.Manager()
        # lst = manager.dict()
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
        # pool = multiprocessing.Pool(4)
        # jobs = []
        # for i in array_split:
        #     job = pool.apply_async(get_pages_links, args=(i, lst))
        #     jobs.append(job)
        # [job.wait(1000000) for job in jobs]
        # pool.close()
        # pool.join()
        # print(len(lst))
        # end = time.time()
        # print(f"\nTime is {(end - start) / 60}.")
        # print(len(lst))
        # for _ in array_split:
        #     p = multiprocessing.Process(target=get_pages_links, args=(_, invalid, lst))
        #     jobs.append(p)
        #     p.start()
        # for proc in jobs:
        #     proc.join()
        #
        # for x, y in lst.items():
        #     if dst_title in y:
        #         print(f"Found in {x} after {counter}")
        #         end = time.time()
        #         print(f"\nFinished in {(end - start) / 60}.")
        #         exit()
        #     else:
        #         invalid.append(x)
        #         titles.extend(y)

    # h = titles.__deepcopy__({})
    # random.shuffle(h)
    # for g in h:
    #     if g not in invalid:
    #         print(f"Getting links for {g}")
    #         links = get_all_page_links(g)
    #         if dst_title in links:
    #             print(f"Finished after {counter}. {g}")
    #             exit()
    #         else:
    #             invalid.append(g)
    #             for link in links:
    #                 if link not in invalid and link not in titles:
    #                     titles.append(link)


if __name__ == '__main__':
    main("Israel", "Banana")
# def main(source_title: str, dst_title: str, min, invalid: list, counter: int):
#     if counter >= min.value:
#         return
#     links = get_all_page_links(source_title)
#     if dst_title in links:
#         print(counter)
#         print(source_title)
#         min.value = counter
#         return
#     invalid.append(source_title)
#     random.shuffle(links)
#     while links:
#         link = links.pop()
#         if link not in invalid:
#             main(link, dst_title, min, invalid, counter + 1)


# def main(dst_title: str, titles: list, invalid: list, counter: int):
#     h = titles.__deepcopy__({})
#     random.shuffle(h)
#     for g in h:
#         if g not in invalid:
#             print(f"Getting links for {g}")
#             links = get_all_page_links(g)
#             if dst_title in links:
#                 print(f"Finished after {counter}. {g}")
#                 exit()
#             else:
#                 invalid.append(g)
#                 for link in links:
#                     if link not in invalid and link not in titles:
#                         titles.append(link)
#
#
# def runInParallel():
#     with multiprocessing.Manager() as manager:
#         titles = manager.list()
#         titles.append("Albert Einstein")
#         invalid = manager.list()
#         counter = 1
#         while True:
#             print(f"Trying {counter} hops.")
#             proc = []
#             for i in range(10):
#                 p = Process(target=main, args=("Banana", titles, invalid, counter))
#                 p.start()
#                 proc.append(p)
#             for p in proc:
#                 p.join()
#             counter += 1
#
#
# if __name__ == '__main__':
#     runInParallel()
