import math
import time
import asyncio

import aiohttp
import numpy as np

from wiki_path import WikiPath

DEFAULT_URL = "https://en.wikipedia.org/w/api.php"
DEFAULT_PARAMS = {
    "action": "query",
    "format": "json",
    "prop": "links",
    "pllimit": "max"
}


async def get_paths_links(paths: list[WikiPath]):
    """
    Receive a list of paths, retrieves each path last title links and returns a dict of paths and links.
    """
    links = {}
    params = {**DEFAULT_PARAMS, "titles": "|".join([path.last_title for path in paths])}
    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=DEFAULT_URL, params=params) as response:
                data = await response.json()
        for _, page in data['query']['pages'].items():
            current_links = page.get("links")
            if current_links:
                # Finds the matching path object for the page links.
                path = next(filter(lambda path: path.last_title == page['title'], paths))
                if path not in links:
                    links[path] = []
                # Adds all link titles to the path value.
                links[path].extend([x['title'] for x in current_links])
        if "batchcomplete" in data:
            break
        params = {**params, "plcontinue": data["continue"]["plcontinue"]}
    return links


def main(src_title: str, dst_title: str):
    hops = 0
    paths = [WikiPath(src_title, dst_title)]
    checked_paths = []
    while True:
        hops += 1
        print(f"Searching...")
        # Splits the paths list to bulks of approximately 2000 paths.
        path_bulks = np.array_split(paths, math.ceil(len(paths) / 2000))
        paths = []
        for bulk in path_bulks:
            # Create and run an async task for every 50 paths to get their links.
            loop = asyncio.get_event_loop()
            tasks = [get_paths_links(url) for url in np.array_split(bulk, math.ceil(len(bulk) / 50))]
            result = loop.run_until_complete(asyncio.gather(*tasks))
            # Checks if the dst title is in one of the path links:
            for batch in result:
                for x, y in batch.items():
                    # If the dst title is found, prints the path and exists the function.
                    if dst_title in y:
                        print(f"\nFound the shortest path after {hops} hops!")
                        print(x)
                        return
                    # If the dst title wasn't found in the links, adds the path to the checked paths
                    # and all his child links to the paths list.
                    else:
                        checked_paths.append(x)
                        paths.extend([WikiPath.create_from_father(x, path) for path in y])
        # Make paths unique by last title and removes paths that their last title was already checked.
        paths = [path for path in set(paths) if path not in checked_paths]


if __name__ == '__main__':
    start = time.time()
    main("Cierno-Żabieniec", "Arik Einstein")
    end = time.time()
    print(f"\nFinished in {round(end - start, 2)} seconds.")
