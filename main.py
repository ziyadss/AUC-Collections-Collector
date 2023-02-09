import os
import sys
import time
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Final, Callable, List, Tuple

import requests

CONNECTION_RETRIES: Final[int] = 5

THREAD_CHUNK: Final[int] = 10

FILE_CHUNK: Final[int] = 1024 * 1024 * 2

THREAD_COUNT: Final[int] = 2

COLLECTION_URL: Final[str] = \
    "https://digitalcollections.aucegypt.edu/digital/api/search/collection/{collection}/page/{pg}/maxRecords/{count}"

ITEM_URL: Final[str] = "https://digitalcollections.aucegypt.edu/digital/api/collection/{collection}/id/{id}/download"


def downloader(path: str) -> Callable[[Tuple[str, str]], bool]:
    def _downloader(name_url: Tuple[str, str]) -> bool:
        name, url = name_url
        print(f"Downloading file: {name}")
        for _ in range(CONNECTION_RETRIES):
            try:
                r: requests.Response = requests.get(url, stream=True)
                break
            except requests.exceptions.RequestException:
                time.sleep(5)
        else:
            print(f"Failed to get file: {name} at url {url}")
            return False

        if r.status_code == requests.codes.ok:
            with open(f"{path}/{name}", 'wb') as f:
                for chunk in r.iter_content(FILE_CHUNK):
                    f.write(chunk)
            print(f"Downloaded file: {name}")
            return True

        return False

    return _downloader


def save_urls(title: str, start: int, count: int, name_url: List[Tuple[str, str]]):
    filename: str = f"{title}/urls.txt"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'a', encoding="utf-8") as f:
        f.write(f"=====================================================================\n")
        f.write(f"Downloading files {start} to {count} of collection '{title}'\n")
        f.write(f"Count: {count - start + 1} --- Timestamp: {datetime.now()}\n")
        f.writelines(f"{name}\t\t{url}\n" for name, url in name_url)
        f.write("\n\n")


def collect(collection: str, start: int) -> None:
    collection_json = requests.get(COLLECTION_URL.format(collection=collection, pg=1, count=1)).json()
    title = next(filter(lambda c: c['selected'], collection_json['filters']['collections']))['name']
    count = collection_json['totalResults']

    items: List = requests.get(COLLECTION_URL.format(collection=collection, pg=1, count=count)).json()['items']
    items = items[start - 1:]

    name_url: List[Tuple[str, str]] = [
        (f"{start + idx} - {item['title']}.pdf", ITEM_URL.format(collection=collection, id=item['itemId']))
        for idx, item in enumerate(items)]

    save_urls(title, start, count, name_url)

    print(f"Downloading files {start} to {count} of collection '{title}' (count: {count - start + 1})")
    results = ThreadPool(THREAD_COUNT).imap_unordered(downloader(title), name_url, THREAD_CHUNK)
    connection_fail_count = len([r for r in results if r])

    if connection_fail_count:
        print(f"Failed to connect for {connection_fail_count} files", sys.stderr)


def argparse(argv: List[str]) -> Tuple[str, int]:
    help_switch = '-h' in argv or '--help' in argv
    if help_switch or len(argv) not in {2, 3}:
        output_stream = sys.stdout if help_switch else sys.stderr
        print(f'Usage: {os.path.basename(argv[0])} [-h] <collection> [<start index>]', file=output_stream)
        sys.exit(not help_switch)

    return argv[1], max(int(argv[2]), 1) if len(argv) == 3 else 1


if __name__ == '__main__':
    collection_id, start_idx = argparse(sys.argv)

    try:
        collect(collection_id, start_idx)
    except requests.exceptions.JSONDecodeError:
        print(f"Collection '{collection_id}' did not return results.", file=sys.stderr)
