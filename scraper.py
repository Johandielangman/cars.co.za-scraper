# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~
#      /\_/\
#     ( o.o )
#      > ^ <
# Author: Johan Hanekom
# Date: January 2025
#
# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~

# ========================= // STANDARD IMPORTS // =================================

import time
import os
import json
import threading
import queue
from dataclasses import (
    dataclass,
    field
)

# ========================= // CUSTOM IMPORTS // =================================

import requests
from loguru import logger
from typing import (
    Dict,
    Generator,
    List
)

# ========================= // CONSTANTS // =================================

START_URL: str = (
    "https://api.cars.co.za/fw/public/v3/vehicle?"
    "page[offset]=0"
    "&page[limit]=20"
    "&include_featured=true"
    "&sort[date]=desc"
)
CAR_DATA_LINK: str = "https://api.cars.co.za/fw/public/v2/specs"

NUM_SEARCH_PAGE_WORKERS: int = 1
NUM_CAR_DATA_WORKERS: int = 25
BATCH_SIZE: int = 10_000
TIMEOUT_SECONDS: float = 30.0

RESULT_FOLDER_PATH: str = os.path.join(os.path.dirname(__file__), "runs")

# ========================= // QUEUES // =================================
# Queues - request
search_page_request_queue: queue.Queue = queue.Queue()
car_data_request_queue: queue.Queue = queue.Queue()

# Queues - result
car_data_result_queue: queue.Queue = queue.Queue()

# ========================= // DATA CLASSES // =================================


@dataclass
class SearchPageLinks:
    self: str = field(default="")
    first: str = field(default="")
    next: str = field(default="")
    prev: str = field(default="")
    last: str = field(default="")


@dataclass
class SearchPageResponse:
    links: SearchPageLinks
    data: List[Dict]
    current_page: int
    total_pages: int

    def get_car_data(self) -> Generator:
        for listing in self.data:
            car_attrs: Dict = listing['attributes']
            yield f"{CAR_DATA_LINK}/{car_attrs['code']}/{car_attrs['year']}", car_attrs


# ========================= // FUNCTIONS // =================================


def new_session() -> requests.Session:
    s: requests.Session = requests.Session()
    s.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0'
        ),
        'Origin': 'https://www.cars.co.za',
        'Referer': 'https://www.cars.co.za/'
    })
    return s


def new_search_page_response(search_page_response: Dict) -> SearchPageResponse:
    return SearchPageResponse(
        links=SearchPageLinks(**search_page_response['links']),
        data=search_page_response['data'],
        current_page=search_page_response['meta']['currentPage'],
        total_pages=search_page_response['meta']['totalPages']
    )


@logger.catch
def initial_data_fetch() -> Dict:
    logger.debug(f"Initial data request to: {START_URL}")
    s: requests.Session = new_session()
    result: requests.Response = s.get(START_URL)
    result.raise_for_status()
    return result.json()


def load_existing_data(filename: str, prefix: str) -> List[Dict]:
    path: str = os.path.join(RESULT_FOLDER_PATH, filename)
    if not os.path.exists(path):
        return []
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.error(f"{prefix} Invalid JSON in file, initializing empty list")
        return []


def save_batch_to_file(
    filename: str,
    batch: List[Dict],
    prefix: str
) -> bool:
    try:
        existing_data = load_existing_data(filename, prefix)
        existing_data.extend(batch)

        temp_filename: str = os.path.join(RESULT_FOLDER_PATH, f"_{filename}")
        final_filename: str = os.path.join(RESULT_FOLDER_PATH, filename)

        with open(temp_filename, 'w') as f:
            json.dump(existing_data, f, indent=4)
        logger.debug(f"{prefix} Batch of {len(batch)} items saved to {filename}")

        # Rename files
        if os.path.exists(final_filename):
            os.remove(final_filename)
        time.sleep(1)
        os.rename(temp_filename, final_filename)

        return True

    except Exception as e:
        logger.error(f"{prefix} Error saving batch: {str(e)}")
        return False


def process_batch(
    batch: List[Dict],
    filename: str,
    prefix: str
) -> None:
    if batch:
        if save_batch_to_file(filename, batch, prefix):
            batch.clear()


def start_workers() -> None:
    worker_id: int = 1
    logger.info("Starting Search Page Workers")
    for _ in range(NUM_SEARCH_PAGE_WORKERS):
        threading.Thread(
            target=search_page_worker,
            kwargs={"worker_id": worker_id},
            daemon=True
        ).start()
        worker_id += 1

    worker_id: int = 1

    logger.info("Starting Car Data Workers")
    for _ in range(NUM_CAR_DATA_WORKERS):
        threading.Thread(
            target=car_data_worker,
            kwargs={"worker_id": worker_id},
            daemon=True
        ).start()
        worker_id += 1

    logger.info("Starting Result Save Worker")
    threading.Thread(
        target=save_car_data_worker,
        kwargs={"worker_id": worker_id},
        daemon=True
    ).start()

    logger.info("All workers started")


def join_all() -> None:
    search_page_request_queue.join()
    car_data_request_queue.join()
    logger.info("All requests done. Waiting for Results to finish saving!")
    car_data_result_queue.join()


def get_valid_filename() -> str:
    filename: str = str(input("What do you want to call the filename?"))
    if len(filename) < 5:
        raise ValueError("Name too short!")

    if not filename.endswith('.json'):
        filename += '.json'

    return filename


def create_logger_prefix(
    name: str,
    worker_id: str
) -> str:
    return f"[{name:<20}][{worker_id:<2}]"

# ========================= // WORKERS // =================================


def search_page_worker(worker_id: int) -> None:
    _p: str = create_logger_prefix("SEARCH_PAGE_WORKER", worker_id)

    logger.info(f"{_p} Starting Worker {worker_id}")
    while True:
        link: str = search_page_request_queue.get()
        s: requests.Session = new_session()
        # logger.debug(f"{_p} Working on {link}")

        # ========// WORK //=========
        _i: str = ""
        try:
            result: requests.Response = s.get(link)
            result.raise_for_status()
            response: SearchPageResponse = new_search_page_response(result.json())
            for car_data_link, attrs in response.get_car_data():
                car_data_request_queue.put((car_data_link, attrs))
            search_page_request_queue.put(response.links.next)
            _i: str = f"[{response.current_page:<5}/{response.total_pages:<5}]"
        except Exception as e:
            logger.error(f"{_p} {e}")
        # ===========================

        logger.debug(f"{_p}{_i} Finished {link}")
        search_page_request_queue.task_done()


def car_data_worker(worker_id: int) -> None:
    _p: str = create_logger_prefix("CAR_DATA_WORKER", worker_id)
    logger.info(f"{_p} Starting Worker {worker_id}")
    while True:
        link, car_attrs = car_data_request_queue.get()
        s: requests.Session = new_session()
        # logger.debug(f"{_p} Working on {link}")

        # ========// WORK //=========
        try:
            result: requests.Response = s.get(link)
            result.raise_for_status()
            car_specs: List[Dict] = result.json()['data'][0]
            car_data_result_queue.put({
                "car_attrs": car_attrs,
                "car_specs": car_specs
            })
        except Exception as e:
            logger.error(f"{_p} {e}")
        # ===========================

        logger.debug(f"{_p} Finished {link}")
        car_data_request_queue.task_done()


def save_car_data_worker(worker_id: int) -> None:
    _p: str = create_logger_prefix("RESULTS", worker_id)
    logger.info(f"{_p} Starting Worker {worker_id}")

    # For batch processing
    batch: List[Dict] = []
    last_save_time: float = time.time()

    while True:
        try:
            data: Dict = car_data_result_queue.get(timeout=TIMEOUT_SECONDS)
            batch.append(data)

            if (
                len(batch) >= BATCH_SIZE or
                (time.time() - last_save_time) >= TIMEOUT_SECONDS
            ):
                process_batch(
                    batch=batch,
                    filename=FILENAME,
                    prefix=_p
                )
                last_save_time = time.time()

            car_data_result_queue.task_done()

        except queue.Empty:
            process_batch(
                batch=batch,
                filename=FILENAME,
                prefix=_p
            )
            last_save_time = time.time()

# ========================= // MAIN // =================================


if __name__ == "__main__":
    logger.info("Starting cars.co.za scraper")
    os.makedirs("runs", exist_ok=True)
    try:
        FILENAME = get_valid_filename()
    except ValueError as e:
        logger.error(str(e))
        exit(0)

    logger.add(
        f"{os.path.join(RESULT_FOLDER_PATH, FILENAME.replace('json', 'log'))}",
        level="INFO"
    )

    start_workers()

    initial_response: SearchPageResponse = new_search_page_response(initial_data_fetch())
    for car_data_link, car_attrs in initial_response.get_car_data():
        car_data_request_queue.put((car_data_link, car_attrs))
    search_page_request_queue.put(initial_response.links.next)
    join_all()
    logger.info("Done 🚀")
