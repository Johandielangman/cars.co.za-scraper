# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~
#      /\_/\
#     ( o.o )
#      > ^ <
# Author: Johan Hanekom
# Date: January 2025
#
# ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~

# ========================= // STANDARD IMPORTS // =================================

import os
import json
from typing import (
    Dict,
    List
)

# ========================= // CUSTOM IMPORTS // =================================

import pandas as pd
from loguru import logger

# ========================= // CONSTANTS // =================================

RESULT_FOLDER_PATH: str = os.path.join(os.path.dirname(__file__), "runs")

# ========================= // FUNCTIONS // =================================


def flatten_car_data(car_data: Dict) -> Dict:
    car_attrs: Dict = car_data['car_attrs']
    car_specs: List[Dict] = car_data['car_specs']

    car_attrs['seller_types'] = ",".join(car_attrs['seller_types'])
    car_attrs.update({f"image_{k}": v for k, v in car_attrs.pop("image").items()})

    new_car_spec: Dict = {}
    for spec_dict in car_specs:
        spec_name: str = spec_dict['title'].replace(" ", "_").lower()
        for spec_item in spec_dict['attrs']:
            label: str = spec_name + "_" + spec_item['label'].replace(" ", "_").lower()
            new_car_spec[label] = spec_item['value']

    return {
        **car_attrs,
        **new_car_spec
    }


if __name__ == "__main__":
    logger.info("Starting parser")
    filename: str = input("What run do you want to parse?") or "Unknown"

    with open(os.path.join(RESULT_FOLDER_PATH, filename), "r") as f:
        data: Dict = json.load(f)

    logger.debug(f"Data loaded with {len(data)} cars")

    processed_data: List[Dict] = []

    for car_data in data:
        processed_data.append(flatten_car_data(car_data))

    df = pd.DataFrame(processed_data)
    df.to_csv(os.path.join(RESULT_FOLDER_PATH, "data.csv"), index=False)

    logger.info("Done 🚀")
