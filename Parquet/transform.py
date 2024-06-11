from typing import Any, Dict, List

import json
import logging
import os
import pandas as pd

from zst_io import read_lines_zst

logger = logging.getLogger('transform')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def zst_to_parquet(zst_file_path: str, parquet_folder_path: str, parquet_file_name: str):

    file_size = os.stat(zst_file_path).st_size

    bad_line_count = 0
    file_lines = 0
    parquet_file_id = 0

    lines = []
    for line, file_bytes_processed in read_lines_zst(file_name=zst_file_path):
        try:
            lines.append(_extract_line_data(line_str=line))
        except (KeyError, json.JSONDecodeError) as err:
            bad_line_count += 1

        file_lines += 1

        # Log progress
        if file_lines % 100_000 == 0:
            logger.info(f'FILE LINES: {file_lines} -- PERCENTAGE: {(file_bytes_processed / file_size) * 100:.0f}% -- BAD LINES SHARE {bad_line_count / file_lines:.2f}, MEGABYTES: {file_bytes_processed / 1_000_000:.2f}')

        # Save the parquet file
        if file_lines % 4_000_000 == 0:
            logger.info(f'SAVING FILE: {parquet_file_id}')
            _save_to_parquet_file(lines=lines, parquet_file_path=f'{parquet_folder_path}/{parquet_file_name}_{parquet_file_id}.parquet.snappy')
            parquet_file_id += 1
            del lines
            lines = []


def _extract_line_data(line_str: str) -> Dict[str, Any]:
    line_json = json.loads(line_str)
    line = {
        'author': line_json['author'],
        'subreddit': line_json['subreddit'],
        'score': line_json['score'],
        'created_utc': line_json['created_utc'],
        'title': line_json['title'],
        'id': line_json['id'],
        'num_comments': line_json['num_comments'],
        'upvote_ratio': line_json['upvote_ratio']
    }
    return line


def _save_to_parquet_file(lines: List[Dict[str, Any]], parquet_file_path: str):
    pd.DataFrame(lines).to_parquet(parquet_file_path, engine='pyarrow', compression='snappy')