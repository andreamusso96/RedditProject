from typing import Any, Dict, List, Tuple, Union

import logging
import os
from pydantic import BaseModel, ValidationError
import pyarrow as pa
import pyarrow.parquet as pq
import gc

from zst_io import read_lines_zst

logger = logging.getLogger('transform')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def zst_to_parquet(zst_file_path: str, parquet_folder_path: str, parquet_file_name: str):
    logger.info(f'Converting {zst_file_path} to parquet files in {parquet_folder_path}')
    file_size_zst_bytes = os.stat(zst_file_path).st_size

    # Figure out how many parquet files to create
    logger.info(f'Computing zst file lines')
    n_lines_zst_file = _count_lines_zst(file_path=zst_file_path)
    n_lines_parquet_file, n_parquet_files = _get_n_files_and_n_lines_parquet(lines_zst=n_lines_zst_file)
    logger.info(f'Number of parquet files to create: {n_parquet_files}, { n_lines_parquet_file} lines per file')

    bad_line_count = 0
    file_lines = 0
    parquet_file_id = 0

    lines = []
    for line, file_bytes_processed in read_lines_zst(file_name=zst_file_path):
        try:
            lines.append(_extract_line_data(line_str=line))
        except ValidationError as err:
            bad_line_count += 1

        file_lines += 1

        # Log progress
        if file_lines % 100_000 == 0:
            logger.info(f'FILE LINES: {file_lines} -- PERCENTAGE: {(file_bytes_processed / file_size_zst_bytes) * 100:.0f}% -- BAD LINES SHARE {bad_line_count / file_lines:.2f}, MEGABYTES: {file_bytes_processed / 1_000_000:.2f}')

        # Save the parquet file
        if file_lines % n_lines_parquet_file == 0 and parquet_file_id < n_parquet_files - 1:
            logger.info(f'SAVING FILE: {parquet_file_id}')
            _save_to_parquet_file(lines=lines, parquet_file_path=f'{parquet_folder_path}/{parquet_file_name}_{parquet_file_id}.parquet.snappy')
            parquet_file_id += 1

            del lines
            gc.collect()
            lines = []

    # Save the last parquet file
    logger.info(f'TOTAL FILE LINES: {file_lines}')
    logger.info(f'SAVING FILE: {parquet_file_id}')
    _save_to_parquet_file(lines=lines, parquet_file_path=f'{parquet_folder_path}/{parquet_file_name}_{parquet_file_id}.parquet.snappy')
    logger.info(f'Finished converting {zst_file_path} to parquet files in {parquet_folder_path}')


def _count_lines_zst(file_path: str) -> int:
    file_lines = 0
    for _, _ in read_lines_zst(file_name=file_path):
        file_lines += 1
    return file_lines


def _get_n_files_and_n_lines_parquet(lines_zst: int) -> Tuple[int, int]:
    bytes_per_line_parquet = 170
    desired_lines_parquet = (240 * 10**6) // bytes_per_line_parquet
    n_files = max(1, lines_zst // desired_lines_parquet)
    remainder = lines_zst % desired_lines_parquet

    if remainder / (desired_lines_parquet * n_files) > 0.5:
        n_files += 1
        n_lines_parquet = desired_lines_parquet
    else:
        n_lines_parquet = desired_lines_parquet + remainder // n_files
    return n_lines_parquet, n_files


class RedditSubmission(BaseModel):
    author: str | None
    subreddit: str
    score: int
    created_utc: int
    title: str
    id: str
    num_comments: int | None
    selftext: str | None
    media: Any | None


def _extract_line_data(line_str: str) -> Dict[str, Any]:
    reddit_submission = RedditSubmission.model_validate_json(line_str)
    line_parse = {
        'author': reddit_submission.author,
        'subreddit': reddit_submission.subreddit,
        'score': reddit_submission.score,
        'created_utc': reddit_submission.created_utc,
        'title': reddit_submission.title,
        'id': reddit_submission.id,
        'num_comments': reddit_submission.num_comments,
        'selftext': reddit_submission.selftext,
        'media': False if reddit_submission.media is None else True
    }
    return line_parse


def _save_to_parquet_file(lines: List[Dict[str, Union[int, str, bool, None]]], parquet_file_path: str):
    schema = pa.schema([
        ('author', pa.string()),
        ('subreddit', pa.string()),
        ('score', pa.int64()),
        ('created_utc', pa.int64()),
        ('title', pa.string()),
        ('id', pa.string()),
        ('num_comments', pa.int64()),
        ('selftext', pa.string()),
        ('media', pa.bool_())
    ])
    table = pa.Table.from_pylist(lines, schema=schema)
    pq.write_table(table, parquet_file_path)