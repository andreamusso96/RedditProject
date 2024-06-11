import pandas as pd
import zstandard
from typing import List


def read_and_decode(reader: zstandard.ZstdDecompressionReader, chunk_size: int, max_window_size: int, previous_chunk=None, bytes_read: int = 0) -> str:
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    # If we get a UnicodeDecodeError, it's likely because we have a character that's split between two chunks
    # The simplest way to handle this is to read another chunk and append it to the previous one
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        print(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name: str, reader_window_size: int = 2 ** 31, reader_chunk_size: int = 2 ** 27):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=reader_window_size).stream_reader(file_handle)
        # Loop through the entire file and read it in chunks
        while True:
            chunk = read_and_decode(reader=reader, chunk_size=reader_chunk_size, max_window_size=reader_window_size // 4)

            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]

        reader.close()


def write_lines_zst(writer: zstandard.ZstdCompressionWriter, lines: List[str]):
    for line in lines:
        writer.write(line.encode('utf-8'))
        writer.write("\n".encode('utf-8'))


if __name__ == '__main__':
    import json
    import logging
    import os

    logger = logging.getLogger('subreddit_extraction')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fp = '/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/comments/RS_2020-06.zst'
    file_size = os.stat(fp).st_size

    bad_line_count = 0
    file_lines = 0
    saved = 0

    vals = []
    for line, file_bytes_processed in read_lines_zst(file_name=fp):
        try:
            line_json = json.loads(line)
            vals.append({
                'author': line_json['author'],
                'subreddit': line_json['subreddit'],
                'score': line_json['score'],
                'created_utc': line_json['created_utc'],
                'title': line_json['title'],
                'id': line_json['id'],
                'num_comments': line_json['num_comments'],
                'upvote_ratio': line_json['upvote_ratio']
            })
        except (KeyError, json.JSONDecodeError) as err:
            bad_line_count += 1

        file_lines += 1
        # Log progress
        if file_lines % 100_000 == 0:
            mega_bytes = file_bytes_processed / 1_000_000
            logger.info(f'FILE LINES: {file_lines} -- PERCENTAGE: {(file_bytes_processed / file_size) * 100:.0f}% -- BAD LINES SHARE {bad_line_count/file_lines:.2f}, MEGABYTES: {mega_bytes:.2f}')

        if file_lines % 4_000_000 == 0:
            logger.info(f'SAVING FILE: {saved}')
            vals = pd.DataFrame(vals)
            vals.to_parquet(f'/Users/andrea/Desktop/PhD/Projects/Current/Reddit/data/parquet_test/file_{saved}.parquet.snappy', engine='pyarrow', compression='snappy')
            saved += 1
            del vals
            vals = []



