import time
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import zstandard
from typing import List
import os
import json
import logging
from transformer_hdf5.zst_io import read_lines_zst, write_lines_zst

logger = logging.getLogger('subreddit_extraction')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

con = duckdb.connect("./data/comments2.db")


def extract_comments(subreddits: List[str], input_file_name: str, reader_window_size: int = 2 ** 31,
                     reader_chunk_size: int = 2 ** 27):
    lines_in_subreddits = []
    subreddits_ = set(subreddits)

    con.sql(
        "CREATE TABLE comments_10_12 (score INT, subreddit VARCHAR, author VARCHAR, created_utc INTEGER, body VARCHAR "
        ", id VARCHAR, parent_id VARCHAR )")

    bad_lines, file_lines, file_bytes_processed, subreddits_lines = 0, 0, 0, 0
    file_size = os.stat(input_file_name).st_size

    # Loop through every line in the file
    for line, file_bytes_processed in read_lines_zst(file_name=input_file_name, reader_window_size=reader_window_size,
                                                     reader_chunk_size=reader_chunk_size):
        try:
            line_json = json.loads(line)
            #do basic preselection of lines
            if (line_json['subreddit'] in subreddits_) and (len(line_json['body']) > 50) and (line_json['score'] > 2):
                desired_attributes = {'score', 'subreddit', 'author', 'created_utc', 'body', 'id',
                                      'parent_id'}  # Specify the attributes you want to keep
                filtered_line = {key: value for key, value in line_json.items() if key in desired_attributes}
                lines_in_subreddits.append(filtered_line)
                subreddits_lines += 1
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1
        file_lines += 1

        # Log progress
        if file_lines % 200_000 == 0:
            logger.info(
                f": {subreddits_lines:,} {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

        # Write the lines to the db file
        if len(lines_in_subreddits) > 200_000:  #for now only opening json file once is fastest..
            with open('./data/temp_10_12.json', 'w') as f:
                json.dump(lines_in_subreddits, f)
            con.execute("COPY comments_10_12 FROM  './data/temp_10_12.json' (FORMAT JSON, AUTO_DETECT true) ;")
            lines_in_subreddits = []

    #last bit of lines
    with open('./data/temp_10_12.json', 'w') as f:
        json.dump(lines_in_subreddits, f)
    con.execute("COPY comments_10_12 FROM  './data/temp_10_12.json' (FORMAT JSON, AUTO_DETECT true) ;")
    print('added lines', con.sql("SELECT COUNT(*) FROM comments_10_12"))

    logger.info(f"Complete : {file_lines:,} : {bad_lines:,} : {subreddits_lines:,}")


def extract_submissions(subreddits: List[str], input_file_name: str, reader_window_size: int = 2 ** 31,
                        reader_chunk_size: int = 2 ** 27):
    lines_in_subreddits = []
    subreddits_ = set(subreddits)

    bad_lines, file_lines, file_bytes_processed, subreddits_lines = 0, 0, 0, 0
    file_size = os.stat(input_file_name).st_size

    con.sql(
        "CREATE TABLE submission_13_05 (subreddit VARCHAR, score INT, author VARCHAR, "
        "created_utc INTEGER, title VARCHAR, id VARCHAR, num_comments INTEGER, downs INTEGER )")

    # Loop through every line in the file
    for line, file_bytes_processed in read_lines_zst(file_name=input_file_name, reader_window_size=reader_window_size,
                                                     reader_chunk_size=reader_chunk_size):
        try:
            line_json = json.loads(line)
            #do basic preselection of lines
            if (line_json['subreddit'] in subreddits_) and (len(line_json['title']) > 30) and (line_json['score'] > 20) \
                    and (line_json['num_comments'] > 10) and (line_json['media'] is None):
                desired_attributes = {'subreddit', 'score', 'author', 'created_utc', 'title', 'id', 'num_comments',
                                      'downs'}  # Specify the attributes you want to keep
                filtered_line = {key: value for key, value in line_json.items() if key in desired_attributes}
                lines_in_subreddits.append(filtered_line)
                subreddits_lines += 1
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1
        file_lines += 1

        # Log progress
        if file_lines % 200_000 == 0:
            logger.info(
                f": {subreddits_lines:,} {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

        # Write the lines to the db file
        if len(lines_in_subreddits) > 200_000:  #for now only opening json file once is fastest..
            with open('./data/temp_10_12.json', 'w') as f:
                json.dump(lines_in_subreddits, f)
            con.execute("COPY submission_13_05 FROM  './data/temp_10_12.json' (FORMAT JSON, AUTO_DETECT true) ;")
            lines_in_subreddits = []

    #last bit of lines
    with open('./data/temp_10_12.json', 'w') as f:
        json.dump(lines_in_subreddits, f)
    con.execute("COPY submission_13_05 FROM  './data/temp_10_12.json' (FORMAT JSON, AUTO_DETECT true) ;")
    print('added lines', con.sql("SELECT COUNT(*) FROM submission_13_05"))

    logger.info(f"Complete : {file_lines:,} : {bad_lines:,} : {subreddits_lines:,}")


def get_comments(collection_name: str):
    df = con.sql("{}{}".format("SELECT id, body AS title, subreddit FROM ", collection_name)).fetchdf()
    print(df)
    return df


def get_submissions(collection_name: str):
    df = con.sql("{}{}".format("SELECT id, title, subreddit FROM ", collection_name)).fetchdf()
    print(df)
    return df


def read_data_file(input_file_name: str) -> pd.DataFrame:
    # Create a list to store the interesting lines
    interesting_lines = []

    # Mostly for logging purposes
    bad_lines, file_lines, file_bytes_processed = 0, 0, 0
    file_size = os.stat(input_file_name).st_size

    # Loop through every line in the file
    for line, file_bytes_processed in read_lines_zst(file_name=input_file_name, reader_window_size=reader_window_size,
                                                     reader_chunk_size=reader_chunk_size):
        try:
            # Load the line as JSON and check if the subreddit is in the list
            line_json = json.loads(line)
            # Do whatever you want with the line (here I print the title of the submission with some other metadata if it in the nra subreddit and then add it to the interesting_lines list)
            interesting_lines.append(line_json)

        # If there are some errors we just skip the line and add it to the bad_lines count
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1

        file_lines += 1

        # I create a pandas dataframe from the interesting lines list
    df_lines = pd.DataFrame(interesting_lines)
    print(df_lines.columns)
    return df_lines


def load_submissions_old():
    #input_file = './data/RS_2020-06_filtered.zst'
    input_file = './data/RC_2007-03.zst'
    lines = read_data_file(input_file)
    # we have to drop a few attributes for which sql is not able to determine a Type
    clean = lines.drop(['author_flair_background_color', 'link_flair_background_color', 'author_flair_css_class',
                        'author_flair_template_id', 'author_flair_text', 'gildings', 'all_awardings', 'category',
                        'content_categories', 'discussion_type', 'distinguished', 'removed_by_category',
                        'suggested_sort', 'thumbnail_height', 'thumbnail_width', 'top_awarded_type',
                        'media_embed', 'secure_media', 'secure_media_embed',
                        'treatment_tags', 'url_overridden_by_dest', 'post_hint', 'preview', 'poll_data',
                        'crosspost_parent', 'crosspost_parent_list', 'author_cakeday',
                        'media_metadata', 'event_end', 'event_is_live', 'event_start', 'collections'], axis=1
                       )
    print(clean.columns)
    con.sql("DROP TABLE IF EXISTS reddit_data")
    con.execute("SET GLOBAL pandas_analyze_sample=100000")
    con.sql(" CREATE TABLE reddit_data AS SELECT * FROM clean")


def get_good_ids():
    df = con.sql("SELECT id, title "
                 "FROM reddit_data "
                 "WHERE num_comments>10 "
                 "AND (subreddit_subscribers/score) < 100000000 "
                 "AND LEN(title)>30 "
                 "AND media IS NULL ;"
                 ).fetchdf()
    print(df)

    return df


def user_subreddit_relation():
    print("number of authors that posted in more than 1 chosen subreddit")
    print(con.sql("SELECT count( DISTINCT R1.author_fullname) "
                  "FROM reddit_data R1, reddit_data R2 "
                  "Where R1.author_fullname != '[deleted]' "
                  "AND R1.author_fullname = R2.author_fullname "
                  "AND R2.subreddit != R1.subreddit "))

    print("number of authors making separate submissions in both subreddits:")
    df = con.sql("SELECT R1.subreddit AS S1, R2.subreddit AS S2, Count( DISTINCT R1.author_fullname) AS Authors "
                 "FROM reddit_data R1, reddit_data R2 "
                 #"JOIN reddit_data R2 ON R1.author_fullname = R2.author_fullname "
                 "Where R1.author_fullname != '[deleted]' "
                 "AND R1.author_fullname = R2.author_fullname "
                 "AND R1.id != R2.id "  #includes count authors that posted in the same subreddit twice
                 "GROUP BY S1, S2 "
                 #"HAVING S1 < S2 "  #also requires setting this to <=, else <
                 "HAVING S1 != S2 "
                 "ORDER BY Authors DESC ").fetchdf()
    print(df)

    pivot_df = df.pivot(index='S1', columns='S2', values='Authors')
    pivot_df.fillna(0, inplace=True)
    print(pivot_df)

    fig = px.imshow(pivot_df,
                    labels=dict(x="subreddit", y="subreddit", color="Authors in both"),
                    x=pivot_df.columns, y=pivot_df.index,
                    )
    fig.update_xaxes(side="top")
    fig.show()


def subreddit_scores():
    df = con.sql("WITH Scores AS ( "
                 "SELECT reddit_data.subreddit, COUNT() AS large_score FROM reddit_data WHERE score > 100 GROUP BY subreddit),"
                 "Comments AS ( "
                 "SELECT reddit_data.subreddit, COUNT(*) AS many_comments FROM reddit_data WHERE num_comments > 10 GROUP BY subreddit),"
                 "Topic AS ("
                 "SELECT reddit_data.subreddit, COUNT(*) AS BLM_submission FROM reddit_data "
                 "WHERE reddit_data.title LIKE '%BLM%' "
                 "OR reddit_data.title LIKE '%blm%' "
                 "OR reddit_data.title LIKE '%racism%' "
                 "GROUP BY reddit_data.subreddit)"
                 "SELECT RD.subreddit, Count(*) AS total_submissions, SUM(RD.num_comments) AS total_comments, "
                 "ANY_VALUE(Scores.large_score) AS large_upvote_submissions, "
                 "ANY_VALUE(Comments.many_comments) AS many_comment_submissions, "
                 "ANY_VALUE(Topic.BLM_submission) AS topic_submissions, "
                 "AVG(RD.upvote_ratio) AS avg_upvote_ratio "
                 "FROM reddit_data AS RD "
                 "LEFT JOIN Scores ON RD.subreddit = Scores.subreddit "
                 "LEFT JOIN Comments ON RD.subreddit = Comments.subreddit "
                 "LEFT JOIN Topic ON RD.subreddit = Topic.subreddit "
                 "GROUP BY RD.subreddit").fetchdf()
    print(df.head())
    df.fillna(0, inplace=True)
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns),
                    fill_color='lightblue',
                    align='left'),
        cells=dict(values=df.transpose().values,
                   fill_color='white',
                   align='left'))
    ])
    fig.show()


if __name__ == '__main__':
    con.sql("DROP TABLE IF EXISTS submission_13_05")
    base_path = './data'
    input_file_path = f'{base_path}/RS_2013-05.zst'
    subr = ['Republican',
            'democrats',
            'healthcare',
            'Feminism',
            'nra',
            'education',
            'climatechange',
            'politics',
            'progressive',
            'The_Donald',
            'TrueChristian',
            'Trucks',
            'teenagers',
            'AskMenOver30',
            'backpacking',
            'news',
            'BlackLivesMatter',
            'racism',
            'news',
            'usa',
            'DefundPoliceNYC']
    start = time.time()
    #extract_comments(subreddits=subr, input_file_name=input_file_path)
    extract_submissions(subreddits=subr, input_file_name=input_file_path)
    print(f'Time: {time.time() - start}')

    #read_data_file(input_file_path)
    #get_comments('comments_10_12')
    #read_data_file(input_file)
    #load_submissions_old()
    #get_good_ids()
    #user_subreddit_relation()
    #subreddit_scores()
