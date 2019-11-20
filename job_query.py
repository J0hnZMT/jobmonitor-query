import requests
import time
import concurrent.futures
import os
import configparser
from configparser import ConfigParser
import sqlite3
import argparse

# initialization
url_with_job_ids = []
t1 = time.perf_counter()


# config files
def config_open(filename, section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


config = config_open('config.ini', 'parameter')
api_url = config['url']
thread = config['num_threads']


def file_open(filename):
    with open(filename, "r") as file:
        for line in file:
            url_with_job_ids.append("{}{}".format(api_url, line.rstrip('\r\n')))
    query_api()


def job_query(url):
    job_data = requests.get(url)
    result = job_data.json()
    json_parse(result)


def json_parse(json_result):
    result = json_result
    metadata = result['data']
    data = metadata[0]
    store(data)


def store(result):
    db_data = (result['job_id'], result['app_name'], result['state'], result['date_created'])
    print(db_data)
    # store to sqlite db
    conn = sqlite3.connect('testing.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE result 
                    ("id" INTEGER PRIMARY KEY AUTOINCREMENT, 
                    "job_id" TEXT, 
                    "app_name" TEXT, 
                    "state" TEXT, 
                    "date" TEXT);''')
    c.execute("INSERT INTO result VALUES (?, ?, ?, ?);", (result['job_id'], result['app_name'], result['state'], result['date_created']))
    conn.commit()


def query_api():
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(thread)) as executor:
        executor.map(job_query, url_with_job_ids)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("text", nargs='?', help="File name of the text file with job ids", type=str)
    args = parser.parse_args()
    file_open(args.text)


if __name__ == "__main__":
    main()
    t2 = time.perf_counter()
    print(f'Finished in {t2 - t1} seconds')