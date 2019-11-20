import requests
import time
import concurrent.futures
import os
from configparser import ConfigParser
import sqlite3
import argparse
import yaml
import logging.config
import logging

# initialization
url_with_job_ids = []
list_metadata = []
t1 = time.perf_counter()


# setup the logger
def setup_logging(default_path, default_level, env_key):
    """ Setup logging configuration """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                logging.config.dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level, filename='logs.log',
                            format="%(asctime)s:%(name)s:%(levelname)s:%(message)s")
        print('Failed to load configuration file. Using default configs')


""" start the logging function """
path = "logging.yaml"
level = logging.INFO
env = 'LOG_CFG'
setup_logging(path, level, env)
logger = logging.getLogger(__name__)
logger.info("logger set..")


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
table = config['table']


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
    logger.info(data)
    list_metadata.append(data)


def store():
    # store to sqlite db
    conn = sqlite3.connect('testing.db')
    logger.info("Database Opened")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS {} 
                    ("id" INTEGER PRIMARY KEY AUTOINCREMENT, 
                    "job_id" TEXT, 
                    "app_name" TEXT, 
                    "state" TEXT, 
                    "date" TEXT);'''.format(table))
    c.executemany("""INSERT INTO {} (job_id, app_name, state, date)
                    VALUES (:job_id, :app_name, :state, :date_created);""".format(table), list_metadata)
    logger.info("Metadata added")
    conn.commit()
    conn.close()


def query_api():
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(thread)) as executor:
        executor.map(job_query, url_with_job_ids)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("text", nargs='?', help="File name of the text file with job ids", type=str)
    args = parser.parse_args()
    file_open(args.text)
    store()


if __name__ == "__main__":
    main()
    t2 = time.perf_counter()
    logger.info(f'Finished in {t2 - t1} seconds')