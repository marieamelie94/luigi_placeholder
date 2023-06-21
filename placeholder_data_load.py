from datetime import datetime
import json
import logging
import luigi
import pandas as pd
import requests
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import tempfile


class GetPlaceholderData(luigi.Task):
    """
    Get fake posts from the JSONPlaceholder API
    """

    def output(self):
        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        return luigi.LocalTarget(f'raw_data/posts_{timestamp}.csv')

    def run(self):
        logger = logging.getLogger('luigi-interface')
        logger.info('Connecting to the API ...')
        try:
            response = requests.get('https://jsonplaceholder.typicode.com/posts')
        except requests.exceptions.RequestException as e:
            print(e)

        if response.status_code != 200:
             raise RuntimeError(f'Error grabbing data, status code: {response.status_code}')

        logger.info('Storing data ...')
        df = pd.DataFrame(response.json())
        df.to_csv(self.output().path, index=False)


class CleanPlaceholderData(luigi.Task):
    """
    Clean up the data
    """

    def requires(self):
        return GetPlaceholderData()

    def output(self):
        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        return luigi.LocalTarget(f'data/posts_{timestamp}.csv')

    def run(self):
        logger = logging.getLogger('luigi-interface')

        # with self.input().open('r') as input_file:
        df = pd.read_csv(self.input().path)
        logger.info(df.head())

        logger.info(f'Cleaning up ...')
        # Null ids
        df = df.dropna(subset=['id', 'userId'])
        # Duplicates, 2 identical rows or 2 identical id
        df.drop_duplicates()
        df = df.drop_duplicates(subset=['id'])
        # Make sure ids are integers
        df['id'] = df['id'].astype(int)
        df['userId'] = df['userId'].astype(int)
        # Rename lower case
        df = df.rename(columns={'userId': 'user_id'})

        logger.info('Storing data ...')
        df.to_csv(self.output().path, index=False)


class LoadPlaceholderData(luigi.Task):
    """
    Load the data to sqlite
    """

    def requires(self):
        return CleanPlaceholderData()

    def run(self):
        engine = create_engine('sqlite://', echo=False)
        logger = logging.getLogger('luigi-interface')

        df = pd.read_csv(self.input().path)
        logger.info(f'Loading to sqlite table placeholder_posts ...')
        df.to_sql('placeholder_posts', con=engine, if_exists='replace', index=False)

        try:
            with engine.connect() as conn:
                count = conn.execute(sa.text("SELECT count(*) as row_count FROM placeholder_posts")).fetchall()[0][0]
                logger.info(f'Loaded {count} rows to placeholder_posts table')

        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            logger.info(error)
