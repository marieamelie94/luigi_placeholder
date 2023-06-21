import luigi
import pandas as pd
from placeholder_data_load import GetPlaceholderData, CleanPlaceholderData, LoadPlaceholderData


def test_get_placeholder_data():
    task = GetPlaceholderData()

    assert luigi.build([task], local_scheduler=True)
    assert task.output().exists(), 'Output file does not exist'

    data = task.output().open("r").read()

    assert len(data) > 0, 'Output file is empty'


def test_clean_placeholder_data():
    task = CleanPlaceholderData()

    assert luigi.build([task], local_scheduler=True)
    assert task.output().exists(), 'Output file does not exist'

    df = pd.read_csv(task.output().path)

    assert df.columns.to_list() == ['user_id', 'id', 'title', 'body'], 'Output file columns != [user_id,id,title,body]'

    assert df['id'].count() > 0, 'Output file is empty'


def test_load_placeholder_data():
    task = LoadPlaceholderData()

    assert luigi.build([task], local_scheduler=True)


if __name__ == "__main__":
    test_get_placeholder_data()
    test_clean_placeholder_data()
    test_load_placeholder_data()


