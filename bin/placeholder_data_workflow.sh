# !/bin/bash
ETL_DIR=".."
cd $ETL_DIR && python3 -m luigi --module placeholder_data_load LoadPlaceholderData --local-scheduler
# cd $ETL_DIR && pipenv run python3 -m luigi --module placeholder_data_load LoadPlaceholderData --local-scheduler # I am using pipenv