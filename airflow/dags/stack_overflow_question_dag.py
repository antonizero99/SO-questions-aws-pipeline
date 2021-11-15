from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from airflow.operators import S3ToRedshiftOperator

default_args = {
    'owner': 'paul',
    'depends_on_past': False,
    'email': ['paulantonio.vn@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
        'stack_overflow_question_pipeline',
        default_args=default_args,
        description='Download, process Stack Overflow questions and push to S3 bucket and send to RedShift DW',
        schedule_interval='0 1 * * *',
        start_date=datetime(2021, 11, 11),
        catchup=False,
        tags=['capstone'],
) as dag:
    def read_config_file():
        """
        Read configuration file
        :return:
        RawConfigParser with config data inside
        """
        import configparser
        # Read configuration file
        config = configparser.RawConfigParser()
        config.read('../configs/param.cfg')
        # print(f'ACCESS_KEY_ID: {config["AWS_CREDENTIAL"]["AWS_ACCESS_KEY_ID"]}\n\
        # SECRET_ACCESS_KEY: {config["AWS_CREDENTIAL"]["AWS_SECRET_ACCESS_KEY"]}\n\
        # SESSION_TOKEN: {config["AWS_CREDENTIAL"]["AWS_SESSION_TOKEN"]}')
        return config

    def print_log(message):
        """
        Log into console datetime to execute along with message
        param:
            message: [String] Message to print in the console
        """
        print('{} - '.format(datetime.now()) + message)

    def check_data_integrity(config, data_name):
        """
        Check tables before and after transformation whether or not number of rows changes
        :param config: environment configuration
        :param data_name: name of data need to check
        :return:
        none
        Raise ValueError when there is data integrity error
        """
        import pandas as pd

        path_data = config['FILE_LOCATION'][f'loc_so_{data_name}']
        path_data_hash = config['FILE_LOCATION'][f'loc_{data_name}_with_hash']

        df = pd.read_csv(path_data, compression='zip')
        df_hash = pd.read_csv(path_data_hash)

        if len(df) != len(df_hash):
            raise ValueError(f'Table {data_name}_with_hash has integrity error')
        print(f'Data quality on table {data_name} check passed integrity')

    def check_duplicate_data_dim_table(config, table, column):
        """
        Check dim table whether there is unexpected duplication of data
        :param config: environment configuration
        :param table: name of table need to check
        :param column: name of column in table need to check
        :return:
        """
        import pandas as pd

        path_data = config['FILE_LOCATION'][f'loc_{table}']

        df = pd.read_csv(path_data)
        df_remove_duplicate = df[column].drop_duplicates()

        if(len(df) != len(df_remove_duplicate)):
            raise ValueError(f'Table {table} has unexpected data duplication at {column} column')
        print(f'Table {table} check duplicated data passed')

    def download_data(config, data_name):
        """
        Send 1 request to data sources and download data
        :return:
        none
        save downloaded file into cluster hard disk
        """
        import requests

        path_question = config['FILE_LOCATION'][f'loc_so_{data_name}']
        url_question = config['SOURCE_URL'][f'so_{data_name}_zip']
        req_question = requests.get(url_question, allow_redirects=True)
        print_log(f'{data_name}.zip downloaded')
        open(path_question, 'wb').write(req_question.content)
        print_log(f'{data_name}.zip saved to local')


    def add_hash_column(config, data_name):
        """
        This function will add a column hashing all data of each row,
        this column will be used to track changes in data source in FUTURE USE
        :return:
        none
        data passed in this function will be added hash column and save as CSV file
        """
        import pandas as pd

        path_data = config['FILE_LOCATION'][f'loc_so_{data_name}']
        path_output = config['FILE_LOCATION'][f'loc_{data_name}_with_hash']
        df = pd.read_csv(path_data, compression='zip', nrows=1000000)

        # Add hash_key column to track data changes in each row
        df['hash_key'] = df.apply(lambda row: pd.util.hash_pandas_object(
            pd.Series(row.to_string())), axis=1)
        print_log(f'hash_key for {data_name} table created')

        df.to_csv(path_output)
        print_log(f'{data_name} table with hash_key saved into local')

    def transform_question_data(config):
        """
        This function will create dimDate table by extracting from [questions] table
        :param config: environment configuration
        :return:
        none
        dimDate table created as CSV file
        factQuestion will be updated by adding 3 date columns, renaming 3 datetime columns
        """
        import pandas as pd
        import numpy as np

        path_question = config['FILE_LOCATION']['loc_question_with_hash']
        path_dim_date = config['FILE_LOCATION']['loc_dim_date']
        path_fact_question = config['FILE_LOCATION']['loc_fact_question']
        print_log(f'Done reading file location config')

        df_question = pd.read_csv(path_question)
        print_log(f'Done reading question_with_hash csv')

        # BEGIN: Create dim_date table
        # - Get all date columns in question table and exclude time in datetime data
        df_date_stg = df_question[['CreationDate', 'ClosedDate', 'DeletionDate']].apply(
            lambda date: pd.to_datetime(date).dt.date, axis=1)

        # - Combine 3 date columns into one,, remove null and duplicates
        sr_date = pd.concat([df_date_stg['CreationDate'], df_date_stg['ClosedDate'], df_date_stg['DeletionDate']],
                            axis=0,
                            ignore_index=True).dropna().drop_duplicates().sort_values()

        # - Create dim_date dataframe, add relative columns
        df_date = pd.DataFrame({'date': sr_date,
                                'day': sr_date.apply(lambda d: d.day),
                                'weekday': sr_date.apply(lambda d: d.weekday()),
                                'month': sr_date.apply(lambda d: d.month),
                                'quarter': sr_date.apply(lambda d: int(np.ceil(d.month / 3))),
                                'year': sr_date.apply(lambda d: d.year)}).reset_index(drop=True)
        df_date.to_csv(path_dim_date)
        print_log(f'dimDate table saved into local')
        # END: Create dim_date table

        # BEGIN: Transform fact_question table
        # - Rename time in datetime columns
        df_question.rename(columns={'CreationDate': 'CreationDateTime', 'ClosedDate': 'ClosedDateTime',
                                    'DeletionDate': 'DeletionDateTime'}, inplace=True)
        df_question = pd.concat([df_question, df_date_stg], axis=1)
        print_log(f'question table updated - adding 3 date columns, rename 3 datetime columns')

        # - Add status column in fact_question table
        df_question['status'] = df_question.apply(lambda row:
                                                  'Closed' if isinstance(row['ClosedDate'], str) else
                                                  'Deleted' if isinstance(row['DeletionDate'], str) else
                                                  'Open', axis=1)
        df_question.to_csv(path_fact_question)
        print_log(f'factQuestion table saved into local')
        # END: Transform fact_question table


    def transform_question_tag_data(config):
        """
        This function will create dimTag table which is extracted from [question tags] data
        :param config: environment configuration
        :return:
        none
        dimTag table created as csv file and factQuestionTag table updated by adding FK from dimTag table
        """
        import pandas as pd

        path_question_tag = config['FILE_LOCATION']['loc_question_tag_with_hash']
        path_fact_question_tag = config['FILE_LOCATION']['loc_fact_question_tag']
        path_dim_tag = config['FILE_LOCATION']['loc_dim_tag']
        print_log('file location config read successful')

        df_question_tag = pd.read_csv(path_question_tag)

        # START: Create dim_tag table
        # - Get the column Tag in question_tag table
        df_tag = df_question_tag['Tag'].drop_duplicates().reset_index(drop=True).reset_index()

        # - Rename column for business understanding
        df_tag.rename(columns={'index': 'TagID'}, inplace=True)
        print_log('dimTag table created')
        df_tag.to_csv(path_dim_tag)
        print_log('dimTag table saved into local')
        # END: Create dim_tag table

        # START: Transform fact_question_tag table
        df_question_tag = df_question_tag.merge(df_tag, on='Tag')
        df_question_tag.drop('Tag', axis=1, inplace=True)
        df_question_tag.rename({'ID': 'QuestionID'}, inplace=True)
        df_question_tag.to_csv(path_fact_question_tag)
        print_log('factQuestionTag table updated and saved into local')
        # END: Transform fact_question_tag table


    def push_data_to_s3(config):
        """
        This function will push processed CSV files into S3 bucket
        :param config: environment configuration
        :return:
        none, 6 csv files will be pushed into S3 bucket
        """
        import io
        import boto3
        import pandas as pd

        # Create AWS Credential Session
        session = boto3.session.Session(aws_access_key_id=config["AWS_CREDENTIAL"]["AWS_ACCESS_KEY_ID"],
                                        aws_secret_access_key=config["AWS_CREDENTIAL"]["AWS_SECRET_ACCESS_KEY"],
                                        aws_session_token=config["AWS_CREDENTIAL"]["AWS_SESSION_TOKEN"])
        print_log('authenticated session to S3 created')

        # Put objects into S3 datalake
        # Create resource and object
        s3 = session.resource('s3')
        obj_questions_raw = s3.Object(bucket_name='so-question-dl', key='raw/questions_hash.csv')
        obj_questions_tag_raw = s3.Object(bucket_name='so-question-dl', key='raw/question_tags_hash.csv')
        obj_questions = s3.Object(bucket_name='so-question-dl', key='factQuestion.csv')
        obj_question_tags = s3.Object(bucket_name='so-question-dl', key='factQuestionTag.csv')
        obj_date = s3.Object(bucket_name='so-question-dl', key='dimDate.csv')
        obj_tag = s3.Object(bucket_name='so-question-dl', key='dimTag.csv')
        print_log('objects in S3 located')

        # Get location of csv files which will be pushed into S3
        path_questions = config['FILE_LOCATION']['loc_question_with_hash']
        path_question_tags = config['FILE_LOCATION']['loc_question_tag_with_hash']
        path_dim_date = config['FILE_LOCATION']['loc_dim_date']
        path_dim_tag = config['FILE_LOCATION']['loc_dim_tag']
        path_fact_question = config['FILE_LOCATION']['loc_fact_question']
        path_fact_question_tag = config['FILE_LOCATION']['loc_fact_question_tag']
        print_log('processed csv files loaded from local')

        # Put [questions.csv] object into bucket
        obj_questions_raw.put(Body=open(path_questions, 'rb'))
        print_log(f'{path_questions} uploaded')

        # Put [question_tags.csv] object into bucket
        obj_questions_tag_raw.put(Body=open(path_question_tags, 'rb'))
        print_log(f'{path_question_tags} uploaded')

        # Put [dimDate.csv] object into bucket
        obj_date.put(Body=open(path_dim_date, 'rb'))
        print_log(f'{path_dim_date} uploaded')

        # Put [dimTag.csv] object into bucket
        obj_tag.put(Body=open(path_dim_tag, 'rb'))
        print_log(f'{path_dim_tag} uploaded')

        # Put [factQuestion.csv] object into bucket
        obj_questions.put(Body=open(path_fact_question, 'rb'))
        print_log(f'{path_fact_question} uploaded')

        # Put [factQuestionTag.csv] object into bucket
        obj_question_tags.put(Body=open(path_fact_question_tag, 'rb'))
        print_log(f'{path_fact_question_tag} uploaded')


    # BEGIN: TASK CONFIGURATION
    start_operator = DummyOperator(task_id='Begin_execution')

    download_question = PythonOperator(
        task_id="download_question_zip",
        python_callable=download_data,
        op_kwargs={'config': read_config_file(), 'data_name': 'question'}
    )

    download_question_tag = PythonOperator(
        task_id="download_question_tag_zip",
        python_callable=download_data,
        op_kwargs={'config': read_config_file(), 'data_name': 'question_tag'}
    )

    add_hash_column_question = PythonOperator(
        task_id="add_hash_column_question",
        python_callable=add_hash_column,
        op_kwargs={'config': read_config_file(), 'data_name': 'question'}
    )

    add_hash_column_question_tag = PythonOperator(
        task_id="add_hash_column_question_tag",
        python_callable=add_hash_column,
        op_kwargs={'config': read_config_file(), 'data_name': 'question_tag'}
    )

    check_integrity_question = PythonOperator(
        task_id="check_integrity_question",
        python_callable=check_data_integrity,
        op_kwargs={'config': read_config_file(), 'data_name': 'question'}
    )

    check_integrity_question_tag = PythonOperator(
        task_id="check_integrity_question_tag",
        python_callable=check_data_integrity,
        op_kwargs={'config': read_config_file(), 'data_name': 'question_tag'}
    )

    transform_question_data = PythonOperator(
        task_id="transform_question_data",
        python_callable=transform_question_data,
        op_kwargs={'config': read_config_file()}
    )

    transform_question_tag_data = PythonOperator(
        task_id="transform_question_tag_data",
        python_callable=transform_question_tag_data,
        op_kwargs={'config': read_config_file()}
    )

    check_dup_data_dim_date = PythonOperator(
        task_id="check_dup_data_dim_date",
        python_callable=check_duplicate_data_dim_table,
        op_kwargs={'config': read_config_file(), 'table': 'dim_date', 'column': 'date'}
    )

    check_dup_data_dim_tag = PythonOperator(
        task_id="check_dup_data_dim_tag",
        python_callable=check_duplicate_data_dim_table,
        op_kwargs={'config': read_config_file(), 'table': 'dim_tag', 'column': 'Tag'}
    )

    push_data_to_s3 = PythonOperator(
        task_id="push_data_to_s3",
        python_callable=push_data_to_s3,
        op_kwargs={'config': read_config_file()}
    )

    dim_date_to_redshift = S3ToRedshiftOperator(
        task_id='dim_date_to_redshift',
        redshift_conn_id='redshift_dw',
        table='dimDate',
        s3_path='s3://so-question-dl/dimDate.csv'
    )

    dim_tag_to_redshift = S3ToRedshiftOperator(
        task_id='dim_tag_to_redshift',
        redshift_conn_id='redshift_dw',
        table='dimTag',
        s3_path='s3://so-question-dl/dimTag.csv'
    )

    fact_question_to_redshift = S3ToRedshiftOperator(
        task_id='fact_question_to_redshift',
        redshift_conn_id='redshift_dw',
        table='factQuestion',
        s3_path='s3://so-question-dl/factQuestion.csv'
    )

    fact_question_tag_to_redshift = S3ToRedshiftOperator(
        task_id='fact_question_tag_to_redshift',
        redshift_conn_id='redshift_dw',
        table='factQuestionTag',
        s3_path='s3://so-question-dl/factQuestionTag.csv'
    )

    start_operator >> [download_question, download_question_tag]

    download_question >> add_hash_column_question
    add_hash_column_question >> check_integrity_question
    download_question_tag >> add_hash_column_question_tag
    add_hash_column_question_tag >> check_integrity_question_tag

    check_integrity_question >> transform_question_data
    transform_question_data >> check_dup_data_dim_date
    check_integrity_question_tag >> transform_question_tag_data
    transform_question_tag_data >> check_dup_data_dim_tag

    [check_dup_data_dim_date, check_dup_data_dim_tag] >> push_data_to_s3
    push_data_to_s3 >> [dim_date_to_redshift, dim_date_to_redshift, fact_question_to_redshift,
                        fact_question_tag_to_redshift]
    # END: TASK CONFIGURATION