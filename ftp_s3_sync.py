import os

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
import airflow.contrib.hooks.ftp_hook
from datetime import timedelta
import os
from toolz.curried import reduce, sorted, map, unique, last, filter
from toolz import pipe
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

from airflow.models import DAG
import arrow
from airflow.contrib.hooks.ftp_hook import FTPHook
import logging
import zipfile
from pathlib import Path

from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=50),
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

dag = DAG(
    dag_id="z3",
    schedule_interval='*/100000 * * * *',
    default_args=default_args
)

REMOTE_PATH = "/data/"
CONN_ID = 'sftp_synchronization'
S3_CONN_ID = "sor_s3"
LOCAL_TMP_DIR = f"/tmp/ftp_files_{datetime.now().microsecond}"
ftp_hook = FTPHook(ftp_conn_id=CONN_ID)
S3_BUCKET = "sap-dqmsdk"
try:
    os.mkdir(LOCAL_TMP_DIR)
except IOError:
    logging.error(f"Can't create {LOCAL_TMP_DIR}")


class FtpDownloadOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            name: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name

    @staticmethod
    def collect_prefix_dates(acc_dates, curr_name):
        try:
            date = arrow.get(curr_name, r"YYYY_MMM_[\w*].ZIP")
            acc_dates.append(date)
        except ValueError as e:
            logging.info(str(e))
            return acc_dates
        finally:
            return acc_dates
    @classmethod
    def select_files(cls, files):
        prefixes = pipe(reduce(cls.collect_prefix_dates, files, list()),
                        unique, sorted, map(lambda d: d.format("YYYY_MMM")), list)
        if len(prefixes) == 0:
            return files
        else:
            select_prefix = last(prefixes)
            skip_prefixes = prefixes[:-1]
            selected_files = pipe(files,
                                  filter(lambda file: not any([prefix in file for prefix in skip_prefixes])), list)
            return selected_files

    def execute(self, context):
        message = "Download files from FTP server".format(self.name)
        files = ftp_hook.list_directory(path=REMOTE_PATH)
        to_download_files = self.select_files(files)
        for f in to_download_files:
            logging.info("file: " + str(f))
            ftp_hook.retrieve_file(f"{REMOTE_PATH}/"
                                   f"{f}", f"{LOCAL_TMP_DIR}/{f}")
        return message


def unzip_files():
    for file in Path.glob(f"{LOCAL_TMP_DIR}/*.ZIP"):
        try:
            zipfile.ZipFile(file).extract(LOCAL_TMP_DIR)
            os.remove(file)
        except:
            IOError


download_files = FtpDownloadOperator(
    task_id="download_files",
    name="download_files",
    dag=dag,
    start_date=days_ago(8)
)

extract_files = PythonOperator(
    task_id='unzip_files',
    provide_context=True,
    python_callable=unzip_files,
    dag=dag
)
s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)


def upload_s3_callback():
    for file in Path.glob(f"{LOCAL_TMP_DIR}/*"):
        try:
            s3_hook.load_file(file, key=f"reference_data/{file}",
                              bucket_name=S3_BUCKET, replace=True,
                              encrypt=True)
        except:
            IOError


upload_s3 = PythonOperator(
    task_id='upload_s3',
    provide_context=True,
    python_callable=upload_s3_callback,
    dag=dag
)

download_files >> extract_files >> upload_s3
