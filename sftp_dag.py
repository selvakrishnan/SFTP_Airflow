import csv

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.dates import days_ago


def process_file(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    input_file = templates_dict.get("input_file")
    output_file = templates_dict.get("output_file")
    output_rows = []
    with open(input_file, newline='') as csv_file:
        for row in csv.reader(csv_file):
            row.append("processed")
            output_rows.append(row)
    with open(output_file, "w", newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(output_rows)


with DAG("sftp_dag",
         schedule_interval=None,
         start_date=days_ago(2)) as dag:

    wait_for_input_file = SFTPSensor(task_id="check-for-file",
                                     sftp_conn_id="my_sftp_server",
                                     path="/{{ ds }}/input.csv",
                                     poke_interval=10)

    download_file = SFTPOperator(
        task_id="get-file",
        ssh_conn_id="my_sftp_server",
        remote_filepath="/{{ ds }}/input.csv", #checks for folder in yyyy-MM-dd
        local_filepath="/tmp/{{ run_id }}/input.csv",
        operation="get",
        create_intermediate_dirs=True
    )

    process_file = PythonOperator(task_id="process-file",
                                  templates_dict={
                                      "input_file": "/tmp/{{ run_id }}/input.csv", #run id
                                      "output_file": "/tmp/{{ run_id }}/output.csv"
                                  },
                                  python_callable=process_file)

    upload_file = SFTPOperator(
        task_id="put-file",
        ssh_conn_id="my_sftp_server",
        remote_filepath="/{{ds}}/output.csv",
        local_filepath="/tmp/{{ run_id }}/output.csv",
        operation="put"
    )

    wait_for_input_file >> download_file >> process_file >> upload_file
