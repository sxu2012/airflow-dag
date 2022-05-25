import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
import yfinance as yf
import pandas as pd

default_args = {'start_date': datetime(2022, 5, 16), 
                'execution_timeout':timedelta(seconds=5)}

dag=DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='00 18 * * 1-5',
    catchup=True
)

dest_dir= '/home/jane/tmp/data/{{ds_nodash}}'
dest_sub_dir='/home/jane/tmp/data/{{ds_nodash}}/'
temp_sub_dir='mv /home/jane/{{ds}}'

#2.1 Create a BashOperator to initialize a temporary directory for data download
t0=BashOperator(
    task_id='create_dir',
    bash_command=f'mkdir -p {dest_dir}',
    dag=dag
)

#2.2 Create a PythonOperator to download the market data
def download_data(symbol,**context):
    start_date=str(context['ds'])
    end_date=str(context['next_ds'])
    tsla_df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    tsla_df.to_csv(f"{start_date}{symbol}_data.csv", header=True)

t1=PythonOperator(
    task_id='download_TSLA_Data',
    python_callable=download_data,
    op_kwargs={'symbol':'TSLA'},
    provide_context=True,
    dag=dag)

t2=PythonOperator(
    task_id='download_AAPL_Data',
    python_callable=download_data,
    op_kwargs={'symbol':'AAPL'},
    provide_context=True,
    dag=dag)

#2.3 create BashOperator to move the downloaded file to a data location
t3=BashOperator(
    task_id='move_TSLA_data',
    bash_command= f'{temp_sub_dir}TSLA_data.csv {dest_sub_dir}',
    dag=dag)

t4=BashOperator(
    task_id='move_AAPL_data',
    bash_command= f'{temp_sub_dir}AAPL_data.csv {dest_sub_dir}',
    dag=dag)

#2.4 create a PythonOperator to run a query on both data files
def query_data(**context):
    filepath='/home/jane/tmp/data/'
    day=str(context['ds'])
    day_nodash=str(context['ds_nodash'])
    tsla_df=pd.read_csv(filepath+day_nodash+'/'+day+'TSLA_data.csv')
    aapl_df=pd.read_csv(filepath+day_nodash+'/'+day+'AAPL_data.csv')
    t=tsla_df['Close'].describe()
    a=aapl_df['Close'].describe()
    print(f'Apple Closing Price Stats({day}):')
    print(a)
    print('\n\n')
    print(f'Tesla Closing Price Stats({day}):')
    print(t)

t5=PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    provide_context=True,
    dag=dag)

#t1 and t2 must run only after t0
t0>>[t1,t2]

#t3 must run after t1
t1>>t3

#t4 must run after t2
t2>>t4

#t5 must run after both t3 and t4 are complete
[t3,t4]>>t5
