from pathlib import Path

#path for the log files of the DAG marketvol
base_log_folder='/home/jane/airflow/logs/dag_id=marketvol'

def log_anaylzer(filepath):
    log_file_path=Path(filepath).rglob('*.log')
    log_file_list=list(log_file_path)
    errors_cnt = 0
    elist = []
    warnings_cnt = 0
    wlist=[]
    for logfile in log_file_list:
        with open(logfile,'r') as file:
            r=file.readlines()
            for line in r:
                if 'ERROR' in line:
                    errors_cnt += 1
                    elist.append(line)
                if 'WARNING' in line:
                    warnings_cnt += 1
                    wlist.append(line)
    return errors_cnt,warnings_cnt, elist, wlist 

error_total,warning_total, errors, warnings=log_anaylzer(base_log_folder)
print(f'Total Number of Error Logs:{error_total}')
print(f'Total Number of Warning Logs:{warning_total}')
for line in errors:
    print(line)
for line in warnings:
    print(line) 


