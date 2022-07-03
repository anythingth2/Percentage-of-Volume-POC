from typing import Dict
import datetime
import json
from google.cloud import bigquery

from percentage_of_volume import PoV

bq = bigquery.Client()

class PoVJobExecutor:
    def __init__(self):
        pass

    def query_single_job(self) -> Dict:
        query = '''
            SELECT id,
                    algorithm,
                    param, 
                    job_execution_start, 
                    status,
                    load_dt
            FROM `trading_terminal_poc.job_inventory`
            WHERE algorithm = "PoV" 
                   AND `status` = "CREATED"
            LIMIT 1
        '''
        job = bq.query(query).to_dataframe().iloc[0].to_dict()
        return job
    
    def preprocess_job(self, job: Dict) -> Dict:
        param = json.loads(job['param'])
        param['start_execution_datetime'] = datetime.datetime.fromisoformat(param['start_execution_datetime'])
        job['load_dt'] = job['load_dt'].to_pydatetime().isoformat()
        job['param'] = param

    def update_before_process_job(self, job: Dict):
        query = '''
            UPDATE `trading_terminal_poc.job_inventory`
            SET job_execution_start = @job_execution_start,
                status = @status
            WHERE id = @id
                    AND load_dt = @load_dt
        '''


        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('id', 'INTEGER', job['id']),
                bigquery.ScalarQueryParameter('job_execution_start', 'DATETIME', job['job_execution_start']),
                bigquery.ScalarQueryParameter('status', 'STRING', job['status']),
                bigquery.ScalarQueryParameter('load_dt', 'DATETIME', job['load_dt']),
            ]
        )

        query_job = bq.query(query, job_config=job_config)

        result = query_job.result()

    def update_after_process_job(self, job: Dict):
        query = '''
            UPDATE `trading_terminal_poc.job_inventory`
            SET job_execution_start = @job_execution_start,
                job_execution_end = @job_execution_end,
                status = @status
            WHERE id = @id
                    AND load_dt = @load_dt
        '''


        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('id', 'INTEGER', job['id']),
                bigquery.ScalarQueryParameter('job_execution_start', 'DATETIME', job['job_execution_start']),
                bigquery.ScalarQueryParameter('job_execution_end', 'DATETIME', job['job_execution_end'] ),
                bigquery.ScalarQueryParameter('status', 'STRING', job['status']),
                bigquery.ScalarQueryParameter('load_dt', 'DATETIME', job['load_dt']),
            ]
        )

        query_job = bq.query(query, job_config=job_config)

        result = query_job.result()
    

    def insert_job_result(self, job: Dict, result: Dict):
        result['id'] = job['id']
        bq.insert_rows_json('trading_terminal_poc.job_pov_result', [result])

    def execute(self):
        while True:
            job = self.query_single_job()
            self.preprocess_job(job)
            

            job['job_execution_start'] = datetime.datetime.now().isoformat()
            job['status'] = 'PROCESSING'
            self.update_before_process_job(job)

            try:
                param = job['param']
                result = PoV(**param)

                job['status'] = 'SUCCESS'
            except Exception as e:
                job['status'] = 'FAILED'
                result['remark'] = str(e)
            finally:
                job['job_execution_end'] = datetime.datetime.now().isoformat()

                self.update_after_process_job(job)

                self.insert_job_result(job, result)
            
executor = PoVJobExecutor()
executor.execute()