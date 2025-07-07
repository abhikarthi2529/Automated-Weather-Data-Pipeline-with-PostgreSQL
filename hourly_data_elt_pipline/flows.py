from prefect import flow, task
import load

@task
def load_data_to_db_postgres():
    l = load.Load()
    l.load_data_to_db()
    
@flow
def execute_tasks():
    load_data_to_db_postgres()
    
if __name__  == '__main__':
     execute_tasks.serve(name = "execute ELT pipline for hourly data - Test1", cron = '0 0 * * *')




