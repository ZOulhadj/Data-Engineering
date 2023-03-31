import logging
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator



def create_conn(**kwargs):
    conn = Connection(conn_id=kwargs["conn_id"],
                      conn_type=kwargs["conn_type"],
                      host=kwargs["host"],
                      login=kwargs["login"],
                      password=kwargs["pwd"],
                      port=kwargs["port"],
                      description=kwargs["desc"])
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return None

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn.conn_id} is created')
    return conn


def create_airflow_connection(dag: DAG) -> PythonOperator:
    task = PythonOperator(
        task_id='create_connection',
        python_callable=create_conn,
        op_kwargs={ "conn_id" : "1",
                    "conn_type" : "postgres", 
                    "host" : "localhost", 
                    "login" : "airflow", 
                    "pwd": "airflow", 
                    "port" : "5432", 
                    "desc" : "Postgres database connection"},
        dag=dag,
    )

    return task
