# Based on https://docs.astronomer.io/

from airflow import DAG, XComArg
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 3, 10)
) as dag:
    def funcion_uno():
        resultados = [
            {'foo': 1, 'bar': 2},
            {'moo': 5, 'car': 6},
            {'doo': 3, 'tar': 4}
        ]
        # this adjustment is due to op_args expecting each argument as a list
        resultado_formateado = [[x] for x in resultados]  # like [[1], [2]]
        return resultado_formateado

    def funcion_dos(x):
        print(x)

    start = DummyOperator(task_id="start")

    funcion_uno_task = PythonOperator(
        task_id="funcion_uno_task",
        python_callable=funcion_uno
    )

    funcion_dos_task = PythonOperator.partial(
        task_id="funcion_dos_task",
        python_callable=funcion_dos
    ).expand(op_args=XComArg(funcion_uno_task))

    end = DummyOperator(task_id="end")

    start >> funcion_uno_task >> funcion_dos_task >> end
