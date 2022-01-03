from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import  DummyOperator
from custom_operators.tworp_spark_submit_operator import TwoRPSparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

usuario="2rp-gustavo"
default_args = {
    "owner": usuario,
    "start_date": datetime(2021, 12, 29),
    "depend_on_past": False,
    "run_as_user": usuario,
    "proxy_user": usuario
    }

with DAG(dag_id='DAG_de_gustavo_dev', schedule_interval=None, default_args=default_args, catchup=False) as dag:
    
    Dummy_task = DummyOperator(
        task_id = "Dummy_task"
    ) 


    t_kinit = BashOperator(
        task_id="t_kinit",
        bash_command=f'kinit -kt /home/{usuario}/{usuario}.keytab {usuario}'
    )

    executar_task =  BashOperator(
        task_id="executar_task",
	bash_command= 'sh /home/'+usuario+'/shell-script/executar.sh /home/2rp-gustavo/teste-shell mensagem_executar_task'
    )

    pokemons_oldschool_task = TwoRPSparkSubmitOperator(
        task_id="pokemons_oldschool_task",
        name="pokemons_oldschool",
        conn_id="spark_conn",
        application=f'/home/{usuario}/pokemons_oldschool.py',
        keytab=f"/home/{usuario}/{usuario}.keytab",
        principal=usuario,
        proxy_user=None,
        verbose=True
    )

Dummy_task >> t_kinit >> executar_task >> pokemons_oldschool_task


