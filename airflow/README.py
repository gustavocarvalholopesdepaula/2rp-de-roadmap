# AIRFLOW

# DEFAULT COMFIGURATION

# sql_alchemy-conn e executor são dois parâmetros importantes para o airflow e necessários na primeira execução
# Executar no terminal e utilizando o bash. '$'
airflow config get-value core sql_alchemy_conn

airflow config get-value core executor

# Criando uma DAG
# Exemplo 1:

from airflow import DAG

from datetime import datetime

default_args= {
    'start_date': datetime(2020, 1 , 1)
}

with DAG (dag_id='parallel_dag', schedule_interval=@daily, default_args=default_args, catchup=False) as dag:

    task_1=BashOperator(
        task_id="task_1"
        bash_command='sleep3'
    )

    task_2=BashOperator(
        task_id='task_2'
        bash_command='sleep3'
    )

    
    task_3=BashOperator(
        task_id="task_3"
        bash_command='sleep3'
    )

    
    task_4=BashOperator(
        task_id="task_4"
        bash_command='sleep3'
    )

    task_1 >> [task_2, task_3] >> task_4

# Exemplo 2:

from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

usuario="2rp-gustavo"
default_agrs = {
    "owner": usuario,
    "start_date": date_time (2021,12,30),
    "depend_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "run_as_user": usuario,
    "proxy_user": usuario
}

with DAG(dag_id='DAG_de_gustavo_dev', schedule_interval="None", default_agrs={}, catchup=False) as dag:
    
    t_kinit = BashOperator(
        task_id="t_kinit",
        bash_command=f'kinit -kt /home/{usuario}/ {usuario}.keytab/'
         )

    t_spark_job_teste = TwoRPSparkSubmitOperator(
        task_id="t_spark_job_teste",
        name="spark_job_teste",
        conn_id="spark_conn",
        application=f'/home/{usuario}/testes/spark_teste.py'
        keytab=f"/home/{usuario}/{usuario}.keytab",
        principal=usuario,
        proxy_user=None,
        verbose=True
    )


t_kinit >> t_spark_job_teste


# Parallelism : permite definir o número máximo de tarefas que podem ser executadas em paralelo durante todo o exemplo.

# DAG_concurrency: 

# max_active_runs_per_DAG : permite denifir o núemro máximo de tarefas que podem ser executadas em um exemplo durante um dia.

# Operadores: Definem 1 task no pipeline. USAR 1 operador para uma task

# Action operators: executam uma ação
# Trasnfer operators: transferem dados
# Sensors: espera que uma condição seja cumprida
# Criando tabelas

from airflow import DAG
# para interagir com o banco de dados de modo seguro
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime


default_agrs{
    'start_date': datetime(2020, 1, 1)
}

with DAG('user_processing', schedule interval:'@daily
        default_args=default_args,
        catchup=False
        )

creating_table = SqliteOperator(
    task_id='creating table',
    sqlite_conn_id='db_sqlite',
    sql='''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
        );
        '''
)

# Instala os provedores do airflow
pip install apache-airflow-providers-http==2.0.0

# lista dos provedores
airflow providers list

# Testando as tasks
airflow tasks test 'dag_id''task_id''data'

# Verifica se o API está 'available'
# Exemplo:
from airflow.providers.http.sensors.http import HttpSensor

is_api_available = HttpSensor(
    task_id='is_api_available'
    http_conn_id='user_api'
    endpoint='api'
)

# Extraindo usuários
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

extracting_user = SimpleHttpOperator(
    task_id = "extracting_user",
    http_conn_id='user_api',
    endpoint='api/',
    method='GET',
    response_filter= lambda response: json.loads(response.text),
    log_response=True

)

# Processando usuários
# Exemplo:
from airflow.operators.python import PythonOperator
from pandas import json_normalize

processing_user = PythonOperator(
    task_id='processing_user',
    python_callable=_processing_user
)

def _processing_user():
    users=None
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    processed_user = json_nromalize({
        'firstname': user['name']['first'],
        'lastaname': user['name']['last'],
        'country': user['location']['country']
        'username':user['login']['password']
        'email': user['email']
    })

with DAG('user_processing',schedule interval='@daily')
        default_args=default_args,
        catchup=False) as dag:
    
    creating_table = SqliteOperator(

    )

# Taskgroups
# Exemplo:

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args= {
    'start_date': datetime(2020, 1 , 1)
}

with DAG (dag_id='parallel_dag', schedule_interval=@daily, default_args=default_args, catchup=False) as dag:

    task_1=BashOperator(
        task_id="task_1"
        bash_command='sleep3'
    )

with TaskGroup('processing_tasks') as processing_tasks:

    task_2=BashOperator(
        task_id='task_2'
        bash_command='sleep3'
    )

    
    task_3=BashOperator(
        task_id="task_3"
        bash_command='sleep3'
    )

    
    task_4=BashOperator(
        task_id="task_4"
        bash_command='sleep3'
    )

    task_1 >> processing_tasks >> task_4

# para escolher uma task de acordo com determinado critério
BranchPythonOperator

# Changing the way your tasks are triggered
# No exemplo abaixo, a task_1 irá falhar, mas task_2 e task_3 serão executadas com sucesso.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args={
    'start_date': datetime (2020, 1, 1)
}

with DAG('trigger_rule', schedule_interval='@daily', default_args=default_args, catchup=False) 

    task_1 = BashOperator(
        task_id='task_1',
        bash_command='exit 1',
        do_xcom_push=False
    )

        task_2 = BashOperator(
        task_id='task_2',
        bash_command='exit 0',
        do_xcom_push=False
    )

        task_3= BashOperator(
        task_id='task_3',
        bash_command='exit 0',
        do_xcom_push=False,
        trigger_rule='all_done'
    )
    
    [task_1, task_2] >> task_3    

# xcom
# Exemplo1:

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
with DAG(dag_id='bash_dag', schedule_interval="@once", start_date=datetime(2020, 1, 1), catchup=False) as dag:
# Task 1
dummy_task = DummyOperator(task_id='dummy_task')
# Task 2
bash_task = BashOperator(task_id='bash_task', bash_command="/usr/local/airflow/dags/command.sh ", xcom_push=True)
dummy_task >> bash_task


# Exemplo 2:

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model():
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')

def _choose_best_model():
    print('choose best model')

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = PythonOperator(
        task_id='task_4',
        python_callable=_choose_best_model
    )

    downloading_data >> processing_tasks >> choose_model




# Curso Alura
# pq  usar o ELT ao invés do ETL

# Vamos primeiro carregar os dados (L) no Data Lake para permitir reprocessamento (T) em caso de falhas ou bugs.

# Alternativa correta! Executar a carga antes da transformação permite que o dado seja transformado apenas quando necessário, prevenindo transformações desnecessárias e permitindo transformar múltiplas vezes, se for preciso.
# 
# 
# Apache Airflow é uma plataforma de gerenciamento de fluxo de trabalho em código aberto. Tudo começou na Airbnb, em outubro de 2014, como uma solução para gerenciar os fluxos cada vez mais complexos da empresa. A criação do Airflow permitiu que a Airbnb pudesse criar e agendar programaticamente seus fluxos de trabalho, monitorando-os por meio da interface de usuário integrada do Airflow. Desde o início, o projeto é open source, tornando-se um projeto Apache Incubator em março de 2016, e um projeto Top-Level Apache Software Foundation em janeiro de 2019.
# 
# O Airflow é escrito em Python, e os fluxos de trabalho são criados por meio de scripts nessa mesma linguagem. O Airflow é projetado sob o princípio de "configuração como código". Embora outras plataformas de fluxo de trabalho deste tipo existam, usando linguagens de marcação como XML, o uso de Python permite que os desenvolvedores importem bibliotecas e classes para ajudá-los a criarem seus fluxos de trabalho.
# 
# Antes do Airflow existir, uma ferramenta bastante usada era o Cron, que basicamente é um agendador de scripts para sistemas Unix. O problema em usá-lo era que qualquer monitoramento ou alerta deveria ser implementado externamente. Outra ferramenta que faz o agendamento ao monitoramento é o Jenkins. Esta ferramenta é bastante usada em processos de CI/CD, e também como orquestrador de pipelines, pela junção do agendamento ao monitoramento dos trabalhos.
# 
# Em ambas as plataformas o agendamento de scripts não permite a quebra em etapas orquestradas, conhecido como DAG, que significa “gráfico acíclico direcionado”, ou seja, o Jenkins e o Cron permitem simplesmente que o trabalho seja executado, mas não existe nenhuma junção entre as etapas ou execuções.
# 
# Para exemplificar, um processo de ETL consiste em 3 etapas bem determinadas que devem ser executadas uma após a outra: a extração, a transformação e a carga. Sendo assim, com sistemas como o Cron ou o Jenkins, você tem 2 opções, criar um único script que executa todos os 3 passos, ou 3 execuções separadas utilizando um horário determinado para cada. Ou seja, se a extração normalmente demora 15 min, a segunda etapa, de transformação, será agendada pelo menos 15 min depois da extração. Isso pode causar tempo ocioso, ou 2 etapas rodando ao mesmo tempo, o que pode não ser proveitoso.
# 
# Apache Spark é um motor analítico unificado para processamento de dados em larga escala. Ele fornece APIs de alto nível em Java, Scala, Python e R, e um mecanismo otimizado que oferece suporte a gráficos de execução geral. Spark também oferece suporte a um rico conjunto de ferramentas de nível superior, incluindo Spark SQL para SQL e processamento de dados estruturados, MLlib para aprendizado de máquina, GraphX para processamento de gráfico e Streams estruturados para computação incremental e processamento de fluxo.
# 
# Spark começou a ser desenvolvido por Matei Zaharia no UC Berkeley's AMPLab em 2009, e teve seu código aberto em 2010 sob uma licença BSD. Em 2013, o projeto foi doado para a Apache Software Foundation e mudou sua licença para o Apache 2.0. Em fevereiro de 2014, Spark tornou-se um Projeto Apache de nível superior. O Spark teve mais de 1000 colaboradores em 2015, tornando-o um dos projetos mais ativos na Apache Software Foundation, e um dos projetos de big data de código aberto mais ativos.
# 
# História do Spark e Hadoop
# Em 2003 e 2004, Google publicou dois papers (artigos), o primeiro sobre o Google File System, que é um sistema de arquivos distribuído e escalável para grandes aplicações distribuídas, provendo tolerância a falhas e a possibilidade de armazenamento de centenas de terabytes. O segundo paper fala sobre um modelo de programação para simplificar o processamento de dados em grandes clusters utilizando duas funções, o Map e o Reduce, que significam “mapear” e “reduzir”.
# 
# Com base nessas ideias da Google, foi criado o Apache Hadoop em 2006, um framework composto por, entre outros:
# 
# Hadoop MapReduce, para processamento distribuído em larga escala;
# Hadoop Distributed File System, ou HDFS, que é o sistema de arquivos distribuído, e a base do que hoje conhecemos como Data Lake;
# Hadoop YARN, lançado em 2012, a plataforma de gerenciamento de recursos computacionais no cluster.
# Junto com o Hadoop, várias ferramentas foram criadas utilizando o motor do Hadoop, como por exemplo o Apache Pig, que é uma linguagem de alto nível para facilitar a criação de trabalhos de MapReduce, e o Apache Hive, criado pelo Facebook e muito usado até hoje, e permite uma interface utilizando SQL para pesquisas no HDFS.
# 
# Outra ferramenta criada foi o Apache Spark, muito parecido com o Hadoop MapReduce, cujo principal foco foi a possibilidade de aplicação de funções repetidamente a um conjunto de dados, impossível de ser feito no Hadoop, sendo necessário iniciar um novo processo e carregar os dados em memória toda vez. A motivação vinha de algoritmos de Machine Learning e pesquisas analíticas que precisam carregar um certo conjunto de dados na memória e aplicar diferentes interações. Com isso, o Spark conseguia superar o Hadoop em trabalhos de Machine Learning com processamento 10x mais rápido, além de pesquisar um conjunto de dados de 40GB em menos de 1 segundo. Atualmente o Spark chega a processar 100x mais rápido que o Hadoop MapReduce.
# 
# 
# Como vimos nessa aula de instalação do Airflow, precisamos iniciar um ou mais serviços para que ele possa funcionar. Quais são estes serviços, e para que são usados?
# 
# Dois serviços: Webserver e Scheduler, o primeiro para iniciar o UI e o segundo para agendamento para tarefas.
# 
# Ganchos são interfaces para comunicar o DAG com recursos externos compartilhados, por exemplo, várias tarefas podem acessar um mesmo banco de dados MySQL. Assim, em vez de implementar uma conexão para cada tarefa, é possível receber a conexão pronta de um gancho.
# 
# Ganchos usam conexões para ter acesso a endereço de serviços e formas de autenticação, mantendo assim o código usado para autenticação e informações relacionadas fora das regras de negócio do data pipeline.
# 
# Nesta aula instalamos o Apache Airflow e começamos a configurar esta ferramenta para usá-la como orquestrador do pipeline que vamos construir neste curso. Também acessamos os dados do Twitter através de requisições à sua API, e assim garantimos que temos acesso aos dados que precisamos processar.
# 
# Conectando o processo de extração de dados do Twitter ao Airflow, começamos com a criação de uma conexão e de um gancho. As conexões são importantes para armazenar diferentes parâmetros de acesso, abstraindo isto do código e mantendo em segurança no banco de dados do Airflow, além de permitir o compartilhamento de acessos.
# 
# Com os ganchos, criamos as funções comuns que serão utilizadas pelos diferentes trabalhos no Airflow para interagir com a API do Twitter. Começamos com um dos Endpoints que retorna os tweets recentes relacionados a uma palavra-chave, essa requisição foi criada de forma que podemos trocar a palavra-chave e o período de tempo em relação aos dados.
# 
# https://github.com/alura-cursos/alura-data-pipeline/tree/Aula-3
# 
# No Airflow, operadores determinam o que será executado em uma tarefa, descrevendo um único passo. Um operador possuirá três características:
# 
# tres caracteristicas
# 
# 1) Idempotência: Independentemente de quantas vezes uma tarefa for executada com os mesmos parâmetros, o resultado final deve ser sempre o mesmo;
# 
# 2) Isolamento: A tarefa não compartilha recursos com outras tarefas de qualquer outro operador;
# 
# 3) Atomicidade: A tarefa é um processo indivisível e bem determinado.
# 
# Operadores geralmente executam de forma independente, e o DAG vai garantir que operadores sejam executados na ordem correta. Quando um operador é instanciado, ele se torna parte de um nodo no DAG.
# 
# Todos os operadores derivam do operador base chamado BaseOperator, e herdam vários atributos e métodos. Existem 3 tipos de operadores:
# 
# Operadores que fazem uma ação ou chamam uma ação em outro sistema;
# Operadores usados para mover dados de um sistema para outro;
# Operadores usados como sensores, que ficam executando até que um certo critério é atingido. Sensores derivam da BaseSensorOperator e utilizam o método poke para testar o critério até que este se torne verdadeiro ou True, e usam o poke_interval para determinar a frequência de teste.
# 
# Como vimos nas aulas de criação de operadores do Airflow, quais são as três características que devemos buscar em um operador?
# 
# Atômico, idempotente e isolado.
# 
# 
# Alternativa correta! Correto, quando um operador é criado, ele precisa ser uma tarefa única ou atômica, permitir reexecução ou ser idempotente e não compartilhar recursos ou ser isolado.
# 
# Macros são uma forma de expor objetos usando modelos e Jinja templates. Airflow utiliza uma poderosa ferramenta chamada Jinja, uma ferramenta de modelos onde marcadores permitem escrever código Python para ser traduzido quando o modelo é aplicado.
# 
# Existe uma lista de variáveis padrão que vem embutida no Airflow, que pode ser acessada aqui. A sua maioria é em relação à data de execução da tarefa, mas também pode retornar objetos como o DAG ou a instância da tarefa, acessando assim os parâmetros destes objetos.
# 
# Os macros também podem ser usados para acessar bibliotecas do Python, como datetime, timedelta, dateutil e time. Usando macros.datetime, você pode acessar funções de data e horário que serão traduzidas em tempo de execução.
# 
# Há três propriedades definidas como chaves para entender como podemos medir o Big Data e comparar quão diferente eles são dos dados que já conhecemos. São eles:
# 
# 1) Volume: a característica mais óbvia, o Big Data tem a ver com o volume. É muito comum hoje em dia as empresas armazenarem Terabytes, se não Petabytes de dados em seus servidores.
# 
# 2) Velocidade: o crescimento dos dados, resultando na sua importância, mudou a forma que olhamos para eles. Velocidade essencialmente mede quão rápido os dados são gerados. Algumas fontes de dados são em tempo real, outras não tão frequentes, mas em pacotes.
# 
# 3) Variedade: quando coletamos dados, eles nem sempre vão estar em um formato conhecido e esperado, como CSV ou em um banco de dados. Muitas vezes temos que extrair dados de formatos variados, como texto e vídeo ou até de redes sociais.
# 
# Big Data é mais do que somente “um monte de dados”, é a forma de prover oportunidades utilizando dados novos ou já existentes, descobrindo novas formas de capturar dados futuros para fazer a diferença em operações,tornan do-as mais ágeis.
# 
# Se você estiver usando a versão 2.0 do Airflow, a forma de adicionar operadores foi alterada. Agora não mais serão usados plugins, e sim módulos do Python. Para adicionar novos operadores como módulos ao Python, você pode seguir a documentação do Airflow.
# 
# Operadores são os objetos mais importantes no Airflow, sem eles nenhuma tarefa pode ser executada. Uma tarefa no Airflow nada mais é do que uma implementação de um operador. Esta implementação configura o operador com parâmetros específicos, tornando cada tarefa única para aquele conjunto de parâmetros.
# 
# Verifique os parâmetros necessários para o operador PythonOperator. O principal e único obrigatório é o parâmetro python_callable para o qual passamos um objeto do Python que pode ser chamado. Quando configurado, este objeto pode executar diferentes códigos. Sendo assim, o operador se torna genérico o suficiente para ser utilizado de diversas maneiras onde o método será a execução de um código Python.
# 
# https://github.com/alura-cursos/alura-data-pipeline/tree/Aula-4
# 
# 
# 
 from spark.sql import functions as f
 df = spark.read.json(“caminho/do/arquivo.json”)
 df.printSchema()
 df.select(“ID”).orderBy(f.desc(“ID”)).show(3)

# 
# Quais são os comandos que devo executar para ler um arquivo JSON, imprimir sua estrutura e depois imprimir os últimos 3 IDs?
# 
# 
# Alternativa correta! Primeiro usamos read json para ler o arquivo JSON para o dataframe, imprimimos o esquema do dataframe e então selecionamos o campo ID, ordenamos de forma decrescente e mostramos apenas os 3 primeiros.
# 
# 
# 
# Trabalhar com o Spark é muito divertido porque ele disponibiliza diversas ferramentas e opções com apenas alguns comandos. A biblioteca SQL suporta diversas fontes de dados através da interface do DataFrame. Como vimos, os DataFrames podem ser utilizados com comandos de transformação, mas também podem ser criados como uma view temporária que permite executar comandos SQL nos dados.
# 
# Na página de fontes de dados do Spark voce pode encontrar alguns métodos para carregar e salvar dados usando Spark, além de opções específicas disponíveis para estas fontes de dados.
# 
# 
# 
# https://github.com/alura-cursos/alura-data-pipeline/tree/Aula-5
# 
# 
# Em vídeo, descobrimos duas formas de reestruturar DataFrames com mais ou menos partições. O que fazem os comandos Repartition e Coalesce?
# 
# O Repartition vai reprocessar o DataFrame, distribuindo novamente os dados para o número de partições requerido, enquanto o Coalesce vai juntar as partições até o número requerido de partições, mesmo que elas fiquem desbalanceadas.
# 
# A arquitetura em medalhas, do inglês medallion, permite acesso flexível e processamento de dados extensíveis. As tabelas na camada bronze são usadas para ingestão de dados e permitem acesso rápido, sem a necessidade de modelagem a uma única fonte de dados. Conforme os dados fluem para as tabelas na camada Silver, ou prata, eles se tornam mais refinados e otimizados para a inteligência de negócio, ou BI, e ciência de dados, através de transformações.
# 
# As camadas Bronze e Silver agem como um ODS, ou seja, um banco de dados operacional, cujas tabelas permitem modificações ágeis para serem reproduzidas nas tabelas transformadas. Para análises profundas, se utiliza a camada Gold, ou ouro, e o usuário possui o poder de extrair conhecimento e formular pesquisas.
# 
# Pensando no data lake como um lago que purifica a água para ser consumida por analistas em suas pesquisas de BI e cientistas de dados em seus algoritmos de Machine Learning, as tabelas na etapa Bronze recebem água constantemente e em grande quantidade, e então fica “suja”, com diferentes procedências. Esta água flui constantemente para a etapa Silver, juntando com águas que vieram de diferentes locais e começando a ser purificada, até que, como em um rio de águas cristalinas, é encontrada na camada Gold pronta para consumo.
# 
# Todos os operadores na versão 2.0 do Airflow são compatíveis com os da versão 1.10. Enquanto isso, na versão 1.0 muitos pacotes vinham instalados por padrão no core do Airflow, e alguns na pasta contrib. E na versão 2.0 os operadores são instalados separadamente como pacotes Python. Cerca de 60 diferentes provedores de pacotes podem ser instalados, como por exemplo Amazon, Google, Salesforce, etc.
# 
# Para instalar os operadores do Apache Spark, é necessário instalar também o pacote chamado apache-airflow-providers-apache-spark, com o comando pip:
# 
# pip install apache-airflow-providers-apache-spark
# Exemplos de como usar os operadores do Spark podem ser encontrados no GitHub do Airflow.
# 
# Nesta aula construímos nosso primeiro trabalho usando Apache Spark escrito em Python. Estes tipos de trabalhos normalmente seguem o formato Batch quando processam os dados, ou seja, processamos pacotes de dados. Esse processamento de dados junta fontes de dados e executa agregações, talvez até aplicando modelos de Machine Learning, mas seja qual for a complexidade, podemos reduzir sempre usando a definição de ETL, cujos passos são extração, transformação e exportação.
# 
# Quando usamos a biblioteca pyspark, criamos um trabalho em Python, e com isso podemos seguir boas práticas ao criarmos o código e o colocamos em produção. É importante aplicá-las para estruturar o código corretamente, e para que ele seja fácil de ser testado e debugado, passando parâmetros para o trabalho e gerenciando dependências com outros módulos ou pacotes.
# 
# https://github.com/alura-cursos/alura-data-pipeline/tree/Aula-6
# 
# Nesta aula finalizamos o DAG, que criamos com dois passos a serem executados em sequência, usando-se os colchetes angulares duplos (>>) para conectar uma etapa à outra. Para adicionar outra etapa de extração que conecta a mesma etapa de transformação, executando em paralelo as duas extrações, como devo configurar no DAG?
# 
extração_1 >> transformação
extração_2 >> transformação
# 
# O Apache Zeppelin é uma ferramenta de notebooks interativa executada no navegador, que permite que engenheiros(as), analistas e cientistas de dados sejam mais produtivos(as) quando desenvolvendo, organizando, executando e compartilhando códigos de dados e visualizando resultados sem utilizar linha de comando ou configurar servidores. Os notebooks, muito usados em ferramentas como Jupyter ou Google Colab, permitem aos usuários não somente executar, mas trabalhar de modo interativo em longos e complexos fluxos de trabalho.
# 
# O Zeppelin permite o uso de diversas linguagens e frameworks, sendo o principal deles o Spark com Python, mas também é possível usar outras linguagens, como Scala e SQL. A lista de interpretadores que o Zeppelin possui é vasta, e os exemplos vão de BigQuery, PostgreSQL, ElasticSearch, Cassandra, Flink, Beam, Pig, até mesmo Angular.
# 
# Instalar e utilizar o Zeppelin em sua máquina local é bastante simples, com alguns comandos você pode rapidamente começar a criar notebooks interagindo com o Spark. Acesse as configurações de interpretadores, ou interpreters, e no Spark coloque o caminho da pasta na variável SPARK_HOME. Com um novo notebook aberto, você simplesmente coloca %pyspark no começo da linha e o Zeppelin vai automaticamente interpretar o código no pySpark.
# 
# Parabéns, você conseguiu chegar ao fim da construção do data pipeline! Começamos com requisitos do projeto, em que deveríamos providenciar dados para o time de marketing e cientistas de dados referentes a dados do Twitter relacionados ao perfil da Alura Online. Muitos outros projetos serão como este na carreira de um(a) engenheiro(a) de dados.
# 
# O importante é entender o que se quer atingir no pipeline antes de pensar nas ferramentas que vai usar. O mundo dos dados possui inúmeras ferramentas para diversas finalidades. Este pipeline pedia um desenvolvimento em Python usando arquitetura em Batch onde o formato ELT se encaixava perfeitamente, mas pode ser que o projeto em desenvolvimento precise de ferramentas para streaming ou bancos de dados para cache, ou então seja necessário apenas criar um processo para uma única execução.
# 
# Portanto, é importante que você conheça a fundo várias ferramentas e arquiteturas para saber aplicá-las aos projetos conforme seus requisitos. Este é o primeiro passo de vários nessa área, o importante é sempre continuar aprendendo!
