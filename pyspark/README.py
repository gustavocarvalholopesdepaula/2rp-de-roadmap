# COMANDOS EM PYTHON

python
# Inicia o Python 

exit()/CRTL-D
# Sai do Python 

ctrl+l
# Limpa a tela 

help(type)
# Detalhes sobre um comando em particular. No caso, o comando 'type'.

# DECLARANDO VARIÁVEIS

a = 0  # Declara a variável 'a'
type(a) # Mostra o tipo da variável 'a'

a = True
type(a)

a = "Hello"
type(a)

# We can convert types using functions like int, bool etc
a = "5"
type(a)
int(a) # converts 5 of type string to int

# We can invoke function print to print output on the console
print("Hello World")


# soma os números de 1 a 100
l = range(1, 100)

for i in l:
  res += I

print(res)


# soma de todos os pares de 1 a 100

l = range(1, 100)
for i in l:
  if(i % 2 == 0):
    res += i

print(res)</pre>


# Mostra a soma de todos os números pares e ímpares de 1 a 100

resEven = 0
resOdd = 0
l = range(1, 100)

for i in l: 
  if(i % 2 == 0):
    resEven += i
  else:
    resOdd += i

print(resEven)
print(resOdd)

# soma de todos os números de 1 a 100 usando 'while'

res = 0
i = 1
while(i &lt;= 100):
  res = res + i
  i = i + 1

print(res)


# CRIANDO FUNÇÕES

def sum(lb, ub):
  total = 0
  while(lb <= ub):
    total += lb
    lb += 1
  return total

def sumOfSquares(lb, ub):
  total = 0
  while(lb <= ub):
      total += lb * lb
      lb += 1
  return total

  # higher order functions, funções que usam argumentos de outra função

  def sum(func, lb, ub):
  total = 0
  while(lb <= ub):
    total += func(lb)
    lb += 1
  return total



# Invocando a função higher order

def id(i):
  return i

def sqr(i):
  return i * i

def cube(i):
  return i * i * i

# Invoking HigherOrderSum
sum(id, 1, 10)

sum(sqr, 1, 10)

sum(cube, 1, 10)

# Anonymous/lambda functions
# também pode usar funções lambda ou anónimas enquanto invoca funções de ordem superior
# Uma função sem nome é chamada de função anónima/lambda
# Podemos fornecer uma lógica simples para funções lambda 
# Podemos usar funções lambda enquanto invocamos uma função de ordem superior onde a função assume outra função como argumento

# Sum of integers in a range
sum(lambda i: i, 1, 10)

# Sum of squares in a range
sum(lambda i: i * i, 1, 10)

# Sum of cubes in a range
sum(lambda i: i * i * i, 1, 10)

# LIST
l = [1, 2, 3, 4, 1, 2, 8, 3, 1]  # Cria uma lista
# Contêm valores duplos nesse caso
s = set (l)
# 's' não terá mais valores duplos


# SET
s = set([8, 1, 2, 3, 4])
# Não contêm valores duplos
list (s)
# converte 'set' para 'list'

# DICT

d = { 1 : "Hello", 2 : "world" }
# d[1] returns "Hello" and d[2] returns "world"
# d[2] = "folks" will change d to {1: 'Hello', 2: 'folks'}
# d.keys will give a list of keys
# d.items will return elements in the form of a tuple
# d.has_key(3) return false as there is no element with key k
# d.get(1) will return “Hello” same as d[1]
# d.get(3) returns null as there is no element in d with key 3, while d[3] throws KeyError exception

# soma dos quadrados usando map reduce

l = range(1, 100)
f = filter(lambda i: i % 2 == 0, l)
m = map(lambda i: i * i, f)
r = reduce(lambda total, element: total + element, m)
print(r)

# Processando dados de um arquivo

orderItemsFile = open("/data/retail_db/order_items/part-00000")
orderItemsRead = orderItemsFile.read()
orderItems = orderItemsRead.splitlines()
orderItemsFilter = filter(lambda rec: int(rec.split(",")[1]) == 68880, orderItems)
orderItemsMap = map(lambda rec: float(rec.split(",")[4]), orderItemsFilter)
orderItemsRevenue = reduce(lambda total, element: total + element, orderItemsMap)

# PYSPARK
# DATAFRAMES

# Importa 'SparkSession'
from pyspark.sql import SparkSession

# Inicia a SparkSession
spark = SparkSession.builder \
    .getOrCreate()

# Exemplo de formato completo de inicialização da 'SparkSession'
spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    enableHiveSupport(). \
    appName(f'{username} | Python - Data Processing - Overview'). \
    master('yarn'). \
    getOrCreate()


# POSSÍVEIS FORMATOS
# Esses são os possíveis formatos de arquivos

# text - para ler dados de uma única coluna de arquivos de texto, bem como ler cada um de todo o arquivo de texto como um registro.

# csv- para ler arquivos de texto com delimitadores. O padrão é uma vírgula, mas podemos usar outros delimitadores também.

# json - para ler dados de arquivos JSON

# orc - para ler dados de arquivos ORC.

# parquet - para ler dados de arquivos parquet

# Para ler dados de determinado formato
spark.read.format

# Tb podemos apresentar outras opções baseadas no formato.

# Mostra os tipos de dados das colunas.
inferSchema 

# para usar o cabeçalho para obter os nomes das colunas no caso de arquivos de texto.
header

# Mostra o esquema
schema 

# ajuda
# Lê dados de arquivos texto.
help(spark.read.csv)

# LENDO A TABELA
# Exemplo:

spark. \
    read. \
    json('/public/retail_db_json/orders'). \
    show()
    
# CRIANDO UMA DATAFRAME

data = [('Zeca','35'),('Eva','29')]
colNames = ['Nome','Idade']
df = spark.createDataFrame (data, colNames)
df
# df é o nome dado ao DataFrame

df.show()
# Visualiza o DataFrame df

df. toPandas()
# Visualiza o DataFrame usando o Pandas

# CARREGAMENTO DE DADOS DE ARQUIVOS CSV
# Montando o drive

from google.colab import drive
drive.mount ('/content/drive')

# Carregando os dados das empresa

import zipfile

zipfile.ZipFile('/content/drive/MyDrive/curso-spark/empresas.zip','r').extractall('/content/Drive/MyDrive/curso-spark')
path = '/content/drive/MyDrive/curso-spark/empresas/part-0000*' 
# O '*' indica que serão lidos todos os arquivos inciados por 'part-0000'
empresas = spark.read.csv(path, sep=';',inferSchema=True)


# Carregando os dados de uma tabela hive dentro do dataframe utizando HiveContext
from pyspark.sql import HiveContext
Hive_context = HiveContext(sc)
generation = Hive_context.table("work_dataeng.generation_gustavo")


# Manipulando dados

# Lista os 5 primeiros itens do Data Frame
empresas.limit(5).toPandas()

# Renomeando as colunas do DataFrame
empresasColNames = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_da_situacao_especial']

for index, ColName in enumerate(empresasColNames):
  empresas = empresas.withColumnRenamed(f"_c{index}",colName)

# visualiza as colunas
empresas.columns

# Analisando dados
# Mostra o tipo de colunas que tem no DataFrame
empresas.printSchema()

# Convertendo uma'string' para uma 'double'

from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as f 

empresas = empresas.withColumn('capital_social_da_empresa', f.regexp_replace('capital_social_da_empresa',',','.'))
# substitui toda vírgula da coluna 'capital_social_da_empresa' por ponto.
# IMPORTANTE: se quisessemos substituir os pontos por vírgulas, o ponto teria que ser espeficado como '\.'
# nesse momento, se fizessemos 'empresas.printSchema()', perceberíamos que nossa coluna ainda está como string.
# Para transformar STRING para DOUBLE:


empresas = empresas.withColumn('capital_social_da_empresa', empresas['capital_social_da_empresa'].cast(DoubleType()))


# Outro exemplo:
# Suponha que temos uma coluna de dados monetários representados como string da seguinte forma:

# “1.000.000,00”

# Note que aqui o dados usa um padrão diferente do americano para representar dados numéricos. A informação acima usa separadores de milhar que não são necessários e também utiliza o caractere “,” para representar as casas decimais. O correto para que nosso DataFrame reconheça esta informação como um double seria:

# 1000000.00

# Assinale a opção que indica os passos corretos para realizar essa transformação.

# Obs.: Considere df como uma DataFrame genérico que possui a coluna valor com a informação apresentada acima (“1.000.000,00”).
# Alternativa correta! Primeiro retiramos os separadores de milhar que são desnecessários, depois modificamos o separador decimal de “,” para “.” e por fim convertemos a string para double.

# CONVERTENDO STRING PARA DATE 
# '\' pula linha
# 'f.to_date' chama a função 'to_date'

estabelecimentos = estabelecimentos\ 
  .withColumn(
    "data_situacao_cadastral",
    f.to_date(estabelecimentos.data_situacao_cadastral.cast(StringType)),'yyyyMMdd')
    )\
    .withColumn(
    "data_de_inicio_atividade",
    f.to_date(estabelecimentos.data_situacao_cadastral.cast(StringType)),'yyyyMMdd')
    )\
    .withColumn(
    "data_da_situacao_especial",
    f.to_date(estabelecimentos.data_situacao_cadastral.cast(StringType)),'yyyyMMdd')
    )

# SELEÇÕES E COLUNAS
    
# Seleciona todas as colunas do DataFrame e mostra as primeiras 5 linhas
# A informação pode aparecer incompleta, para corrigir isso basta substituir a última linha por '.show(5,False)'
    empresas\
      .select(*)\
      .show(5)

# Seleciona somente algumas colunas

    empresas\
      .select('natureza_juridica','porte_da_empresa','capital_social_da_empresa')
      .show(5)

# Mostra as colunas mencionadas e apenas o ano da coluna'capital_social_da_empresa', além de trazer essa informação com outro nome, no caso, 'ano_de_entrada'
    socios\
    .select('nome_do_socio_ou_razao_social','faixa_etaria',f.year('capital_social_da_empresa').alias('ano_de_entrada'))\
    .show(5,False)



    df \
    .select(
        f.concat_ws(
            ', ', 
            f.substring_index('nome', ' ', -1), 
            f.substring_index('nome', ' ', 1)
        ).alias('ident'), 
        'idade') \
    .show(truncate=False)

# A função concat_ws concatena (junta) várias colunas de strings em uma única coluna utilizando como separador a informação fornecida no primeiro argumento. Outra função que utilizamos foi a substring_index que divide uma string de acordo com um delimitador específico e retorna a substring de acordo com sua posição. Observe que quando queremos pegar a última ocorrência podemos utilizar o índice -1.

# O programa deve retornar:

ident	idade
CASTRO, GISELE	15
OLIVEIRA, ELAINE	22
LOURDES, JOAO	43
FERREIRA, MARTA	24
ROEDER, LAUDENETE	51

# ORDENANDO OS DADOS
# Vai mostrar as primeiras 5 linhas da tabela com base no ano de entrada de forma decrescente.

socios\
    .select('nome_do_socio_ou_razao_social','faixa_etaria',f.year('capital_social_da_empresa').alias('ano_de_entrada'))\
    .OrderBy('ano_de_entrada', ascending=False)\
    .show(5,False)

# Vai mostrar as primeiras 10 linhas da tabela com base no ano de entrada e na faixa etária.

socios\
    .select('nome_do_socio_ou_razao_social','faixa_etaria',f.year('capital_social_da_empresa').alias('ano_de_entrada'))\
    .OrderBy(['ano_de_entrada','faixa_etaria'], ascending=[False, False])\
    .show(10,False)


# Exemplo:
# Utilize o DataFrame df do código abaixo para responder o exercícios:

data = [
    ('CARMINA RABELO', 4, 2010), 
    ('HERONDINA PEREIRA', 6, 2009), 
    ('IRANI DOS SANTOS', 12, 2010), 
    ('JOAO BOSCO DA FONSECA', 3, 2009), 
    ('CARLITO SOUZA', 1, 2010), 
    ('WALTER DIAS', 9, 2009), 
    ('BRENO VENTUROSO', 1, 2009), 
    ('ADELINA TEIXEIRA', 5, 2009), 
    ('ELIO SILVA', 7, 2010), 
    ('DENIS FONSECA', 6, 2010)
]
colNames = ['nome', 'mes', 'ano']
df = spark.createDataFrame(data, colNames)
df.show(truncate=False)

# O DataFrame df contém os nomes de dez alunos com os respectivos meses e anos de nascimento. 
# Utilizando o conteúdo aprendido no último vídeo, assinale a alternativa que apresenta o código correto para ordenar este DataFrame dos alunos mais novos para os mais velhos.

df\
    .select('*')\
    .orderBy(['ano', 'mes'], ascending=[False, False])\
    .show(truncate=False)

# A ordem em que são informadas as colunas para o método orderBy influencia diretamente o resultado.
# Como queremos os mais novos primeiro temos que inicialmente ordenar de forma decrescente a coluna de anos e depois ordenar de forma decrescente a coluna de meses.

# FILTRANDO OS DADOS

# Usando uma string com condição
# Vai mostrar todos os dados da tabela que tiverem o capital social da empresa = 50

empresas\
  .where("capital_social_da_empresa==50")\
  .show(5, False)

# Passando a condição através de uma coluna do DataFrame
# Vai mostrar todos os 10 primeiros nomes que começarem com 'Gustavo' e terminarem com 'Paula'
# da coluna 'nome_do_socio_ou_razao_social' do DataFrame 'socios'
# Aqui também poderia ter sido utilizado o 'where' no lugar do 'filter'

socios\
  .select("nome_do_socio_ou_razao_social")\
  .filter(socios.nome_do_socio_ou_razao_social.startswith("Gustavo"))\
  .filter(socios.nome_do_socio_ou_razao_social.endswith("Paula"))\
  .limit(10)\
  .toPandas()

# Exemplo

data = [
    ('CARMINA RABELO', 4, 2010), 
    ('HERONDINA PEREIRA', 6, 2009), 
    ('IRANI DOS SANTOS', 12, 2010), 
    ('JOAO BOSCO DA FONSECA', 3, 2009), 
    ('CARLITO SOUZA', 1, 2010), 
    ('WALTER DIAS', 9, 2009), 
    ('BRENO VENTUROSO', 1, 2009), 
    ('ADELINA TEIXEIRA', 5, 2009), 
    ('ELIO SILVA', 7, 2010), 
    ('DENIS FONSECA', 6, 2010)
]
colNames = ['nome', 'mes', 'ano']
df = spark.createDataFrame(data, colNames)
df.show(truncate=False)

# Os códigos a seguir trazem os alunos nascidos no primeiro semestre de 2009.

# Sempre que formos utilizar um operador lógico entre duas expressões no Python precisamos envolver as expressões com parênteses. Note também que os operadores lógicos neste caso são diferentes. 
# Temos o & para AND e o | para o OR.

df\
    .filter((df.mes <= 6) & (df.ano == 2009))\
    .show(truncate=False)


# Utilizando uma string para realizar nossa query podemos utilizar os operadores lógicos com and e or. 
# No caso abaixo usamos o and pois precisávamos que as duas condições fossem atendidas.
df\
    .filter("mes<=6 and ano==2009")\
    .show(truncate=False)

# Note que podemos, neste caso, utilizar o encadeamento de métodos filter.
# Isto é equivalente a utilizar o operador lógico AND. 
# Outro ponto interessante de ser destacado é que em uma query no formato string não existe diferença entre os operadores = e ==.
# Ambos podem ser utilizados para comparações neste tipo de query.
df\
    .filter("mes<=6")\
    .filter("ano=2009")\
    .show(truncate=False)


# Nesse caso, não poderia ter sido utilizado '=' no lugar do '=='
# '=' tem a função de atribuição e por isso não pode ser utilizado em um procedimento de comparação,NESTE CASO.
# Para os casos de comparação utilizamos o operador '=='.
df\
    .filter(df.mes <= 6)\
    .filter(df.ano == 2009)\
    .show(truncate=False)

# O COMANDO LIKE
# Vai criar um DataFrame com as linhas 'RESTAURANTE DO RUÍ','Juca restaurantes ltda' e 'Joca Restaurante' , todos na coluna 'data'

df = spark.CreateDataFrame([('RESTAURANTE DO RUÍ',),('Juca restaurantes ltda',),('Joca Restaurante')],['data'])
df.toPandas()

# Buscando tudo que tenha 'RESTAURANTE' no nome ou razão social
# A função upper retorna todas as strings em letra maiúscula, no caso serão todas as strings da coluna 'data'
# Não significa que na tabela aparecerá tudo maiúsculo.
# O comando '.like(%RESTAURANTE%)' reconhecerá todas as palavras'RESTAURANTE', mesmo as que estão em letra minúscula.
# O programa vai retornar a string do modo como foi escrita na criação do DataFrame.
# Se estivessemos escrito'.like('RESTAURANTE%'), seriam retornados apenas os nomes que começassem com 'RESTAURANTE'
# Se estivessemos escrito'.like('%RESTAURANTE'), seriam retornados apenas os nomes que terminassem com 'RESTAURANTE'

df\
  .where(f.upper(df.data).like('%RESTAURANTE%'))\
  .show(truncate=False)

# Do DataFrame 'empresas', serão retornados as colunas do 'select', aonde na coluna 'razao_social_nome_empresarial', exista a palavra 'RESTAURANTE'
# Lembrando que mesmo se 'RESTAURANTE' estiver em minúsculo na string, será retornado, pois o comando 'upper' transformou tudo em maiúsculo.

empresas\
  .select('razao_social_nome_empresarial','natureza_juridica','porte_da_empresa','capital_social_da_empresa')\
  .filter(f.upper(empresas['razao_social_nome_empresarial']).like(%RESTAURANTE%))\
  .show(15, False)

# Exemplo:

data = [
    ('CARMINA RABELO', 4, 2010), 
    ('HERONDINA PEREIRA', 6, 2009), 
    ('IRANI DOS SANTOS', 12, 2010), 
    ('JOAO BOSCO DA FONSECA', 3, 2009), 
    ('CARLITO SOUZA', 1, 2010), 
    ('WALTER DIAS', 9, 2009), 
    ('BRENO VENTUROSO', 1, 2009), 
    ('ADELINA TEIXEIRA', 5, 2009), 
    ('ELIO SILVA', 7, 2010), 
    ('DENIS FONSECA', 6, 2010)
]
colNames = ['nome', 'mes', 'ano']
df = spark.createDataFrame(data, colNames)
df.show(truncate=False)

# O código correto para selecionarmos apenas os alunos que têm seus nomes iniciados com a letra “C” segue abaixo:

df\
    .filter(df.nome.like('C%'))\
    .show(truncate=False)

# O código abaixo estaria ERRADO!
# DataFrames do Spark não possuem o método like.
# Este é um método que deve ser aplicado a objetos Column que são as colunas de um DataFrame.
# É um conceito similar às Series do pacote Pandas.

df\
    .select('nome')\
    .like('C%')\
    .show(truncate=False)

# ERRADO!

# SUMARIZANDO OS DADOS
# Esse código seleciona o ano da'data_de_entrada_sociedade, renomeia como 'ano_de_entrada'
# Mostra a partir de 2010
# Agrupa pelo 'ano_de_entrada' e faz a contagem em cima disso, ordenando de forma crescente
# Portanto, esse código retornará em ordem crescente a partir de 2010, a contagem de todos os sócios que entraram, por ano.

socios\
  .select(f.year('data_de_entrada_sociedade').alias('ano_de_entrada'))\
  .where('ano_de_entrada >= 2010')\
  .groupBy('ano_de_entrada')\
  .count()\
  .orderBy('ano_de_entrada', ascending=True )\
  .show()


# Agrupa 'porte_da_empresa', ordena de forma crescente, e retorna a média do capital social das empresas, por grupo, além de quantas empresas tem por grupo.
# Os grupos, no caso, são empresas do porte1 , empresas do porte 3 e empresas do porte 5.

empresas\
  .select('cnpj_basico','porte_da_empresa','capital_social_da_empresa')\
  .groupBy('porte_da_empresa')\
  .agg( # É como se fizesse um outro agrupamento dentro do GropuBy
      f.avg("capital_social_da_empresa").alias)("capital_social_medio")\
      f.count("cnpj_basico").alias("frequencia")\
  )\
  .orderBy('porte_da_empresa', ascending=True)\
  .show()

# O código abaixo retorna todas as funções disponíveis, que estão comentadas abaixo. Poderiam ser especificas, se fosse desejado uma função em particular.
# .summary("count","mean","stdev","min","25%","50%","75%","max")

empresas\
  .select("capital_social_da_empresa")\
  .summary()\
  .show()

# WHEN/OTHERWISE

# Suponha que temos um DataFrame com os nomes de alguns alunos, as matérias que eles cursaram e as respectivas notas em cada matéria:

data = [
    ('CARLOS', 'MATEMÁTICA', 7), 
    ('IVO', 'MATEMÁTICA', 9), 
    ('MÁRCIA', 'MATEMÁTICA', 8), 
    ('LEILA', 'MATEMÁTICA', 9), 
    ('BRENO', 'MATEMÁTICA', 7), 
    ('LETÍCIA', 'MATEMÁTICA', 8), 
    ('CARLOS', 'FÍSICA', 2), 
    ('IVO', 'FÍSICA', 8), 
    ('MÁRCIA', 'FÍSICA', 10), 
    ('LEILA', 'FÍSICA', 9), 
    ('BRENO', 'FÍSICA', 1), 
    ('LETÍCIA', 'FÍSICA', 6), 
    ('CARLOS', 'QUÍMICA', 10), 
    ('IVO', 'QUÍMICA', 8), 
    ('MÁRCIA', 'QUÍMICA', 1), 
    ('LEILA', 'QUÍMICA', 10), 
    ('BRENO', 'QUÍMICA', 7), 
    ('LETÍCIA', 'QUÍMICA', 9)
]
colNames = ['nome', 'materia', 'nota']
df = spark.createDataFrame(data, colNames)
df.show()

# Neste caso seria interessante ter uma rotina que criasse um indicador para os alunos APROVADOS ou REPROVADOS. 
# Com a função when podemos criar esta nova coluna de forma bastante simples.

df = df.withColumn('status', f.when(df.nota >= 7, "APROVADO").otherwise("REPROVADO"))
df.show()

# A função when é bem simples de ser utilizada, bastando passar no primeiro argumento a condição que queremos testar e no segundo argumento qual valor atribuir a nova coluna caso esta condição seja verdadeira.
# O método otherwise pode ser utilizado para indicar o valor que a nova coluna deve ter caso a condição testada na função when não seja verdadeira.

# DESCRIBE
# Este método permite a seleção de quais colunas vão fazer parte da sumarização e ele retorna as seguintes estatísticas descritivas: count, mean, stddev, min e max.


# SPARK SQL

# Ambos os códigos abaixo fornecem o mesmo resultado
# Código em SQL

empresas_join.createOrReplaceTempView("empresasJoinView")

freq = spark\
    .sql("""
        SELECT YEAR(data_de_inicio_atividade) AS data_de_inicio, COUNT(cnpj_basico) AS count
            FROM empresasJoinView 
            WHERE YEAR(data_de_inicio_atividade) >= 2010
            GROUP BY data_de_inicio
            ORDER BY data_de_inicio
    """)

freq\
    .show()


# Código em Python
empresas_join\
    .select(f.year(empresas_join.data_de_inicio_atividade).alias('data_de_inicio'))\
    .where("data_de_inicio >= 2010")\
    .groupBy('data_de_inicio')\
    .count()\
    .orderBy('data_de_inicio')\
    .show()

# ARQUIVOS PARQUET

# É um formato de armazenamento colunar disponível para qualquer projeto no ecossistema Hadoop, independentemente da escolha da estrutura de processamento de dados, modelo de dados ou linguagem de programação.

# ARQUIVOS ORC

# O projeto do ORC foi criado em 2013 como uma iniciativa de acelerar o Hive e reduzir o armazenamento no Hadoop. O foco era habilitar o processamento de alta velocidade e reduzir o tamanho dos arquivos.

# Assim como o PARQUET, ORC é um formato de arquivo colunar. Ele é otimizado para grandes leituras de streaming, mas com suporte integrado para localizar as linhas necessárias rapidamente. O armazenamento de dados em formato colunar permite ler, descompactar e processar apenas os valores necessários para a consulta.

# Muitos grandes usuários do Hadoop adotaram o ORC. Por exemplo, o Facebook usa ORC para salvar dezenas de petabytes em seu data warehouse e demonstrou que ORC é significativamente mais rápido do que RCFILE ou PARQUET. O Yahoo usa o ORC para armazenar seus dados de produção e

# COMPARANDO A PERFORMANCE
# Uma forma bastante simples de se ter uma medida de performance é utilizando a magic word %time. 
# Colocando %%time no início de cada célula do notebook você tem um print do tempo gasto para a execução do código daquela célula.

# FINALIZANDO A SESSÃO SPARK
spark.stop()

# FUNÇÕES

# Concatenando strings
concat

# Getting Length 
length

# Trimming Strings 
trim, rtrim, ltrim

# Padding Strings  
lpad, rpad

# Extracting Strings 
split, substring

# Date Manipulation Functions

# Date Arithmetic 
date_add, date_sub, datediff, add_months

# Date Extraction 
dayofmonth, month, year

# Get beginning period 
trunc, date_trunc

# Numeric Functions 
abs, greatest

# Aggregate Functions 
sum, min, max

# ESCREVENDO DATAFRAMES DE DIFERENTES FORMATOS
# Todas as APIs de gravação em lote são agrupadas sob gravação, que é exposta aos objetos Data Frame.

# Todas as APIs são expostas em spark.read

# text - para gravar dados de coluna única em arquivos de texto.

# csv - para gravar em arquivos de texto com delimitadores. O padrão é uma vírgula, mas podemos usar outros delimitadores também.

# json - para gravar dados em arquivos JSON

# orc - para gravar dados em arquivos ORC

# parquet - para gravar dados em arquivos Parquet.

# Filtragem
 filter/ where 

# Agrupar dados por chave e realizar agregações 
groupBy

# Classificando dados 
sort ou orderBy

# Podemos passar nomes de colunas ou literais ou expressões para todas as APIs de Data Frame.

# As expressões incluem operações aritméticas, transformações usando funções de pyspark.sql.functions.

# Existem aproximadamente 300 funções em pyspark.sql.functions.

# Falaremos sobre algumas das funções importantes usadas para manipulação de string, manipulação de data, etc.

# CATEGORIAS DE FUNÇÃO

# São aproximadamente 300 funções em  pyspark.sql.functions.
Essas funções podem ser agrupadas em categorias.

String Manipulation Functions

Case Conversion 
lower, upper

Getting Length -
length

Extracting substrings -
substring, split

Trimming - 
trim, ltrim, rtrim

Padding - 
lpad, rpad

Concatenating string - 
concat, concat_ws

Date Manipulation Functions

Getting current date and time - 
current_date, current_timestamp

# Date Arithmetic 
date_add, date_sub, datediff, months_between, add_months, next_day

# Beginning and Ending Date or Time - 
last_day, trunc, date_trunc

# Formatting Date - 
date_format

# Extracting Information  
dayofyear, dayofmonth, dayofweek, year, month

# Agregando funções

count, countDistinct

sum, avg

min, max


CASE e WHEN

# for type casting
cast

# Functions to manage special
ARRAY, MAP, STRUCT type columns


# 'col','lit'
# Funções muito usadas para converter dados do tipo strings em dados do tipo coluna
# 'col' é a fução usada para converter o nome da coluna do tipo 'string'para o tipo'column'
from pyspark.sql.functions import col
employeesDF. \
    select(col("first_name"), col("last_name")). \
    show()

from pyspark.sql.functions import concat, col, lit

# 'lit' é usada para converter por exemplo uma ',' em tipo coluna.
employeesDF. \
    select(concat(col("first_name"), 
                  lit(", "), 
                  col("last_name")
                 ).alias("full_name")
          ). \
    show(truncate=False)


# FUNÇÕES PARA MANIPULAÇÃO DE STRINGS

# Concatenando strings
concat

# Converte todos os caracteres alfabéticos em maiúsculo.
upper 

# Converte todos os caracteres alfabéticos em minúsculo.
lower

# Converte o primeiro caracter da string em maiúsculo.
initcap

# Obtêm o número de caracteres em uma string.
length

# Exemplo:

from pyspark.sql.functions import col, lower, upper, initcap, length
employeesDF. \
  select("employee_id", "nationality"). \
  withColumn("nationality_upper", upper(col("nationality"))). \
  withColumn("nationality_lower", lower(col("nationality"))). \
  withColumn("nationality_initcap", initcap(col("nationality"))). \
  withColumn("nationality_length", length(col("nationality"))). \
  show()
+-----------+--------------+-----------------+-----------------+-------------------+------------------+
|employee_id|   nationality|nationality_upper|nationality_lower|nationality_initcap|nationality_length|
+-----------+--------------+-----------------+-----------------+-------------------+------------------+
|          1| united states|    UNITED STATES|    united states|      United States|                13|
|          2|         India|            INDIA|            india|              India|                 5|
|          3|united KINGDOM|   UNITED KINGDOM|   united kingdom|     United Kingdom|                14|
|          4|     AUSTRALIA|        AUSTRALIA|        australia|          Australia|                 9|
+-----------+--------------+-----------------+-----------------+-------------------+------------------+

# SPLIT
# Para colunas de comprimento variável
# split tem dois argumentos:coluna e delimitador
# Converte cada string em array e podemos acessar os elementos usando o índice.
# Também podemos usar 'explode' em conjunto com 'split' para explodir a lista ou array em registros no Data Frame. 
# Pode ser usado em casos como contagem de palavras, contagem de telefones, etc.

# Exemplo 1
from pyspark.sql.functions import split, explode
employeesDF = employeesDF. \
    select('employee_id', 'phone_numbers', 'ssn'). \
    withColumn('phone_number', explode(split('phone_numbers', ',')))
employeesDF.show(truncate=False)
+-----------+---------------------------------+-----------+----------------+
|employee_id|phone_numbers                    |ssn        |phone_number    |
+-----------+---------------------------------+-----------+----------------+
|1          |+1 123 456 7890,+1 234 567 8901  |123 45 6789|+1 123 456 7890 |
|1          |+1 123 456 7890,+1 234 567 8901  |123 45 6789|+1 234 567 8901 |
|2          |+91 234 567 8901                 |456 78 9123|+91 234 567 8901|
|3          |+44 111 111 1111,+44 222 222 2222|222 33 4444|+44 111 111 1111|
|3          |+44 111 111 1111,+44 222 222 2222|222 33 4444|+44 222 222 2222|
|4          |+61 987 654 3210,+61 876 543 2109|789 12 6118|+61 987 654 3210|
|4          |+61 987 654 3210,+61 876 543 2109|789 12 6118|+61 876 543 2109|
+-----------+---------------------------------+-----------+----------------+

# Exemplo 2

l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")
from pyspark.sql.functions import split, explode, lit
df.select(split(lit("Hello World, how are you"), " ")). \
    show(truncate=False)
+----------------------------------+
|split(Hello World, how are you,  )|
+----------------------------------+
|[Hello, World,, how, are, you]    |
+----------------------------------+
df.select(split(lit("Hello World, how are you"), " ")[2]). \
    show(truncate=False)
+-------------------------------------+
|split(Hello World, how are you,  )[2]|
+-------------------------------------+
|how                                  |
+-------------------------------------+
df.select(explode(split(lit("Hello World, how are you"), " ")).alias('word')). \
    show(truncate=False)
+------+
|word  |
+------+
|Hello |
|World,|
|how   |
|are   |
|you   |
+------+

# SUBSTRING
# Para colunas de comprimento fixo.
# Extrai substrings da string principal.
# Têm 3 argumentos : coluna, posição e comprimento.
# Primeiro argumento é a coluna da qual será extraída a substring.
# Segundo argumento é o caracter do qual a string será extraída.
# Terceiro argumento é o número de caracteres do primeiro argumento.
# Também podemos fornecer a posição do final passando o valor negativo.

# Exemplo

from pyspark.sql.functions import substring, lit
# First argument is a column from which we want to extract substring.
# Second argument is the character from which string is supposed to be extracted.
# Third argument is number of characters from the first argument.
df.select(substring(lit("Hello World"), 7, 5)). \
  show()


# Para preencher uma string com um caractere específico no lado esquerdo ou à esquerda 
lpad 

# Para preencher no lado direito ou à direita.
rpad

# Exemplo:

from pyspark.sql.functions import lpad, rpad, concat
empFixedDF = employeesDF.select(
    concat(
        lpad("employee_id", 5, "0"), 
        rpad("first_name", 10, "-"), 
        rpad("last_name", 10, "-"),
        lpad("salary", 10, "0"), 
        rpad("nationality", 15, "-"), 
        rpad("phone_number", 17, "-"), 
        "ssn"
    ).alias("employee")
)
empFixedDF.show(truncate=False)
+------------------------------------------------------------------------------+
|employee                                                                      |
+------------------------------------------------------------------------------+
|00001Scott-----Tiger-----00001000.0united states--+1 123 456 7890--123 45 6789|
|00002Henry-----Ford------00001250.0India----------+91 234 567 8901-456 78 9123|
|00003Nick------Junior----00000750.0united KINGDOM-+44 111 111 1111-222 33 4444|
|00004Bill------Gomes-----00001500.0AUSTRALIA------+61 987 654 3210-789 12 6118|
+------------------------------------------------------------------------------+


# Aparando strings
# Corta espaços a esquerda
ltrim

# Corta espaços a direita
rtrim

# Corta espaços em ambos os lados
trim

# Exemplo

from pyspark.sql.functions import col, ltrim, rtrim, trim
df.withColumn("ltrim", ltrim(col("dummy"))). \
  withColumn("rtrim", rtrim(col("dummy"))). \
  withColumn("trim", trim(col("dummy"))). \
  show()
+-------------+----------+---------+------+
|        dummy|     ltrim|    rtrim|  trim|
+-------------+----------+---------+------+
|   Hello.    |Hello.    |   Hello.|Hello.|
+-------------+----------+---------+------+

# MANIPULANDO DATA E TEMPO
# Obtêm a data do servidor de hoje.
# formato : yyyy-MM-dd
current_date

# Obtêm a hora atual do servidor.
# Formato : yyyy-MM-dd HH:mm:ss:SSS
current_timestamp

# Exemplo

from pyspark.sql.functions import current_date, current_timestamp
df.select(current_date()).show() #yyyy-MM-dd
+--------------+
|current_date()|
+--------------+
|    2021-02-28|
+--------------+
df.select(current_timestamp()).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS
+-----------------------+
|current_timestamp()    |
+-----------------------+
|2021-02-28 18:34:08.548|
+-----------------------+

# Converte uma string que contêm data 
to_date
# Converte uma string que contêm horário
to_timestamp

# Exemplo

from pyspark.sql.functions import lit, to_date, to_timestamp
df.select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date')).show()
+----------+
|   to_date|
+----------+
|2021-02-28|
+----------+
df.select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()
+-------------------+
|       to_timestamp|
+-------------------+
|2021-02-28 17:25:00|
+-------------------+

# VALORES NULOS

# Retorna o primeiro valor não nulo
coalesce
# Exemplo
from pyspark.sql.functions import lit
employeesDF. \
    withColumn('bonus1', coalesce('bonus', lit(0))). \
    show()
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|bonus1|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|    10|
|          2|     Henry|     Ford|1250.0| null|         India|+91 234 567 8901|456 78 9123|     0|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|      |
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|    10|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+


# Avalia expressões, não necessariamente aritméticas.
# sql
expr
# Exemplo
from pyspark.sql.functions import expr
employeesDF. \
    withColumn('bonus', expr("nvl(bonus, 0)")). \
    show()
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0|    0|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+

# CASE/WHEN
# Tipicamente usados em transformações baseadas em condições. CASE e WHEN são similares a 'expr' or 'selectExpr', no SQL.
# Também pode ser usado juntamente com '.otherwise'
# Exemplo
from pyspark.sql.functions import when
employeesDF. \
    withColumn(
        'bonus',
        when((col('bonus').isNull()) | (col('bonus') == lit('')), 0).otherwise(col('bonus'))
    ). \
    show()
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0|    0|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|    0|united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+

# INNER JOIN
# Pega os pontos de intersecção entre os dois dataframes
orders. \
    join(
        order_items, 
        on=orders['order_id'] == order_items['order_item_order_id'],
        how='inner'
    ). \
    select(orders['*'], order_items['order_item_subtotal']). \
    show()

# LEFT JOIN E RIGHT JOIN
# Pega todos os registros da esquerda e da direita, respectivamente e faz a intersecção na coluna passada.
# Exemplo:

select * from pedido p
left join pedido_item i on i.num_pedido = p.numero_pedido

# COMO ENVIAR UM SCRIPT PARA O CLUSTER
# Executar no terminal
# Exemplo:
spark-submit --master yarn --conf spark.driver.memory=15g --conf spark.executer.memory=6g --conf spark.executer.memoryOverhead=1g --conf spark.cores.max=3 --conf spark.dynamicAllocation.maxExecutors=40 --keytab /home/<seu-usuario>/<seu-usuario>.keytab --principal <seu-usuario> --name dev-pokemons_oldschool --deploy-mode client /home/<seu-usuario>/caminho/para/pokemons_oldschool.py

# SPARK SQL

# Ambos os códigos abaixo fornecem o mesmo resultado
# Código em SQL

empresas_join.createOrReplaceTempView("empresasJoinView")

freq = spark\
    .sql("""
        SELECT YEAR(data_de_inicio_atividade) AS data_de_inicio, COUNT(cnpj_basico) AS count
            FROM empresasJoinView 
            WHERE YEAR(data_de_inicio_atividade) >= 2010
            GROUP BY data_de_inicio
            ORDER BY data_de_inicio
    """)

freq\
    .show()


# Código em Python
empresas_join\
    .select(f.year(empresas_join.data_de_inicio_atividade).alias('data_de_inicio'))\
    .where("data_de_inicio >= 2010")\
    .groupBy('data_de_inicio')\
    .count()\
    .orderBy('data_de_inicio')\
    .show()

# USANDO O SQL
# Seleciona todas as colunas de orders limitando a 10.
SELECT * FROM orders LIMIT 10

# Seleciona todas as colunas de 'orders' sem pegar os valores repetidos e limitando a 10.
SELECT DISTINCT * FROM orders LIMIT 10

# Obs: se estiver usando o zeppelin basta iniciar o parágrafo com %sql, se for python, utilizar %pyspark

# Filtrando
# Seleciona todas as colunas de order_status aonde o status for 'COMPLETO' e limitando a 10 registros.
SELECT * FROM orders WHERE order_status = 'COMPLETE' LIMIT 10

# inner join
# Exemplo:

SELECT o.order_id,
    o.order_date,
    o.order_status,
    oi.order_item_subtotal
FROM orders o JOIN order_items oi
    ON o.order_id = oi.order_item_order_id
LIMIT 10

# Obtêm todos os registros de ambos os conjuntos de dados que satisfaçam a condição JOIN juntamente com os registros que estão na tabela do lado esquerdo, mas não na tabela do lado direito
LEFT OUTER JOIN

# Obtêm todos os registros de ambos os conjuntos de dados que satisfaçam a condição JOIN junto com os registros que estão na tabela do lado direito, mas não na tabela do lado esquerdo.
RIGHT OUTER JOIN

# Une a esquerda com a direita
FULL OUTER JOIN

# Agregando dados
# Exemplo:
SELECT order_item_order_id,
    round(sum(order_item_subtotal), 2) AS order_revenue
FROM order_items
GROUP BY order_item_order_id LIMIT 10

# Ordenando dados
# Exemplo
SELECT o.order_date,
    oi.order_item_product_id,
    round(sum(oi.order_item_subtotal), 2) AS revenue
FROM orders o JOIN order_items oi
    ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY o.order_date,
    oi.order_item_product_id
ORDER BY o.order_date,
    revenue DESC
LIMIT 10

# Trunca a tabela
TRUNCATE TABLE

# Apaga a tabela
DROP TABLE

# Carregando dados de uma pasta para dentro de uma tabela.
LOAD DATA LOCAL INPATH '/data/retail_db/orders' INTO TABLE orders

# Criando tabela externa
# Exemplo:
CREATE EXTERNAL TABLE orders (
  order_id INT COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/itversity/external/retail_db/orders'

# Formatos aceitos
# TEXTFILE, ORC, PARQUET, AVRO, SEQUENCEFILE, JSONFILE

# Como usar o SELECT/INSERT
INSERT INTO TABLE order_items
SELECT * FROM order_items_stage

# Apaga os dados antigos e insere os novos.
INSERT OVERWRITE TABLE

# Criando tabelas com partições
# Exemplo:

CREATE TABLE orders_part (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) PARTITIONED BY (order_month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

# Adicionando partições
# Exemplo:

ALTER TABLE orders_part ADD PARTITION (order_month=201307)

ALTER TABLE orders_part ADD
    PARTITION (order_month=201308)
    PARTITION (order_month=201309)
    PARTITION (order_month=201310)

# Carregando dados dentro de uma determinada partição
# Exemplo:
LOAD DATA LOCAL INPATH '/home/itversity/orders/orders_201309'
  INTO TABLE orders_part PARTITION (order_month=201309)

# Inserindo dados dentro de uma partição
INSERT INTO TABLE orders_part PARTITION (order_month=201311)
  SELECT * FROM orders WHERE order_date LIKE '2013-11%'

# Categorias de funções do SQL

# String Manipulation

# Date Manipulation

# Numeric Functions

# Type Conversion Functions

# CASE e WHEN

# Funções usadas para manipular strings

# Case Conversion 
lower, upper, initcap

# Getting size of the column value 
length

# Extracting Data  
substr and split

# Trimming and Padding functions 
trim, rtrim, ltrim, rpad and lpad

# Reversing strings 
reverse

# Concatenating multiple strings
concat and concat_ws

# Case Conversion Functions 
lower, upper, initcap
# Exemplo:

SELECT lower('hEllo wOrlD') AS lower_result,
    upper('hEllo wOrlD') AS upper_result,
    initcap('hEllo wOrlD') AS initcap_result

#Getting length 
length
# Exemplo:

SELECT length('hEllo wOrlD') AS result

# split
# Exemplo:

SELECT split('2013-07-25', '-') AS result

# substr
# Exemplo:

SELECT substr('2013-07-25 00:00:00.0', 12) AS result

# ltrim,rtrim,trim

# Usado para remover os espaços a esquerda da string
ltrim 

# Usado para remover os espaços a direita da string
rtrim  

# Usado para remover os espaços de ambos os lados da strin.
trim is used to remove the spaces on both sides of the string.

# Exemplo:

SELECT ltrim('     Hello World') AS result
%%sql

SELECT rtrim('     Hello World       ') AS result
%%sql

SELECT length(trim('     Hello World       ')) AS result

# reverse
# para reverter a ordem dos caracteres da string
# Exemplo:

SELECT FirstName, REVERSE(FirstName) AS Reverse  
FROM Person.Person  
WHERE BusinessEntityID < 5  
ORDER BY FirstName;  
GO  


FirstName      Reverse
-------------- --------------
Ken            neK
Rob            boR
Roberto        otreboR
Terri          irreT

(4 row(s) affected)

# Concatenando várias strings
# Exemplo:

SELECT concat(year, '-', lpad(month, 2, 0), '-',
              lpad(myDate, 2, 0)) AS order_date
FROM
    (SELECT 2013 AS year, 7 AS month, 25 AS myDate) q

# Funções numéricas

# Retorna o módulo.
abs 

# Retornam respectivamente, a soma e a média.
sum, avg

# Retorna um valor decimal, arredondando de acordo com o número de casas decimais especificadas na função
round 

# Retorna o número inteiro
ceil, floor 

# Retorna o maior.
greatest

# Retornam mínimo e máximo
min, max

# Getting Current Date and Timestamp
# Retorna a data
date_add

# Retorna a data de iníco
date_trunc

# Extrai informações da data
date_format

# Retorna o tempo e o número de dias que se passaram desde o surgimento do Linux.
from_unixtime, to_unix_timestamp

