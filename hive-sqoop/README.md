COMANDOS DO HIVE:

# Cria a base de dados chamada training_retail
CREATE DATABASE IF NOT EXISTS training_retail;

# Formatos de arquivo
TEXTFILE
ORC
PARQUET
AVRO
SEQUENCEFILE
JSONFILE

# Cria uma tabela no formato ORC chamada order_items dentro da base de dados training_retail
CREATE TABLE training_retail.order_items (
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
) STORED AS orc;


CREATE TABLE orders (
  order_id INT COMMENT 'Unique order id', 
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
SHOW CREATE TABLE orders;

# Mostra a estrutura da tabela e seu conteúdo.
# Aqui mostra como comentar nas tabelas hive.

# Mostra todas as caraterísticas da tabela
DESCRIBE FORMATTED order_items


# Carrega os dados da pasta dentro da tabela order_items
LOAD DATA LOCAL INPATH '/data/retail_db/order_items/' INTO TABLE order_items;

# Seleciona todas as colunas da tabela order_items e mostra as 10 primeiras
SELECT * FROM order_items LIMIT 10;


# Conta as colunas da tabela order_items
SELECT count(1) FROM order_items;


# Inseri dados na tabela order_items
INSERT INTO TABLE order_items;


# Lista os arquivos
dfs -ls

# Exibi as últimas linhas do arquivo
dfs -tail
 

# Lista as tabelas
SHOW TABLES;


# Apaga a tabela
DROP TABLE;


# Apaga apenas as linhas e mantêm a estrutura da tabela, colunas e comentários  
TRUNCATE TABLE;


# Apaga o que tem na tabela e inseri novos dados
INSERT OVERWRITE TABLE order_items;

# Criando partitioned table e inserindo dados na tabela.
# Let us understand how to create partitioned table and get data into that table.

# Earlier we have already created orders table. We will use that as reference and create partitioned table.
# We can use PARTITIONED BY clause to define the column along with data type. In our case we will use order_month as partition column.
# We will not be able to directly load the data into the partitioned table using our original orders data (as data is not # in sync with structure).

USE training_retail;
CREATE TABLE orders_part (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) PARTITIONED BY (order_month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

# LOADING INTO PARTITIONS IN HIVE TABLES
# Let us understand how to use load command to load data into partitioned tables.

# We need to make sure that file format of the file which is being loaded into table is same as the file format used while creating the table.
# We also need to make sure that delimiters are consistent between files and table for text file format.
# Also data should match the criteria for the partition into which data is loaded.
# Our /data/retail_db/orders have data for the whole year and hence we should not load the data directly into partition.
# We need to split into files matching partition criteria and then load into the table.
# Splitting data into smaller files by month.

grep 2013-07 /data/retail_db/orders/part-00000 ~/orders/orders_201307
grep 2013-08 /data/retail_db/orders/part-00000 ~/orders/orders_201308
grep 2013-09 /data/retail_db/orders/part-00000 ~/orders/orders_201309
grep 2013-10 /data/retail_db/orders/part-00000 ~/orders/orders_201310

Loading data into corresponding partitions
USE training_retail;
LOAD DATA LOCAL INPATH '/home/training/orders/orders_201307'
  INTO TABLE orders_part PARTITION (order_month=201307);
LOAD DATA LOCAL INPATH '/home/training/orders/orders_201308'
  INTO TABLE orders_part PARTITION (order_month=201308);
LOAD DATA LOCAL INPATH '/home/training/orders/orders_201309'
  INTO TABLE orders_part PARTITION (order_month=201309);
LOAD DATA LOCAL INPATH '/home/training/orders/orders_201310'
  INTO TABLE orders_part PARTITION (order_month=201310);

dfs -ls -R /apps/hive/warehouse/training_retail_db/orders_part;
dfs -tail /apps/hive/warehouse/training_retail_db/orders_part/order_month=201310/orders_201310;


# Inserindo dados nas partitions
# Let us understand how to use insert data into static partitions in Hive from existing table called as orders.
# We can pre-create partitions in Hive partitioned tables and insert data into partitions using appropriate INSERT command.

USE training_retail;
ALTER TABLE orders_part ADD PARTITION (order_month=201311);

INSERT INTO TABLE orders_part PARTITION (order_month=201311)
  SELECT * FROM orders WHERE order_date LIKE '2013-11%';

# Let us understand how we can insert data into partitioned table using dynamic partition mode.

# Using dynamic partition mode we need not pre create the partitions. Partitions will be automatically created when we
# issue INSERT command in dynamic partition mode.
# To insert data using dynamic partition mode, we need to set the property hive.exec.dynamic.partition to true
# Also we need to set hive.exec.dynamic.partition.mode to nonstrict
# You will see new partitions created starting from 201312 to 201407.

USE training_retail;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE orders_part PARTITION (order_month)
SELECT o.*, date_format(order_date, 'YYYYMM') order_month
FROM orders o
WHERE order_date >= '2013-12-01 00:00:00.0';

# CREATING BUCKETED TABLES

# Let us see how we can create Bucketed Tables in Hive.

# Bucketed tables is nothing but Hash Partitioning in conventional databases.
# We need to specify the CLUSTERED BY Clause as well as INTO BUCKETS to create Bucketed table.

USE training_retail;
CREATE TABLE orders_buck (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) CLUSTERED BY (order_id) INTO 8 BUCKETS;

# INSERTING DATA INTO BUCKETED TABLES
Let us see how we can add data to bucketed tables.

Typically we use INSERT command to get data into bucketed tables, as source data might not match the criterial of our bucketed table.
If the data is in files, first we need to get data to stage and then insert into bucketed table.
We already have data in orders table, let us use to insert data into our bucketed table orders_buck
hive.enforce.bucketing should be set to true.
Here is the example of inserting data into bucketed table from regular managed or external table.

INSERT INTO orders_buck
SELECT * FROM orders;

dfs -ls /apps/hive/warehouse/training_retail.db/orders_buck;

SELECT * FROM orders_buck LIMIT 10;

# BUCKETING WITH SORTING

We can also ensure that data is sorted with in the bucket in the bucketed table.

Number of files in the bucketed tables will be multiples of number of buckets.
Using hash mod algorithm on top of bucket key, data will land into appropriate file.
However data is not sorted with in bucket.
We can sort the data with in the bucket by using SORTED BY while creating bucketed tables
Let us get create table statement of orders_buck and recreate with SORTED BY clause.
Here is the example of creating bucketed table using sorting and then inserting data into it.

USE training_retail;
CREATE TABLE orders_buck (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) 
CLUSTERED BY (order_id)
SORTED BY (order_id)
INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT INTO orders_buck
SELECT * FROM orders;
Use dfs -tail command to confirm data is sorted as expected.

# HIVE ACID TRANSACTIONS TO INSERT, UPDATE AND DELETE DATA

Pre -Requisites

Hive Transactions Manager should be set to DbTxnManager SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
We need to enable concurrency SET hive.support.concurrency=true;
Once we set the above properties, we should be able to insert data into any table.
For updates and deletes, table should be bucketed and file format need to be ORC or any ACID Compliant Format.
We also need to set table property transactions to true TBLPROPERTIES ('transactional'='true');


CREATE TABLE orders_transactional (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) CLUSTERED BY (order_id) INTO 8 BUCKETS
STORED AS orc
TBLPROPERTIES('transactional' = 'true');

INSERT INTO orders_transactional VALUES 
(1, '2013-07-25 00:00:00.0', 1000, 'COMPLETE');

INSERT INTO orders_transactional VALUES 
(2, '2013-07-25 00:00:00.0', 2001, 'CLOSED'),
(3, '2013-07-25 00:00:00.0', 1500, 'PENDING');

UPDATE orders_transactional 
  SET order_status = 'COMPLETE'
WHERE order_status = 'PENDING';

DELETE FROM orders_transactional
WHERE order_status <> 'COMPLETE';

SELECT * FROM orders_transactional;

# FUNÇÕES

# Lista as funções do Hive
SHOW functions;

# Descreve as funções
DESCRIBE FUNCTION substr;

# Validando funções 
# A função substr nesse caso mostra os 5 primeiros caracteres da string

USE training_retail;
CREATE TABLE dual (dummy STRING);
INSERT INTO dual VALUES ('X');

SELECT current_date FROM dual;
SELECT substr('Hello World', 1, 5) FROM dual;

# STRING MANIPULATION - CASE CONVERSION AND LENGTH
# A função lower imprimirá  a string passada em letre minúscula
# A função upper imprimirá a string passada em letra maiúscula
# A função initcap retorna a string passada com a primeira letra de cada palavra maiúscula.
# A função length retorna o número de caracteres existentes na string passada incluindo os espaços.

SELECT lower('hEllo wOrlD');
SELECT upper('hEllo wOrlD');
SELECT initcap('hEllo wOrlD');
SELECT length('hEllo wOrlD');


# Se a última tabela seja transacional, precisamos habilitar transações.
# As colunas que eram  minúsculas ficarão maiúsculas.

SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
# Seta configurações necessárias, nesse exemplo é uma variável
SELECT * FROM orders LIMIT 10;
SELECT order_id, order_date, order_customer_id,
  upper(order_status) AS order_status
FROM orders LIMIT 10;


# STRING MANIPULATION - SUBSTR AND SPLIT

# SUBSTR - EXTRAI PARTE DA STRING STRING

SELECT substr('2013-07-25 00:00:00.0', 1, 4);
# Retorna 2013, O '1'é a partir de que caracter queremos contar e o '4' é quantos caracteres contaremos a partir do '1'
SELECT substr('2013-07-25 00:00:00.0', 6, 2);
# Retorna 07
SELECT substr('2013-07-25 00:00:00.0', 9, 2); 
# Retorna 25
SELECT substr('2013-07-25 00:00:00.0', 12); 
# Retorna 00:00:00.0

# Todas as linhas da coluna order_date seriam retornada apenas do 1 ao 10 caracter.
SELECT order_id,
  substr(order_date, 1, 10) AS order_date,
  order_customer_id,
  order_status
FROM orders_part LIMIT 10;

# O comando split é empregado para dividir um arquivo em partes de tamanho igual para facilitar a visualização e também a manipulação
# No primeiro caso, o '-' é definido como um delimitador e o '1' indica que a função retornará a primeira palavra da strinf, no caso, '07', pois a função começa a contar no zero.
# No segundo caso,  'explode' fará com que a função retorne as palavras da string separadamente. Nesse caso, retornaria '2013','07'e'25'

SELECT split('2013-07-25', '-')[1];

SELECT explode(split('2013-07-25', '-'));

# Remove o espaço a esquerda da string
ltrim

# Remove o esoaço a direita da string
rtrim

# Remove o espaço de amnos os lados
trim

# Como usar

SELECT ltrim('     Hello World');
SELECT rtrim('     Hello World       ');
SELECT length(trim('     Hello World       '));

# Completa uma string do lado esquerdo
lpad

# Completa uma string do lado direito
rpad

# Como usar

SELECT 2013 AS year, 7 AS month, 25 AS myDate;
SELECT lpad(7, 2, 0); 
# Retorna 07

SELECT lpad(10, 2, 0);
# Retorna 10

SELECT lpad(100, 2, 0)
# Retorna 10

-----------------------------------------------------------------------------------------------------------
Reverse
# Retorna a string ao contrário


SELECT reverse('Hello World');
# Como usar

Concat
# Uni as strings em uma só.

SELECT concat('Hello ', 'World');
SELECT concat('Order Status is ', order_status)
FROM orders_part LIMIT 10;
SELECT * FROM (SELECT 2013 AS year, 7 AS month, 25 AS myDate) q;

SELECT concat(year, '-', lpad(month, 2, 0), '-',
              lpad(myDate, 2, 0)) AS order_date
FROM
(SELECT 2013 AS year, 7 AS month, 25 AS myDate) q;

# Como usar
-------------------------------------------------------------------------------------------------------------------------
SELECT current_date;
# Retorna a data de hoje.

SELECT current_timestamp;
# Retorna o horário de hoje com precisão.

# date_add can be used to add or subtract days.
# days_sub can be used to subtract or add days.
# datediff can be used to get difference between 2 dates
# add_months can be used add months to a date


# Como usar

SELECT date_add(current_date, 32);
SELECT date_add('2018-04-15', 730);
SELECT date_add('2018-04-15', -730);

SELECT date_sub(current_date, 30);

SELECT datediff('2019-03-30', '2017-12-31');

SELECT add_months(current_date, 3);
SELECT add_months('2019-01-31', 1);
SELECT add_months('2019-05-31', 1);
SELECT add_months(current_timestamp, 3);

SELECT date_add(current_timestamp, -730);

# We can use MM to get beginning date of the month.
# YY can be used to get begining date of the year.
# We can apply trunc either on date or timestamp, however we cannot apply it other than month or year (such an hour or day).

# Como usar

DESCRIBE FUNCTION trunc;
# Essa função truca os valores do m~es ou ano para a primeira data possível
SELECT trunc(current_date, 'MM');
SELECT trunc('2019-01-23', 'MM');
# Retorna '2019-01-01', se fosse trucando com 'YYYY', retornaria '2019-01-01'
SELECT trunc(current_date, 'YY');
SELECT trunc(current_timestamp, 'HH'); // will not work

# Here is how we can get date related information such as year, month, day etc from date or timestamp.

DESCRIBE FUNCTION date_format;


SELECT current_timestamp, date_format(current_timestamp, 'YYYY');
SELECT current_timestamp, date_format(current_timestamp, 'YY');
SELECT current_timestamp, date_format(current_timestamp, 'MM');
SELECT current_timestamp, date_format(current_timestamp, 'dd');
SELECT current_timestamp, date_format(current_timestamp, 'DD');

# Here is how we can get time related information such as hour, minute, seconds, milliseconds etc from timestamp.

SELECT current_timestamp, date_format(current_timestamp, 'HH');
SELECT current_timestamp, date_format(current_timestamp, 'hh');
SELECT current_timestamp, date_format(current_timestamp, 'mm');
SELECT current_timestamp, date_format(current_timestamp, 'ss');
SELECT current_timestamp, date_format(current_timestamp, 'SS'); // milliseconds

# Here is how we can get the information from date or timestamp in the format we require.

SELECT date_format(current_timestamp, 'YYYYMM');
SELECT date_format(current_timestamp, 'YYYYMMdd');
SELECT date_format(current_timestamp, 'YYYY/MM/dd');

# We can get year, month, day etc from date or timestamp using functions. There are functions such as day, dayofmonth, month, weekofyear, year etc available for us.

DESCRIBE FUNCTION day;
DESCRIBE FUNCTION dayofmonth;
DESCRIBE FUNCTION month;
DESCRIBE FUNCTION weekofyear;
DESCRIBE FUNCTION year;

# Let us see the usage of the functions such as day, dayofmonth, month, weekofyear, year etc.

SELECT year(current_date);
SELECT month(current_date);
SELECT weekofyear(current_date);
SELECT day(current_date);
SELECT dayofmonth(current_date);

# We can unix epoch in Unix/Linux terminal using date '+%s'
# retorna o numero de segundos entre a data informada e o tempo do unix o tem;po unix é 1970-01-01 '00:00:00';

Select unix_timestamp('31-03-31 00:00:00')
SELECT from_unixtime(1556662731);
SELECT to_unix_timestamp('2019-04-30 18:18:51');

SELECT from_unixtime(1556662731, 'YYYYMM');
SELECT from_unixtime(1556662731, 'YYYY-MM-dd');
SELECT from_unixtime(1556662731, 'YYYY-MM-dd HH:mm:ss');

SELECT to_unix_timestamp('20190430 18:18:51', 'YYYYMMdd');
SELECT to_unix_timestamp('20190430 18:18:51', 'YYYYMMdd HH:mm:ss');

# Here are some of the numeric functions we might use quite often.

abs
sum, avg
round
ceil, floor
greatest, min, max
rand
pow, sqrt
cumedist, stddev, variance

# Some of the functions highlighted are aggregate functions, eg: sum, avg etc.

SELECT avg(order_item_subtotal) FROM order_items
WHERE order_item_order_id = 2;
SELECT round(avg(order_item_subtotal), 2) FROM order_items
WHERE order_item_order_id = 2;

# Let us understand how we can type cast to change the data type of extracted value to its original type.

SELECT current_date;
SELECT split(current_date, '-')[1];
SELECT cast(split(current_date, '-')[1] AS INT);
SELECT cast('0.04' AS FLOAT);
SELECT cast('0.04' AS INT); //0


# A função nvl retorna o valor padrão se o valor for nulo, caso contrário, retorna o valor

select nvl(coluna, 100) from tabela
# Se o valor da coluna for 0, ele retornará 100, se não retornará o valor da coluna


select supplier_id, nvl(suplier_desc, suplier_name) from supliers; 
# Procura o primeiro argumento, se for NULL, seleciona o segundo argumento


SELECT 1 + NULL;

SELECT nvl(1, 0);
SELECT nvl(NULL, 0);

CREATE TABLE wordcount (s STRING);

INSERT INTO wordcount VALUES
  ('Hello World'),
  ('How are you'),
  ('Let us perform the word count'),
  ('The definition of word count is'),
  ('to get the count of each word from this data');

# Now let us develop the logic to get the word count.

# Split the lines into array of words
# Explode them into records

SELECT split(s, ' ') FROM wordcount;
SELECT explode(split(s, ' ')) FROM wordcount;

# Let us come up with the query to get word count.

# We need to use nested subquery to get the count.


SELECT word, count(1) FROM (
  SELECT explode(split(s, ' ')) AS word FROM wordcount
) q  
# Queries in q are shorter and simpler and extend the capabilities of sql. The main query expression is the ‘select expression’, which in its simplest form extracts sub-tables but it can also create new columns.
GROUP BY word;
--------------------------------------------------------------------------------------------------------------------------
SELECT FROM WHERE GROUP BY HAVING ORDER BY
# Tem que ser nessa ordem

SELECT - Campos do projeto ou campos derivados
FROM - Especifica as tabelas
JOIN - Junte vários conjuntos de dados. Também pode ser OUTER
WHERE - Filtra dados
GROUP BY [HAVING] - Para operações de grupo, como agregações
ORDER BY - Para ordenar os dados.

# Execution life cycle of a query.

SELECT order_item_order_id, sum(order_item_subtotal) AS order_revenue
FROM order_items
GROUP BY order_item_subtotal
LIMIT 10;

# Once the query is submitted, this is what happens.
# -Syntax and Semantecs Check
# -Compile Hive query into Map Reduce application
# -Submit Map Reduce Application as one or more Job
# -Execute all the associated Map Reduce Jobs
--------------------------------------------------------------------------------------------------------------------------

# Logs for Hive Queries

# -Once the query is submitted details will be logged to /tmp/OS_USER_NAME/hive.log by default.
# -As Hive will be running as Map Reduce jobs, we can actually go through Map Reduce Job logs using Job History Server UI.
# -We can also go to Job Configuration and get more details about run time details of associated Map Reduce jobs.
# -Running Query
# -Number of reducers
# -Input Directory
# -and any other run time property for the underlying Map Reduce jobs.
--------------------------------------------------------------------------------------------------------------------------

# PERFORMING BASIC AGGREGATIONS

# Let us see how we can use aggregate functions such as count, sum, min, max etc.

# Here are few examples with respect to count.

SELECT count(1) FROM orders;
SELECT count(DISTINCT order_date) FROM orders;
SELECT count(DISTINCT order_date, order_status) 
FROM orders;

# Let us see functions such as min, max, avg etc in action.

SELECT * FROM order_items WHERE order_item_order_id = 2;

SELECT sum(order_item_subtotal) 
FROM order_items 
WHERE order_item_order_id = 2;

SELECT sum(order_item_subtotal),
  min(order_item_subtotal),
  max(order_item_subtotal),
  avg(order_item_subtotal)
FROM order_items 
WHERE order_item_order_id = 2;
--------------------------------------------------------------------------------------------------------------------------
# Let us get an overview of GROUP BY and perform basic aggregations such as SUM, MIN, MAX etc using GROUP BY…

# It is primarily used for performing aggregate type of operations based on a key.
# When GROUP BY is used, SELECT clause can only have those columns/expressions specified in GROUP BY clause and then aggregate functions.
# We need to have key defined in GROUP BY Clause.
# Same key should be specified in SELECT clause along with aggregate function.
# Let us see aggregations using GROUP BY in action.

SELECT * FROM order_items LIMIT 10;

SELECT order_item_order_id,
  sum(order_item_subtotal) AS order_revenue,
  min(order_item_subtotal) AS min_order_item_subtotal,
  max(order_item_subtotal) AS max_order_item_subtotal,
  avg(order_item_subtotal) AS avg_order_item_subtotal,
  count(order_item_subtotal) AS cnt_order_item_subtotal
FROM order_items
GROUP BY order_item_order_id;

--------------------------------------------------------------------------------------------------------------------------
# FILTERING POST AGGREGATION USING HAVING

# Let us see how we can filter the data based on aggregated results using HAVING Clause.

# Having clause can be used only with GROUP BY
# We need to use function as part of HAVING clause to filter the data based on aggregated results.


SELECT order_item_order_id,
  sum(order_item_subtotal) AS order_revenue
FROM order_items
GROUP BY order_item_order_id
HAVING sum(order_item_subtotal) >= 500;

# GLOBAL SORTING USING ORDER BY

# Let us see how we can sort the data globally using ORDER BY.

# When we sort the data using ORDER BY, only one reducer will be used for sorting.
# We can sort the data either in ascending or descending order.
# By default data will be sorted in ascending order.
# We can sort the data using multiple columns.
# If we have sort the data in descending order on a particular column, then we need to specify DESC on that column.
# Let us see few examples of using ORDER BY to sort the data.


SELECT * FROM orders
ORDER BY order_customer_id
LIMIT 10;

SELECT * FROM orders
ORDER BY order_customer_id, order_date
LIMIT 10;

SELECT * FROM orders
ORDER BY order_customer_id ASC, order_date DESC
LIMIT 10;

# SORTING DATA WITH IN GROUPS USING DISTRIBUTED BY AND SORT BY

# Let us create database training_nyse if it is not already existing and then create table stocks_eod

CREATE DATABASE IF NOT EXISTS training_nyse;
USE training_nyse;

CREATE TABLE stocks_eod (
  stockticker STRING,
  tradedate INT,
  openprice FLOAT,
  highprice FLOAT,
  lowprice FLOAT,
  closeprice FLOAT,
  VOLUME BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/data/nyse_all/nyse_data'
INTO TABLE stocks_eod;

# Let us try creating new table stocks_eod_orderby using stocks_eod data sorting by tradedate and then volume descending.

# -Create table stocks_eod_orderby
# -Set number of reducers to 8
# -Insert into stocks_eod_orderby from stocks_eod using ORDER BY tradedate, volume DESC
# -Even though number of reducers are set to 8, it will use only 1 reducer as we have ORDER BY clause in our query.
# Here are the commands to create table stocks_eod_orderby and insert data into the table.


CREATE TABLE stocks_eod_orderby (
  stockticker STRING,
  tradedate INT,
  openprice FLOAT,
  highprice FLOAT,
  lowprice FLOAT,
  closeprice FLOAT,
  VOLUME BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

SET mapreduce.job.reduces=8;

INSERT INTO stocks_eod_orderby
SELECT * FROM orders
ORDER BY tradedate, volume DESC;
Now let us create the table stocks_eod_sortby where data is grouped and sorted with in each date by volume in descending order.

# -Create table stocks_eod_sortby
# -Insert into stocks_eod_sortrby from stocks_eod using DISTRIBUTE BY tradedate SORT BY tradedate, volume DESC
# -Now data will be inserted into stocks_eod_sortby using 8 reducers.
# -Data will be distributed/grouped by tradedate and with in each tradedate data will be sorted by volume in descending order.
# -Data need not be globally sorted on the tradedate.
# Here are the commands to create table stocks_eod_sortby.

CREATE TABLE stocks_eod_sortby (
  stockticker STRING,
  tradedate INT,
  openprice FLOAT,
  highprice FLOAT,
  lowprice FLOAT,
  closeprice FLOAT,
  VOLUME BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

# Let us take care of inserting the data using DISTRIBUTE BY and SORT BY.

SET mapreduce.job.reduces=8;

INSERT INTO stocks_eod_orderby
SELECT * FROM orders
DISTRIBUTE BY tradedate
SORT BY tradedate, volume DESC;

# Now we can look into the location of the table and we will see 8 files as 8 reducers are used.
--------------------------------------------------------------------------------------------------------------------------

# OVERVIEW OF NESTED SUBQUERIES

SELECT * FROM ( SELECT * FROM orders ) LIMIT 10;
# não vai dar certo
# fazer assim :

SELECT * FROM ( SELECT * FROM orders ) q LIMIT 10;

# JOIN

SELECT t1.*, t2.* FROM table 1 t1  [OUTER] JOIN table 2 t2
ON t1.id = t2.id
WHERE filters;

# MAIS DE UM JOIN

SELECT o.order_id,o.order_date,o.order_status,
       oi.order_item.product_id,oi.order_item_subtotal,
       # colunas
       ON o.order_id = oi.order_item_order_id
       # Relaciona a coluna de uma tabela diretamente com a coluna da outra
       LIMIT 10;


SELECT * FROM
table1 t1 JOIN table 2 t2
ON t1.key = t2.related_key
JOIN table 3 ON t3.key = t2.related_t3_key

# Dessa maneira é possível fazer o join de múltiplas tabelas
-------------------------------------------------------------------------------------------------
# No T-SQL
# A palavra JOIN é usada para obter dados provenientes de duas ou mais tabelas, baseado em um relacionamento entre colunas nestas tabelas.

# INNER JOIN: retorna linhas quando houver pelo menos uma correspondência em ambas as tabelas.
# OUTER JOIN: retorna linhas mesmo quando não houver pelo menos uma correspondência em uma das tabelas (ou ambas). O OUTER JOIN divide-se em LEFT JOIN, RIGHT JOIN e FULL JOIN.
# Formato:

SELECT colunas
FROM tabela 1
INNER JOIN tabela 2
ON tabela1.coluna = tabela2.coluna

# Exs: 

SELECT * FROM  tbl_Livro
INNER JOIN tbl_autores
ON tbl_Livro.ID_Autor = tbl_autores.ID_Autor


SELECT tbl_Livro.Nome_Livro,tbl_Livro.ISBN,tbl_autores.Nome_Autor
FROM tbl_Livro
INNER JOIN tbl_autores
ON tbl_Livro.ID_Autor = tbl_autores.ID_Autor

# Ou usando aliases

SELECT L.Nome_Livro, E.Nome_editora
FROM tbl_Livro AS L
INNER JOIN tbl_editoras AS E
ON L.ID_Editora = E.ID_editora

# OUTER JOINS

# LEFT JOIN: Retorna todas as linhas da tabela à esquerda, mesmo se não houver nenhuma correspondência na tabela à direita.
# RIGHT JOIN: Retorna todas as linhas da tabela à direita, mesmo se não houver nenhuma correspondência na tabela à esquerda.
# FULL JOIN: Retorna linhas quando houver uma correspondência em qualquer uma das tabelas. É uma combinação de LEFT e RIGHT JOINS.

# LEFT JOIN

SELECT coluna
FROM tabela_esq
LEFT (OUTER) JOIN tabela_dir
ON tabela_esq.coluna = tabela_dir.coluna

# Ex:

SELECT * FROM tbl_autores
LEFT JOIN tbl_Livro
ON tbl_Livro.ID_Autor = tbl_autores.ID_Autor

# LEFT JOIN excluindo correspondências

SELECT coluna
FROM tabela_esq
LEFT (OUTER)JOIN tabela_dir
ON tabela_esq.coluna=tabela_dir.coluna
WHERE tabela_dir.coluna IS NULL

# Ex:

SELECT * FROM tbl_autores
LEFT JOIN tbl_Livro
ON tbl_Livro.ID_Autor = tbl_autores.ID_Autor
WHERE tbl_Livro.ID_Autor IS NULL

# RIGHT JOIN

SELECT colunas
FROM tabela_esq
RIGHT (OUTER) JOIN tabela_dir
ON tabela_esq.coluna=tabela_dir.coluna

# Ex:

SELECT * FROM tbl_Livro AS Li
RIGHT JOIN tbl_editoras AS Ed
ON Li.ID_editora = Ed.ID_editora

# RIGHT JOIN excluindo correspondências

SELECT coluna
FROM tabela_esq
RIGHT(OUTER)JOIN tabela_dir
ON tabela_esq.coluna=tabela_dir.coluna
WHERE tabela_esq.coluna IS NULL

# Ex:

SELECT * FROM tbl_Livro
RIGHT JOIN tbl_editoras
ON tbl_Livro.ID_editora = tbl_editora.ID_editora
WHERE tbl_Livro.ID_editora IS NULL

# FULL JOIN
 
 Combinação de RIGHT JOIN com LEFT JOIN, retornando registros que não possuam correspondências em ambas as tabelas.

 SELECT colunas
 FROM tabela 1
 FULL(OUTER)JOIN tabela 2
 ON tabela1.coluna = tabela2.coluna
 
# Ex:

SELECT Li.Nome_Livro, Li.ID_autor, Au.Nome_autor
FROM tbl_Livro AS Li
FULL JOIN tbl_autores AS Au
ON Li.ID_autor = Au.ID_autor

SELECT colunas
FROM tabela 1
FULL(OUTER)JOIN tabela 2
ON tabela1.coluna=tabela2.coluna
WHERE tabela1.coluna IS NULL
OR tabela2.coluna IS NULL

=================================================================================================

# SQOOP

Como o MySQL é normalmente usado para aprender Sqoop, vamos validar o banco de dados MySQL.
Podemos seguir os mesmos passos para qualquer outro banco de dados que funcionará como fonte.
Conecte-se ao servidor MySQL usando mysql -u retail_user -h ms.itversity.com -p. Digite a senha quando solicitado.
Podemos listar bancos de dados usando o comando SHOW databases.
retail_user tem permissões somente leitura em retail_db e permissões de leitura e gravação em retail_export.
Todos os usuários no servidor MySQL terão permissões somente leitura em information_schema.
Aqui estão alguns dos comandos padrão que podem ser usados ​​no MySQL.

Switch Database - USE retail_db;
Tabelas de lista - tabelas SHOW;
Dados de visualização - SELECT * FROM pedidos LIMIT 10;
Temos outros bancos de dados junto com os usuários também em nosso servidor MySQL.

NYSE
RH

# Apache Sqoop é escrito em Java e usa JDBC para se conectar ao banco de dados.

Como o Apache Sqoop usa Java, é importante para nós ter um arquivo jar JDBC para o banco de dados subjacente como parte do Sqoop Classpath.
Sqoop Classpath em nosso ambiente - /usr/hdp/2.6.5.0-292/sqoop/lib
Jar FileName - mysql-connector-java.jar

sqoop  help
# Mostra a lista de comandos

sqoop command --help
# Descreve um comando em particular, que poderia ser 'import', por exemplo.

sqoop list-databases \
  --connect "jdbc:mysql://ms.itversity.com:3306" \
  --username retail_user \
  --password itversity
# Estes são os comandos para conectar a base de dados do MySQL sobre JDBC e listar os bancos de dados.


sqoop list-tables \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity
# Vamos ver como listar tabelas usando o Sqoop em uma configuração de banco de dados no MySQL.
# Para listar tabelas, precisamos especificar o nome do banco de dados como parte do JDBC URI.
# Ele listará as tabelas existentes no MySQL Server subjacente no caso do MySQL.


sqoop eval \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --query "SELECT * FROM orders LIMIT 10"

sqoop eval \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --query "DESCRIBE orders"
  # Pode ser passado por -e ou -query
  # Usado para descrever tabelas,visualizar dados usando SELECT e invocar procedimentos armazenados.
-------------------------------------------------------------------------------------------------
# Vamos entender detalhes sobre logs no sqoop. Existem dois tipos de logs.

# sqoop logs - que vemos depois de executar um comando sqoop.
# Se executarmos comandos como importação sqoop, exportação sqoop etc, que acionam o mapa reduzir os empregos - devemos rever os registros de trabalho indo para a Interface do Usuário, como o Job History Server.
# Podemos revisar o mapa, reduzir os registros de emprego usando a URL de rastreamento gerada quando executamos comandos como sqoop import, sqoop export,etc.





sqoop eval \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --query "SELECT * FROM orders LIMIT 10" 1>query.out 2>query.err
# Redireciona Sqoop Logs dentro de arquivos.
-------------------------------------------------------------------------------------------------

sqoop import
# Comando mais importante do Sqoop
# Usamos esse comando para copiar dados do RDBMS para o Hive.
# Copiaremos os dados usando um trabalho de Map Reduce
# Submete um trabalho de Map Reduce no cluster.
# Existem várias categorias de argumentos que podem ser usados durante a realização da importação de sqoop

# Aqui importamos os dados de retail_db.orders para /user/training/sqoop_import/retail_db/orders

sqoop import \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --table orders \
  --target-dir /user/training/sqoop_import/retail_db/orders  # 'target-dir' especifica a localização
  -----------------------------------------------------------------------------------------------

  sqoop import \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/training/sqoop_import/retail_db
# Aqui, importamos retail_db.orders para /user/training/sqoop_import/retail_db usando o warehouse-dir.

-------------------------------------------------------------------------------------------------
# Neste exemplo para sqoop import aonde dados serão acrescentados na locaização HDFS especificadas como parte do target-dir ou warhouse-dir.

sqoop import \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/training/sqoop_import/retail_db \
  --append
# Aqui stá um exemplo de sqoop import aonde os dados serão sobrescritos dentro da localização HDFS especificada como parte do target-dir ou warhouse-dir.

sqoop import \
  --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/training/sqoop_import/retail_db \
  --delete-target-dir
  ==============================================================================================
  
# Preparando dados para usar 'export'

# Aqui vamos preparar dados para 'export' em tabelas de bases de dados RELACIONAIS
# Aqui usaremos o Hive para fazer uma consulta
# Uma vez que a tabela é criada, podemos usar o local HDFS usado pela tabela Hive para 'export sqoop'


CREATE TABLE training_sqoop_retail.daily_revenue (
  order_date STRING,
  revenue FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT INTO daily_revenue
SELECT order_date, round(sum(order_item_subtotal), 2) AS revenue
FROM orders JOIN order_items 
ON order_id = order_item_order_id
WHERE order_status IN ('COMPLETE', 'CLOSED')
GROUP BY order_date;
===============================================================================================
CREATE TABLE work_dataeng.pokemon_test (
idnum INT,
name STRING,
hp INT,
speed INT,
attack INT,
special_attack INT,
defense INT,
special_defense INT,
generation INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
tblproperties ("skip.header.line.count"="1")
STORED AS TEXTFILE;

