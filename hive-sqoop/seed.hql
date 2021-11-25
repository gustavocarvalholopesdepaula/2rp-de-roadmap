
git checkout -b hive-sqoop

# Cria a branch ' hive-sqoop ' e a coloca em uso simultaneamente
# No Visual Studio Code, ir em ' 2rp-de-roadmap, que é nosso repositório, selecionar 'New Folder' e chamar de 'hive-sqoop'.


# Dentro dessa subpasta, selecionar ' New File ' e nomear de ' cria-tabelas.hql
# Fazer o login no HUE e na base de dados work_dataeng, criar as 2 tabelas no formato ORC, seguem os scripts:

CREATE TABLE work_dataeng.generation_gustavo (
generation INT,
date_introduced DATE
) STORED as orc; 



CREATE TABLE work_dataeng.pokemon_gustavo ( 
idnum INT,
name STRING,
hp INT,
speed INT,
attack INT,
special_attack INT,
defense INT,
special_defense INT,
generation INT
) STORED AS orc;

# Para visualizar os resultados, poderíamos fazer :

SHOW CREATE TABLE work_dataeng.generation_gustavo;

SHOW CREATE TABLE work_dataeng.pokemon_gustavo;

# Agora chegou a hora de inserir os dados nas tabelas

# Os dados necessários podem ser encontrados no repositório 'squad-de-roadmap' dentro da pasta 'materiais-complementares/nivel-1
# No caso da tabela work_dataeng.generation_gustavo, como são poucos dados, podemos inseri-los manualmente, da seguinte maneira:

INSERT INTO TABLE work_dataeng.generation_gustavo VALUES
(1,'1996-02-27'),
(2,'1999-11-21'),
(3,'2002-11-21'),
(4,'2006-09-28'),
(5,'2010-09-18'),
(6,'2010-12-13'),
(7,'2016-11-18');

# Truncando a tabela e inserindo os dados novamente, podemos utilizar o comando SELELCT * FROM e visualizar os dados da tabela

TRUNCATE TABLE work_dataeng.generation_gustavo 


SELECT * FROM work_dataeng.generation_gustavo 
# Primeira tabela finalizada

# Agora vamos inserir os dados na segunda tabela
# O primeiro é criar uma tabela externa temporária no formato CSV. Isso pode ser feito da seguinte maneira:


CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS work_dataeng.pokemon_gustavo(
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
STORED AS TEXTFILE;

# Usando o comando SHOW CREATE TABLE podemos visualizar nossa tabela, que ainda não tem dados.

 SHOW CREATE TABLE work-dataeng.pokemon_gustavo_temp

# Agora, vamos enviar o arquivo work_dataeng.pokemon_gustavo para o cluster, para então conseguir inserir os dados na tabela.
# Feito isso, executaremos um join entre as duas tabelas no Hive e no Impala e compararemos o tempo de execução.
# Agora, podemos ir no 'git bash', usar os comandos necessários para comitar a branch hive-sqoop.

git add --all
git commit -m " hive-sqoop "
git push -u origin hive-sqoop

# No Git Hub, fazemos o pull request e o merge.
# Tarefa finalizada.










