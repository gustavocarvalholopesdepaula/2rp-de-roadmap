# pokemon_gustavo

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

SELECT * FROM work_dataeng.pokemon_gustavo


CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS work_dataeng.pokemon_gustavo_temp (
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

SHOW CREATE TABLE work_dataeng.pokemon_gustavo_temp

SELECT * FROM work_dataeng.pokemon_gustavo_temp




# generation_gustavo


CREATE TABLE work_dataeng.generation_gustavo (
generation INT,
date_introduced DATE
) STORED as orc; 

INSERT INTO TABLE work_dataeng.generation_gustavo VALUES
(1,'1996-02-27'),
(2,'1999-11-21'),
(3,'2002-11-21'),
(4,'2006-09-28'),
(5,'2010-09-18'),
(6,'2010-12-13'),
(7,'2016-11-18');

TRUNCATE TABLE work_dataeng.generation_gustavo 


SELECT * FROM work_dataeng.generation_gustavo 


