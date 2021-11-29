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


# generation_gustavo


CREATE TABLE work_dataeng.generation_gustavo (
generation INT,
date_introduced DATE
) STORED as orc; 




