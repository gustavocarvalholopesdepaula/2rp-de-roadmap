# LOAD LIBRARIES

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# INICIANDO SPARKSESSION

spark = SparkSession.builder \
    .getOrCreate()

# POKEMON

pokemon = spark.read.table("work_dataeng.pokemon_gustavo")

# GENERATION

generation = spark.read.table("work_dataeng.generation_gustavo")

generation = generation.withColumn("date_introduced", F.to_date(F.col("date_introduced"))) 

# FILTRO/CACHE

generation = generation.where(F.col("date_introduced") < "2002-11-21")
generation = generation.cache()

# INNER JOIN

pokemon_oldschool = generation.join(pokemon,'generation',how='inner')\

# SALVANDO A TABELA

pokemon_oldschool.write.format('orc').mode('overwrite').saveAsTable("work_dataeng.pokemons_oldschool_2rpgustavo")

