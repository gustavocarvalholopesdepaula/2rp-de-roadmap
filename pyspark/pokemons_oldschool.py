# LOAD LIBRARIES

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col, lower

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

# ENVIANDO DADOS PARA O TÓPICO

#service_user = '2rp-gustavo'
#topic_name = 'nifi.send.trilha.conhecimento'
#kafka_bootstrap_servers = '192.168.80.8:19093,192.168.80.7:19093,192.168.80.14:19093'

#pokemon_oldschool = spark.read.table("work_dataeng.pokemons_oldschool_2rpgustavo").limit(10)

#insert_kafka = pokemon_oldschool.select(to_json(struct([col(name).alias(name) for name in pokemon_oldschool.columns])).alias("value"))


#options = {
#    "kafka.sasl.jaas.config" : "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/home/{user}/{user}.keytab\" principal=\"{user}@BDACLOUDSERVICE.ORACLE.COM\";".format(user=service_user),
#    "kafka.security.protocol" : "SASL_SSL",
#    "kafka.sasl.kerberos.service.name" : "kafka",
#    "kafka.ssl.truststore.location" : "/opt/cloudera/security/pki/bds.truststore",
#    "kafka.ssl.truststore.password" : "dqmQtyVB6zzjcJbZAi7DIa8LRkM7zVX3",
#    "kafka.bootstrap.servers" : kafka_bootstrap_servers,
#    "topic" : topic_name
#}

#insert_kafka.write.format("kafka").options(**options).save()

# ENVIANDO SOMENTE OS NOMES PARA O TÓPICO

%pyspark
service_user = '2rp-gustavo'
topic_name = 'nifi.send.trilha.conhecimento'
kafka_bootstrap_servers = '192.168.80.8:19093,192.168.80.7:19093,192.168.80.14:19093'

pokemon_oldschool = spark.read.table("work_dataeng.pokemons_oldschool_2rpgustavo").withColumn('name', lower(col('name'))).limit(10)

pokemonName = ['name']

insert_kafka = pokemon_oldschool.select(to_json(struct([col(name).alias(name) for name in pokemonName])).alias("value"))

options = {
    "kafka.sasl.jaas.config" : "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/home/{user}/{user}.keytab\" principal=\"{user}@BDACLOUDSERVICE.ORACLE.COM\";".format(user=service_user),
    "kafka.security.protocol" : "SASL_SSL",
    "kafka.sasl.kerberos.service.name" : "kafka",
    "kafka.ssl.truststore.location" : "/opt/cloudera/security/pki/bds.truststore",
    "kafka.ssl.truststore.password" : "dqmQtyVB6zzjcJbZAi7DIa8LRkM7zVX3",
    "kafka.bootstrap.servers" : kafka_bootstrap_servers,
    "topic" : topic_name 
}

insert_kafka.write.format("kafka").options(**options).save()