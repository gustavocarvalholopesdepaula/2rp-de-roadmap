Hadoop é uma plataforma de software em Java de computação distribuída voltada para clusters e processamento de grande volume de dados, com atenção a tolerância a falhas.

Cluster é um grupo de servidores que funciona como sistema único.

O ecossistema Hadoop é formado por HDFS, Map Reduce e outros componentes.
Para salvar um arquivo em HDFS, o Hadoop divide o arquivo em blocos, de 64 ou 128Mb e armazena cada bloco em um data node. Em seguida, replica o bloco por 3, para haver cópias em outros servidores. Por fim, reconhece se um bloco de dados não foi replicado e o replica normalmente.

Alguns comandos:

$ vagrant ssh pocd-cm581-dev-node/
# Importa o conteúdo do servidor para o computador. Neste caso, o usuário é 'vagrant' e o servidor é ' pocd-cm581-dev-node/ '

$ hostname 
# Mostra o endereço eletrônico do servidor

$ df
# Mostra o espaço usado/disponível de todo o sistema.

$ df -h
# Mostra o espaço usado/disponível em Mb/Gb/Kb ao invés de bloco.


$ view part 00000
# Abri o arquivo part 00000

$ wc-l part 00000
# Conta as linhas do arquivo

$ du
# Fornece a quantidade de espaço ocupada por cada subdiretório que se encontra abaixo do diretório atual.

$ hdfs dfs 
# Lista todos os comandos que podem ser utilizados para o cluster.

$ hdfs dfs -df -h
# Mostra a capacidade em Mb, Kb, Gb dentro do cluster.

$ hdfs dfs -help df
# Mostra o que faz o comando 'df' no cluster.

$ hdfs dfs -mkdir
# Cria um diretório chamado 'user' dentro do cluster

$ sudo -u hdfs
# O comando sudo -u hdfs é utilizado quando aparece a mensagem :'Superuser privilege is required'

$ hdfs dfs -chown vagrant: vagrant/user/vagrant
# Muda o dono do cluster para 'vagrant'

$ hdfs dfs -mkdir
# Cria um diretório dentro do cluster.

$  hdfs dfs -put 
# Permite copiar arquivos do ambiente local para o ambiente remoto. 
# Permite vários arquivos dentro de um só.

$ hdfs dfs tail 
# O comando tail Linux exibe a última parte – por padrão, são 10 linhas que aparecem ao final – de um ou mais arquivos ou dados de saída no prompt Linux. Também pode ser usado para monitorar as alterações do arquivo em tempo real.

$ hdfs fsck 
# Descrição. O comando fsck verifica e repara interativamente os sistemas de arquivos inconsistentes. Normalmente, o sistema de arquivos está consistente e o comando fsck apenas reporta o número de arquivos, os blocos utilizados e os blocos livres no sistema de arquivos. Aqui podem ser acrecentados depois do arquivo comando cmom '-files', '-blocks', '-locations' para mais detalhes do arquivo.

$ hdfs dfs -rm 
# Envia o arquivo do cluster para lixeira

$ hdfs dfs -expunge 
# Resgata o arquivo da lixeira.

$ hdfs dfs -rm -skipTrash
# Remove o arquivo permanentemente

$ sudo vi arquivo.txt
# Cria o 'arquivo.txt' dentro do diretório
# Para finalizar a edição do arquivo usar ':wq'

$ hdfs dfs -appendToFile 
# Acrescenta o conteúdo de todos os arquivos dados para o arquivo dst

$ hdfs cat
# comando cat é mais comumente usado para exibir o conteúdo de um ou vários arquivos de texto, combinar arquivos anexando o conteúdo de um arquivo ao final de outro arquivo e criar novos.

$ hdfs dfs find . -name "2rp.txt"
# Encontra o arquivo "2rp.txt" dentro do cluster.

$ hdfs dfs -get
# Copia arquivos do cluster para o computador.

$ hdfs dfs getmerge
# Obtem todos os arquivos no diretório que correspondem ao padrão de arquivo de origem, mescla e classifica para apenas um arquivo local.

$ hdfs dfs count
# Conta quantos diretórios, arquivos e bytes que correspondem ao padrão do arquivo especificado.

$ hdfs dfs cp nomedoarq.txt/ endereço do diretório
# Faz uma cópia do arquivo.

$ hdfs dfs mv nomedoarq.txt / nomedoarqnovo.txt
# Move arquivos, pode ser usado para renomear um arquivo.

$ hdfs dfs -touchz
# Cria um arquivo vazio. O comando 'hdfs dfs -appendToFile' é muito útil para adicionar informações a este arquivo.

# HADOOP

hadoop fs or hdfs dfs 
# list all the commands available

hadoop fs -usage 
# will give us basic usage for given command

hadoop fs -help 
# will give us additional information for all the commands. It is same as just running hadoop fs or hdfs dfs.
# We can run help on individual commands as well - example: hadoop fs -help ls or hdfs dfs -help ls

hdfs dfs -ls /public/nyse_all/nyse_data # Lista os arquivos da pasta passada.

ls -lhtr /data/crime/csv
# Lista os arquivos presenres na pasta passada e os seus tamanhos.

hdfs dfs -ls -r /public/nyse_all/nyse_data

# We can sort the files and directories by time using -t option. 
# By default you will see latest files at top. We can reverse it by using -t -r.

hdfs dfs -ls -t /public/nyse_all/nyse_data

hdfs dfs -ls -t -r /public/nyse_all/nyse_data

# We can sort the files and directories by size using -S. By default, the files will be sorted in descending order by size.
# We can reverse the sorting order using -S -r.

hdfs dfs -ls -S /public/nyse_all/nyse_data

hdfs dfs -ls -S -r /public/nyse_all/nyse_data

hdfs dfs -ls -S -r /public/nyse_all/nyse_data

hdfs dfs -ls -h -t /public/nyse_all/nyse_data

hdfs dfs -ls -h -S /public/nyse_all/nyse_data

hdfs dfs -ls -h -S /public/nyse_all/nyse_data

hadoop fs -mkdir or hdfs dfs -mkdir 
# to create directories
# You can create the directory structure using mkdir -p. 
# The existing folders will be ignored and non existing folders will be created.

hadoop fs -chown or hdfs dfs -chown 
# to change ownership of files
# We can change the group using -chgrp command as well.


hdfs dfs -rmdir /user/${USER}/retail_db/orders/year=2020
# 'hdfs dfs -rmdir' deleta um diretório vazio

hdfs dfs -rm -R
# Deleta um diretório que não está vazio

hdfs dfs -rm -R -skipTrash /user/${USER}/crime
# Deleta /user/itversity/crime da lixeira.

hdfs dfs -copyFromLocal or hdfs dfs -put 
# to copy files or directories from local filesystem into HDFS.
# We can also use hadoop fs in place of hdfs dfs

hdfs dfs -copyToLocal or hdfs dfs -get 
# to copy files or directories from HDFS to local filesystem.

hdfs fsck retail_db
# We can get high level overview for a retail_db folder by using .
# hdfs fsck get the metadata for the files stored in HDFS

hdfs dfs -tail
# can be used to preview last 1 KB of the file

hdfs dfs -cat 
# can be used to print the whole contents of the file on the screen.
# Be careful while using -cat as it will take a while for even medium sized files.

hdfs fsck /public/yelp-dataset-json/yelp_academic_dataset_user.json \
    -files \
    -blocks \
    -locations
# Esse comando vai dar detalhes da pasta passada, incluindo arquivos, blocos e localização

hdfs dfs -stat
# Print statistics about the file/directory at <path> in the specified format. 
# Format accepts filesize in blocks (%b), type (%F), group name of owner (%g),
# name (%n), block size (%o), replication (%r), user name of owner (%u), modification date (%y, %Y).
#  %y shows UTC date as "yyyy-MM-dd HH:mm:ss" and %Y shows milliseconds since January 1, 1970 UTC.
# If the format is not specified, %y is used by default.

hdfs dfs -df
# to get the current capacity and usage of HDFS.

hdfs dfs -du
# to get the size occupied by a file or folder.