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