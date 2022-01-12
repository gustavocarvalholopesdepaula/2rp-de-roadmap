git --version
# Verifica se o git foi instalado corretamente

git config --global user.name "Gustavo"
# Configura o nome do usuário

git config --global user.email "gustavo.lopes@2rpnet.com"
# Configura o e-mail do usuário

git config --list
# Lista as configurações realizadas

clear
# Apaga a tela

cd name
# Navega até o arquivo name

cd..
# Sai da pasta

dir
# Lista o conteúdo da pasta

dir name.txt
# Lista o arquivo name.txt

dir -a
# Lista arquivos ocultos

git init
# Inializa o repositório

git status
# Mostra o estado do arquivo
# O arquivo ainda não está rastreado

git add name.txt
# nome.formatodoarquivo
# Faz com que o Git rastreie o arquvivo desejado

git add.
git add -A
git add --all
# Faz com que o Git adicione tudo que está na pasta

git commit -m " Arquivo "
# Adiciona um arquivo para ser rastreado
# Salva uma nova versão no banco de dados local do git

git diff
# Mostra as diferenças que ocorreram no meu repositório

git diff --cached
# Mostra as diferenças na área de preparação

git diff --staged
# Mostra também as configurações

git log
# Lista todos os commits realizados

git log --oneline
# Mostra todas as modificações em uma única linha

git push
# Envia para o servidor

git pull
# Baixa do servidor

git checkout "6 primeiros números do commit"
# Para usar comits anteriores
# Aparacerá a mensagem YOU ARE IN DETACHED HEAD

git checkout master
# Volta para o commit anterior

git checkout name.txt
# nome.formato
# Altera um arquivo específico

git reset --hard
# Para modificar todas as alterações da branch

git clean -f
# Força a exclusão de arquivos que foram adicionados

ren name.txt .gitignore
# Tem que fazer no cmd
# Cria um arquivo sem nome no formato .gitignore

git clone name / nameclone
# Clona o repositório name e renomeia como nameclone

git branch
# Lista os branchs

git branch 2rp
# Cria uma branch chamada 2rp

git checkout 2rp
# Sai da branch atual e entra na branch 2rp

git checkout -b 2rp
# Cria a branch 2rp e simultaneamente a utiliza

git push --set-upstream origin 2rp
git push -u origin 2rp
# Envia a branch 2rp para o GitHub

git branch -d nome-da-branch 
# Deleta uma branch

git branch -D nome-da-branch
# Força a remoção da branch




