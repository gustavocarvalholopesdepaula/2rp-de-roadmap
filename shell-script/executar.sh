#!/bin/bash

source $(dirname $0)/funcoes.sh # busca o diretorio aonde está o arquivo

declare -a LISTA

lista_arquivos $1 LISTA
for i in $( seq 1 ${#LISTA[@]} )
    do
        insere_texto $2 ${LISTA[i-1]} 
    done



