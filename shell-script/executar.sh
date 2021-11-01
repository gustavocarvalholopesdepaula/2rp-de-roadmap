#!/bin/bash

source ./funcoes.sh 

declare -a LISTA

lista_arquivos $1 LISTA
for i in $( seq 1 ${#LISTA[@]} )
    do
        insere_texto $2 ${LISTA[i-1]} 
    done



