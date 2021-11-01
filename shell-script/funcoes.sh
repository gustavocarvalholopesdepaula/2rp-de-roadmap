#!/bin/bash

lista_arquivos(){

    LISTA=(`find $1 -type f`)

}

insere_texto(){

    echo $1 >> $2

}


