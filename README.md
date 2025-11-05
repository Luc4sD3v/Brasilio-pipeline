# Projeto: Coleta e Processamento de Dados - Brasil.IO

Este projeto foi desenvolvido como atividade da disciplina de Engenharia de Dados.

## Objetivo
Criar uma pipeline em Python que:
1. Baixa dados da API do [Brasil.IO](https://brasil.io/)
2. Armazena os resultados em formato `.json` (camada *raw*)
3. Converte e particiona os dados em formato `.parquet` (camada *bronze*)

## Estrutura de Pastas

dataset/
├── raw/ → dados brutos (.json)
├── bronze/ → dados processados (.parquet)
├── silver/ → camada intermediária (reservada)
└── gold/ → camada final (reservada)