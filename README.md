# Curso engenharia de dados

## Descrição

Este projeto é um pipeline de dados local que simula processos de ETL (Extract, Transform, Load) inspirado nos casos práticos do curso de Engenharia de Dados da Udemy.
A ideia surgiu durante a resolução dos exercícios de SQL da Atividade 17, onde decidi não apenas executar as queries, mas também automatizar todo o processo de transformação utilizando Python, criando um fluxo completo de engenharia de dados.

## ❗ Objetivos

- Construção de tabelas no PostgreSQL
- Inserção de dados através dos comandos copy
- Processamento de queries simples
- Distribuição dos dados em formato parquet simulando um S3

## 🛠️ Tecnologias
- Python 3.12
- Pandas
- SQLAlchemy
- PostgreSQL
- Pipenv

## 🔨 Instalação e Configuração
Pré-requisitos

    Python 3.12+

    Pipenv instalado (pip install pipenv)

    PostgreSQL (Necessário instalar e configurar: host, database, user, port, password)

## 🚀 Como executar

- Para executar, basta rodar o arquivo run_pipeline.bat que irá verificar se já existem as tabelas, caso não exista será adicionada e atualizadas com os arquivos csv
        

