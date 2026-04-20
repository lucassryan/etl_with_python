# Pipeline ETL com Python
Projeto de construção de um pipeline ETL (Extract, Transform, Load) utilizando Python para tratamento e padronização de dados de corridas de rua. O dataset foi gerado por I.A (Chat GPT).

---

## Objetivo
Afim de continuar na jornada Engenheiro de Dados, o projeto tem como objetivo simular um cenário real de engenharia de dados, onde dados brutos apresentam inconsistências e precisam ser tratados antes de serem utilizados pelos analistas de dados.

---

## Tecnologias utilizadas
* Python
* Biblioteca `csv`
* Manipulação de dados com estruturas nativas (listas e dicionários)

---

## Etapas do Pipeline

### 1. Extract (Extração)
Leitura de um arquivo CSV contendo dados brutos de corredores.

### 2. Transform (Transformação)
Tratamento e padronização dos dados:

* Idade: valores inválidos tratados como `None`
* Gênero: padronizado para `Masculino` e `Feminino`
* Tempo de prova: convertido para o formato `HH:MM:SS`
* Cidade e estado: padronizados para letras maiúsculas
* Distância: padronizada para o formato `XXkm`
* Data do evento: convertida para o formato `YYYY-MM-DD`

### 3. Load (Carga)
Os dados tratados são salvos em um novo arquivo CSV pronto para análise.

---

## Resultados
O pipeline gera um novo arquivo CSV com dados limpos e padronizados, prontos para análise ou integração com ferramentas de BI.

---

## Aprendizados
* Construção de pipeline ETL do zero
* Tratamento de dados inconsistentes
* Manipulação de arquivos CSV em Python
* Boas práticas de organização de código

---

## Autor
Lucas Ryan
Projeto desenvolvido para estudo de Engenharia de Dados