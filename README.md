# Pipeline com AWS Glue e PySpark

Este projeto demonstra um pipeline de ETL **serverless** utilizando o **AWS Glue com PySpark**, armazenando dados transformados no **Amazon S3**, particionando e criando tabelas de consulta com **Glue Catalog** e **Amazon Athena**.

---

## Objetivo

Processar dados de e-mails de phishing com um pipeline real usando serviços gerenciados da AWS, explorando boas práticas de transformação, particionamento e consultas SQL otimizadas.

---

## Tecnologias Utilizadas

- **AWS Glue** (Job + Crawler)
- **PySpark**
- **Amazon S3**
- **Glue Data Catalog**
- **Amazon Athena**
- **IAM (AWSGlueServiceRole, AmazonS3FullAccess, AmazonAthenaFullAccess)**

---

## Dataset

- **Fonte**: [Kaggle – Email Phishing Dataset]  
- **Formato Original**: CSV  
- **Nome do arquivo no S3**: `email_phishing_data.csv`

O dataset contém aproximadamente 520.000 e-mails, com as colunas: num_words, num_links, num_stopwords, num_spelling_errors, label

---

## Pipeline ETL

### 1. Upload do dado bruto

O arquivo original `email_phishing_data.csv` foi carregado no seguinte bucket: s3://data-engineer-projects-jota/email_phishing_data.csv

---

### 2. Glue Job – ETL com PySpark

O Glue Job executa as seguintes etapas:

```python
# Leitura do CSV com cabeçalho
df = spark.read.option("header", True).csv("s3://data-engineer-projects-jota/email_phishing_data.csv")

# Limpeza de dados
df_clean = df.dropna()

# Conversão da coluna 'label' para inteiro e renomeação para 'phishing_label'
from pyspark.sql.functions import col
df_clean = df_clean.withColumnRenamed("label", "phishing_label")
df_clean = df_clean.withColumn("phishing_label", col("phishing_label").cast("int"))

# Escrita em formato Parquet particionado
df_clean.write.mode("overwrite").partitionBy("phishing_label").parquet("s3://data-engineer-projects-jota/projeto2/curated/")

---
Glue Crawler
Um crawler foi criado e configurado para:

Apontar para: s3://data-engineer-projects-jota/projeto2/curated/
 Banco de dados: projeto3_jota

Nome da tabela gerada: projeto2

---


Consultas no Athena
Após a execução do crawler, a tabela foi registrada no Glue Catalog e pode ser consultada via Athena:

-- Visualizar os dados
SELECT * FROM projeto3_jota.projeto2 LIMIT 10;

-- Quantidade de e-mails phishing x não phishing
SELECT phishing_label, COUNT(*) AS total
FROM projeto3_jota.projeto2
GROUP BY phishing_label;

---

data-engineer-projects-jota/
├── email_phishing_data.csv             # Dado bruto original
└── projeto2/
    └── curated/
        └── phishing_label=0/
        └── phishing_label=1/
        ...

---

Principais Aprendizados
Utilização do AWS Glue como mecanismo de ETL serverless com PySpark

Leitura de CSV e escrita particionada em Parquet no S3

Criação automatizada de catálogo com Glue Crawler

Consultas otimizadas no Athena com particionamento

Tratamento de tipos, limpeza e validação dos dados

