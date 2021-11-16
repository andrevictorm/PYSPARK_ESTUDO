# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,GROUP BY
CONTEUDO

# COMMAND ----------

import pyspark.sql.functions as F
dados = [
         ('Anderson', 'Vendas', 'SP', 1500.00, 34,1000.00),
         ('Kennedy', 'Vendas', 'CE', 1200.00, 56, 2000.00),
         ('Bruno', 'Vendas', 'SP', 1100.00, 30, 2300.00),
         ('Maria', 'Financas', 'CE', 3600.00, 24, 2300.00),
         ('Eduardo', 'Financas', 'CE', 4500.00,40,2400.00),
         ('Mendes', 'Financas', 'CE', 8000.00, 36,1900.00),
         ('Kethlyn', 'Finanças', 'RS', 1200.00,53,1500.00),
         ('Thiago', 'Marketing', 'GO', 1100.00,25,1800.00),
]
schema= ['nome','departamento','estado','salario','bonus']
df = spark.createDataFrame(data=dados, schema=schema)
df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,SUM() - Mostrar a soma dos salários por departamento
df.groupBy(F.col("departamento")).sum("salario").display()

# COMMAND ----------

# DBTITLE 1,COUNT() - CONTAGEM
df.groupBy(F.col("departamento")).count().display()

# COMMAND ----------

# DBTITLE 1,MIN() - RETORNA O MENOR VALOR DAQUELE AGRUPAMENTO
df.groupBy(F.col("departamento")).min("salario").display()

# COMMAND ----------

# DBTITLE 1,MAX() - RETORNA O MAIOR VALOR DAQUELE AGRUPAMENTO
df.groupBy(F.col("departamento")).max("salario").display()

# COMMAND ----------

# DBTITLE 1,MEAN() - RETORNA A MÉDIA DE UM AGRUPAMENTO
df.groupBy(F.col("departamento")).mean("salario").display()

# COMMAND ----------

df.groupBy(F.col("departamento"),F.col("estado")).sum('salario','bonus').display()

# COMMAND ----------

# DBTITLE 1,AGG - PERMITE AGREGAR MAIS DE UMA FUNÇÃO
(df.groupBy(F.col("departamento")).agg(
            F.sum("salario").alias("soma_salario"),
            F.mean("salario").alias("media_salario"),
            F.avg("salario").alias("media2_salario"),
            F.sum("bonus").alias("bonus_salario"),
            F.max("bonus").alias("maior_bonus"),
            F.min("bonus").alias("menor_bonus")
            ).display()
)

# COMMAND ----------

# DBTITLE 1,WHERE em função agregada
(df.groupBy(F.col("departamento")).agg(
            F.sum("salario").alias("soma_salario"),
            F.mean("salario").alias("media_salario"),
            F.avg("salario").alias("media2_salario"),
            F.sum("bonus").alias("bonus_salario"),
            F.max("bonus").alias("maior_bonus"),
            F.min("bonus").alias("menor_bonus")
            ).where(F.col("soma_salario")>= 3000).display()
)
