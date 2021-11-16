# Databricks notebook source
As Windows Functions retornam um único valor para cada grupo de linhas. O PySpark oferece suporte a 3 tipos de Windows Functions:

Ranking functions
Analytic functions
aggregate functions
Documentação: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.html?highlight=window#pyspark.sql.Window

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dados = [
         ("Anderson", "Vendas", "SP", 1500.00, 34, 1000.00),
         ("Kennedy", "Vendas", "CE", 1200.00, 56, 2000.00),
         ("Bruno", "Vendas", "SP", 1100.00, 30, 2300.00),
         ("Maria", "Finanças", "CE", 3600.00, 24, 2300.00),
         ("Eduardo", "Finanças", "CE", 4500.00, 40, 2400.00),
         ("Mendes", "Finanças", "RS", 8000.00, 36, 1900.00),
         ("Kethlyn", "Finanças", "RS", 1200.00, 53, 1500.00),
         ("Thiago", "Marketing", "GO", 1100.00, 25, 1800.00),
]
schema = ['nome','departamento','estado','salario','idade','bonus']
df=spark.createDataFrame(data=dados,schema=schema)
df.printSchema()
df.show()

# COMMAND ----------

w0 = Window.partitionBy(F.col("departamento")).orderBy("salario")


# COMMAND ----------

# DBTITLE 1,Row Window
#RETORNA O NÚMERO DA LINHA DE ACORDO COM A COLUNA QUE FOI PARTICIONADA
df.withColumn("row_number",F.row_number().over(w0)).display()

# COMMAND ----------

# DBTITLE 1,rank Window Functions
df.withColumn("rank",F.rank().over(w0)).display()

# COMMAND ----------

# DBTITLE 1,dense_rank() Window Function
df.withColumn("dense_rank",F.dense_rank().over(w0)).display()

# COMMAND ----------

# DBTITLE 1,percent_rank Window Functions
df.withColumn("percent_rank",F.percent_rank().over(w0)).display()

# COMMAND ----------

# DBTITLE 1,lag window() Window Funciton
df.withColumn("lag",F.lag("salario",2).over(w0)).display()

# COMMAND ----------

# DBTITLE 1,lead Window Function
df.withColumn("lead",F.lead("salario",2).over(w0)).display()

# COMMAND ----------

# DBTITLE 1,Window Aggregate Functions
(df.withColumn("row",F.row_number().over(w0))
  .withColumn("avg",F.avg(F.col("salario")).over(w0))
  .withColumn("sum",F.sum(F.col("salario")).over(w0))
  .withColumn("max",F.max(F.col("salario")).over(w0))
  .withColumn("min",F.min(F.col("salario")).over(w0))
  .select("row","departamento","avg","sum","min","max").display()
)

# COMMAND ----------


