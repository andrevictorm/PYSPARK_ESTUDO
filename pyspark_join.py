# Databricks notebook source
O Pyspark Join é usado para combinar dois DataFrames e, encadeando-os, você pode juntar vários DataFrames;
Ele suporta todas as operações básicas de tipo de junção disponíveis no SQL tradicional, como INNER< LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN.

EXEMPLOS:


inner ==> INNER JOIN SQL
outer, full, fullouter, full_outer ==> FULL OUTER JOIN
left, leftouter, left_outer ==> LEFT JOIN
right, rightouter, right_outer ==> RIGHT JOIN
cross
anti, leftanti, left_anti
semi, leftsemi, left_semi

# COMMAND ----------

import pyspark.sql.functions as F
dados = [
         (1, 'Anderson', 1000.00),
         (2, 'Kennedy', 2000.00),
         (3, 'Bruno', 2300.00),
         (4, 'Maria', 2300.00),
         (5, 'Eduardo', 2400.00),
         (6, 'Mendes', 1900.00),
         (7, 'Kethlyn', 1500.00),
         (8, 'Thiago', 1800.00),
         (9, 'Carla', 2100.00)
]

schema = ["id", "nome", "salario"]
df1 = spark.createDataFrame(data=dados, schema=schema)
df1.printSchema()

# COMMAND ----------

dados2 = [
    (1, "Delhi", "India"),
    (2, "Tamil Nadu", "India"),
    (3, "London", "UK"),
    (4, "Sydney", "Australia"),
    (8, "New York", "USA"),
    (9, "California", "USA"),
    (10, "New Jersey", "USA"),
    (11, "Texas", "USA"),
    (12, "Chicago", "USA")
]
schema2 = ["id", "local", "pais"]
df2 = spark.createDataFrame(data=dados2, schema=schema2)
df2.printSchema()

# COMMAND ----------

# DBTITLE 1,INNER JOIN
df1.display()
df2.display()

# COMMAND ----------

df_inner = df1.join(df2,on=['id'], how='inner')
df_inner.display()

# COMMAND ----------

# DBTITLE 1,OUTER JOIN
#RETORNA TODOS OS DADOS DO DATAFRAME DA COLUNA DA ESQUERDA E DA DIREITA
df_outer = df1.join(df2, on=['id'], how='outer')
df_outer.display()

# COMMAND ----------

# DBTITLE 1,LEFT JOIN
df_outer = df1.join(df2, on=['id'], how='left')
df_outer.display()

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN
df_outer = df1.join(df2, on=['id'], how='right')
df_outer.display()

# COMMAND ----------

# DBTITLE 1,LEFT ANTI JOIN
df_outer = df1.join(df2, on=['id'], how='left_anti')
df_outer.display()

# COMMAND ----------

# DBTITLE 1,LEFT SEMI JOIN
df_outer = df1.join(df2, on=['id'], how='left_semi')
df_outer.display()

# COMMAND ----------

# DBTITLE 1,ANTI JOIN
df_outer = df1.join(df2, on=['id'], how='anti')
df_outer.display()
