# Databricks notebook source
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
df.display()

# COMMAND ----------

# DBTITLE 1,approx_count_distinct_Agrregate Function
approx = df.select(F.approx_count_distinct("salario")).collect()[0][0]
print(f"approx_count_distinct: {approx}")

# COMMAND ----------

# DBTITLE 1,AVG Aggregate Function
avg = df.select(F.avg("salario")).collect()[0][0]
print(f"avg: {avg}")

# COMMAND ----------

# DBTITLE 1,collect_list Aggregate Function
df.select(F.collect_list("salario")).show(truncate=False)

# COMMAND ----------

df.select(F.collect_set("salario")).show(truncate=False)

# COMMAND ----------

df.select(F.collect_set("salario")).show(truncate=True)

# COMMAND ----------

# DBTITLE 1,CountDistinct Agrregate Funciton
df2 =df.select(F.countDistinct(F.col("departamento"),F.col("salario")))
df2.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,First Function
df.select(F.first("salario")).show(truncate=False)


# COMMAND ----------

# DBTITLE 1,MAX
df.select(F.max(F.col("salario"))).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,MIN
df.select(F.min(F.col("salario"))).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,SUM
df.select(F.sum(F.col("salario"))).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Collect()[0][0]
df.select(F.count("salario")).show(truncate=False)

valor= df.select(F.count("salario")).collect()[0][0]
print(valor)
