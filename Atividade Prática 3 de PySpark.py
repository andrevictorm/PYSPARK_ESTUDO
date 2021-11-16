# Databricks notebook source
df = (spark.read.format("csv")
         .option("inferschema", "true")
         .option("header", "true")
         .option("sep", ";")
         .load("/FileStore/tables/arquivo_geral.csv")
         .createOrReplaceTempView("tabela_covid")
      )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabela_covid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao,
# MAGIC SUM(obitosNovos) as QTD_obitos_TOTAL,
# MAGIC SUM(casosNovos) as QTD_casos_TOTAL
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC SUM(casosNovos) as casos_NOVOS_RJ
# MAGIC FROM tabela_covid
# MAGIC WHERE estado == "RJ"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao,estado FROM tabela_covid
# MAGIC WHERE estado LIKE 'R%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao,estado
# MAGIC FROM tabela_covid
# MAGIC WHERE regiao LIKE "N%" AND (estado LIKE "A%" OR estado LIKE "%O") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(obitosNovos) as novos_obitos_sum,
# MAGIC SUM(casosNovos) as casos_novos_sum,
# MAGIC estado
# MAGIC FROM tabela_covid
# MAGIC WHERE estado IN ('RJ','SP','ES')
# MAGIC GROUP BY estado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(casosAcumulados) as casosAcumulados_sum,
# MAGIC SUM(obitosAcumulados) as obitosAcumulados_sum,
# MAGIC estado
# MAGIC FROM tabela_covid
# MAGIC GROUP BY estado
# MAGIC ORDER BY casosAcumulados_sum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(casosAcumulados) as casosAcumulados_sum,
# MAGIC SUM(obitosAcumulados) as obitosAcumulados_sum,
# MAGIC estado
# MAGIC FROM tabela_covid
# MAGIC GROUP BY estado
# MAGIC ORDER BY obitosAcumulados_sum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(casosAcumulados) as casosAcumulados_sum,
# MAGIC SUM(obitosAcumulados) as obitosAcumulados_sum,
# MAGIC regiao
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao
# MAGIC ORDER BY casosAcumulados_sum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(casosAcumulados) as casosAcumulados_sum,
# MAGIC SUM(obitosAcumulados) as obitosAcumulados_sum,
# MAGIC regiao
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao
# MAGIC ORDER BY obitosAcumulados_sum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE tabela_covid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao, 
# MAGIC SUM(casosNovos) AS soma_regiao,
# MAGIC MIN(casosNovos) AS min_casos,
# MAGIC MAX(casosNovos) AS max_casos,
# MAGIC AVG(casosNovos) as media_casos
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao
# MAGIC ORDER BY regiao
