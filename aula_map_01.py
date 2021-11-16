# Databricks notebook source
# DBTITLE 1,CRIANDO RDD
rdd =spark.sparkContext.parallelize(range(1,100))

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# DBTITLE 1,MAP - Percorre os valores de RDD fazendo alterações
rdd.map(lambda x: x * 5).collect()

# COMMAND ----------

CRIAR UMA LISTA COM NOMES E DEIXAR EM NOME MAIUSCULO

# COMMAND ----------

name=['ASas','JAjsj','Jamami','TuTutUTu','Babe','ZouUk','Torreto','Dio','JoJo']
rdd1 =spark.sparkContext.parallelize(name)
rdd1.collect()

# COMMAND ----------

type(rdd1)

# COMMAND ----------

rdd1.map(lambda x: x .upper()).collect()

# COMMAND ----------

# DBTITLE 1,flatMap
name2 = ['Eng. de Dados','Curso da Soul Code','Professores Bis e Godi']

rdd3 = spark.sparkContext.parallelize(name2)
rdd3.collect()

rdd3.flatMap(lambda y:y.split(" ")).map(lambda z:z.upper()).collect()


# COMMAND ----------

# DBTITLE 1,Filter
rdd4 =spark.sparkContext.parallelize(range(1,51))

rdd4.filter(lambda x: x <=30).collect()
rdd4.filter(lambda y: y >=10).filter(lambda z:z <= 20).collect()
rdd4.filter(lambda z: 10 <= z <=20).collect()

# COMMAND ----------

name3 = ['Eng. de Dados Code Soul','Curso da PUTARIA para iniciantes','Professores Naruto e Ana Maria Braga']

rdd5 = spark.sparkContext.parallelize(name3)
rdd5.collect()

rdd5.filter(lambda x : x in name3).collect()


# COMMAND ----------

# DBTITLE 1,Union
rdd6 =spark.sparkContext.parallelize(range(1,11))
rdd7 =spark.sparkContext.parallelize(range(11,21))

rdd.union(rdd7).collect()

# COMMAND ----------

# DBTITLE 1,Distinct
listnumber = [10,10,3,5,5,4,9,4,9,4]
rdd8 = spark.sparkContext.parallelize(listnumber)
rdd8.distinct().collect()

# COMMAND ----------

rdd9 = spark.sparkContext.parallelize(['Eu','Meu','Tio','e','Tu'])
rdd9.distinct().collect()

# COMMAND ----------

# DBTITLE 1,SortBy
rdd10 = spark.sparkContext.parallelize([('Eu',1),('Meu',2),('Tio',3),('E',4),('Tu',5)])
rdd10.collect()

# COMMAND ----------

rdd10.sortByKey(lambda x: x[1]).collect()

# COMMAND ----------

rdd10.sortByKey(lambda x: -x[1]).collect()

# COMMAND ----------

rdd10.sortByKey().collect()

# COMMAND ----------

# DBTITLE 1,Intersection
rdd11 = spark.sparkContext.parallelize(range(1,50))
rdd12 = spark.sparkContext.parallelize(range(30,51))

rdd11.intersection(rdd12).collect()

# COMMAND ----------

rdd13 = spark.sparkContext.parallelize(['Eu','Meu','Tio','e','Tu'])
rdd14 = spark.sparkContext.parallelize(['Come','Meu','Saco','ae','Tio'])

rdd13.intersection(rdd14).collect()

# COMMAND ----------

# DBTITLE 1,Subtract
rdd13 = spark.sparkContext.parallelize(['Eu','Meu','Tio','e','Tu'])
rdd14 = spark.sparkContext.parallelize(['Come','Meu','Saco','ae','Tio'])

rdd13.subtract(rdd14).collect()

# COMMAND ----------

# DBTITLE 1,COUNT
rdd15 = spark.sparkContext.parallelize(range(1,101))
rdd15.count()

# COMMAND ----------

# DBTITLE 1,TAKE
rdd15.take(10)

# COMMAND ----------

# DBTITLE 1,TOP
rdd15.top(2)

# COMMAND ----------

# DBTITLE 1,First
rdd15.first()

# COMMAND ----------

# DBTITLE 1,takeOrdered
rdd15.takeOrdered(10,key = lambda x: x)

# COMMAND ----------

# DBTITLE 1,Reduce
rdd.reduce(lambda a , b: a + b)
