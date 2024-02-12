// Databricks notebook source
// MAGIC %md
// MAGIC ##### Conferindo se dos dados foram montados e se temos acesso a pasta inbound

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Lendo os dados na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Transofrmando os campos json em colunas

// COMMAND ----------


display(df.select("anuncio.*"))

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Removendo colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")

display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Salvando na camada silver

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"

df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------


