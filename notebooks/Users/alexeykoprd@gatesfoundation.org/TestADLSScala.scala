// Databricks notebook source
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "28dd9b07-f9c4-45ff-a24b-3ed4d3be8115")
spark.conf.set("dfs.adls.oauth2.credential", "WsCpZy25PUe6dqyD50fQMcdHRW0uUpPw3icYTCAa5HM=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/296b3838-4bd5-496c-bd4b-f456ea743b74/oauth2/token")

// COMMAND ----------

val df = spark.read.json("adl://sandbox0523dls.azuredatalakestore.net/small_radio_json.json")
//dbutils.fs.ls("adl://sandbox0523dls.azuredatalakestore.net/small_radio_json.json")


// COMMAND ----------

