// Databricks notebook source
// Blob Storage
var account = "datasciencestoragedev1"
var storagekey = dbutils.secrets.get(scope = "public", key = "public")

var container = "ihme"  
var filename = "vTargetMail.csv"
//var mountname = "test_to_mount"

var sourceString = f"wasbs://$container%s@$account%s.blob.core.windows.net"
var extraConfigKey = f"fs.azure.account.key.$account%s.blob.core.windows.net"

spark.conf.set(
  extraConfigKey,
  storagekey)

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(f"$sourceString%s/$filename%s")

// COMMAND ----------

