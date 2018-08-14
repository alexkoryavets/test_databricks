// Databricks notebook source
// Blob Storage
var account = "datasciencestoragedev1"
var storagekey = dbutils.secrets.get(scope = "public", key = "public")
////var storagekey = "qbExypzZfzJ5MEa9UltsHib1v3dnOuSk0Z1zkRDpn3ZY8CuIOvyLgzWV5AgnGu2vj10xeyI4xPJBf+3flYENCw=="

//var account = "alexkoryavetsslalom"
//var storagekey = "m+jth4PnL682AYToXeCKtZR2Th5bPiY3ddzRDgbcTW3toqO+droBz3AhIomXg6PO9ou6On+kBXJNN22clHdQnw=="

// General Purpose v2
//var account = "sandbox0424blob"
//var storagekey = "kB003J+nwPChdjvycAojrl5KDMszjsyFK5ntEUU+Xf8xhSLSpaWsjPX5iGkgjLDdS8r6NSOR4WHkC3YuLyg6wg=="

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

