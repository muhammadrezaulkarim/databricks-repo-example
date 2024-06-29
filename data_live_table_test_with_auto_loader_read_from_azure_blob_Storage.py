@@ -0,0 +1,56 @@
+# Databricks notebook source
+import dlt
+from pyspark.sql.functions import *
+
+# COMMAND ----------
+
+storage_account_name = 'datastorageaccount760'
+storage_account_access_key = 'OM4+/m3W5myxo6RR7YsMMJhHwg7z6LWlWmD0cmk'
+spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
+
+# COMMAND ----------
+
+@dlt.table(
+  comment="Popular baby first names in New York. This data was ingested from the New York State Department of Health."
+)
+def baby_names_raw():
+  blob_container = 'baby-names'
+  directory_path = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"
+  
+  df = (
+        spark.readStream.format("cloudFiles")
+        .option("cloudFiles.format", "csv")
+        .load(directory_path)
+  )
+
+  df_renamed_column = df.withColumnRenamed("First Name", "First_Name")
+  return df_renamed_column
+
+# COMMAND ----------
+
+@dlt.table(
+  comment="New York popular baby first name data cleaned and prepared for analysis."
+)
+@dlt.expect("valid_first_name", "First_Name IS NOT NULL")
+@dlt.expect_or_fail("valid_count", "Count > 0")
+def baby_names_prepared():
+  return (
+    dlt.read("baby_names_raw")
+      .withColumnRenamed("Year", "Year_Of_Birth")
+      .select("Year_Of_Birth", "First_Name", "Count")
+  )
+
+# COMMAND ----------
+
+@dlt.table(
+  comment="A table summarizing counts of the top baby names for New York for 2021."
+)
+def top_baby_names_2021():
+  return (
+    dlt.read("baby_names_prepared")
+      .filter(expr("Year_Of_Birth == 2021"))
+      .groupBy("First_Name")
+      .agg(sum("Count").alias("Total_Count"))
+      .sort(desc("Total_Count"))
+      .limit(10)
+  )
