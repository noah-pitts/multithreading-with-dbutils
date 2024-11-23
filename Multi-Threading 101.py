# Databricks notebook source
# Multi-Threading Intro - This cluster has 8 active threads. I can use anywhere between 1-8
import threading

print(threading.active_count())  # Returns the number of active threads

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/FileStore/")  # Show files in directory
display(files)

# COMMAND ----------

for file in files:
    dbutils.fs.rm(file.path, True)

# COMMAND ----------

def writeFiles(folderName, suffix):
    dbutils.fs.put(
        f"dbfs:/FileStore/{folderName}/{folderName}_{suffix}.txt", "Hello Youtube.", True
    )

# COMMAND ----------

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

numbers = range(1, 80)
threads = 8

# Create a ThreadPoolExecutor with worker threads
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    # Use the map method to apply the lambda function to each element in the list
    results = executor.map(lambda x: writeFiles("test8", x), numbers)

results = list(results)

# COMMAND ----------

def moveFiles(sourceFolder, targetFolder, suffix):
    dbutils.fs.mv(
        f"dbfs:/FileStore/{sourceFolder}/{sourceFolder}_{suffix}.txt",
        f"dbfs:/FileStore/{targetFolder}/{sourceFolder}_{suffix}",
    )

# COMMAND ----------

numbers = range(1, 80)
threads = 8
# Create a ThreadPoolExecutor with 4 worker threads
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    # Use the map method to apply the lambda function to each element in the list
    results = executor.map(lambda x: moveFiles("test8", "land8", x), numbers)

