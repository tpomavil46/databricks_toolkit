# Databricks notebook source
dbutils.widgets.text(name='catalog', defaultValue='')
dbutils.widgets.text(name='schema', defaultValue='')
full_path = f"/Volumes/{dbutils.widgets.get('catalog')}/{dbutils.widgets.get('schema')}/trigger_storage_location/"

babynames = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(full_path)
display(babynames.filter(babynames.Year == "2014"))