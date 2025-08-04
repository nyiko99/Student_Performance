# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE studentperformances
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path '/FileStore/shared_uploads/nyikoelsie99@gmail.com/studentsperformance.csv',
# MAGIC   header 'true',
# MAGIC   inferSchema 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM studentperformances;

# COMMAND ----------

# MAGIC %md
# MAGIC # **EDA**

# COMMAND ----------

df = spark.table("studentperformances")

# Get row and column counts
num_rows = df.count()
num_columns = len(df.columns)

print(f"Number of rows: {num_rows}")
print(f"Number of columns: {num_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC  Exploring Data Types

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE studentperformances;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Descriptive statistics**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'math score' AS column,
# MAGIC   COUNT(`math score`) AS count,
# MAGIC   AVG(`math score`) AS mean,
# MAGIC   STDDEV(`math score`) AS stddev,
# MAGIC   MIN(`math score`) AS min,
# MAGIC   MAX(`math score`) AS max
# MAGIC FROM studentperformance
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'reading score' AS column,
# MAGIC   COUNT(`reading score`) AS count,
# MAGIC   AVG(`reading score`) AS mean,
# MAGIC   STDDEV(`reading score`) AS stddev,
# MAGIC   MIN(`reading score`) AS min,
# MAGIC   MAX(`reading score`) AS max
# MAGIC FROM studentperformance
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'writing Score' AS column,
# MAGIC   COUNT(`writing Score`) AS count,
# MAGIC   AVG(`writing Score`) AS mean,
# MAGIC   STDDEV(`writing Score`) AS stddev,
# MAGIC   MIN(`writing Score`) AS min,
# MAGIC   MAX(`writing Score`) AS max
# MAGIC FROM studentperformances;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data Cleansing**

# COMMAND ----------

# MAGIC %md
# MAGIC Standardizing Field Names

# COMMAND ----------

# Step 1: Load your table
df = spark.table("studentperformances")

# Step 2: Rename the columns
df_renamed = df.selectExpr(
    "`math score` as Math_Score",
    "`writing score` as Writing_Score",
    "`reading score` as Reading_score",
    "pass_fail_status as Result",
    "`parental level of education` as Parental_Level_of_education",
    "`race/ethnicity` as Ethnicity",
    "`Gender` as Gender",
    "`Lunch` as Lunch",
    "`average_score` as Average_Score",
 "`test preparation course` as Test_Preparation_Course "
)

# Step 3: Overwrite the original table (or write to a new one)
df_renamed.write \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("studentperformances_clean")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM studentperformances_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC # **Checking for Missing**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM studentperformances_clean
# MAGIC WHERE
# MAGIC   Math_Score IS NULL OR
# MAGIC   Reading_score IS NULL OR
# MAGIC   Writing_Score IS NULL OR
# MAGIC   Result IS NULL OR
# MAGIC   Test_Preparation_Course IS NULL OR
# MAGIC   Gender IS NULL OR
# MAGIC   Parental_Level_of_education IS NULL OR
# MAGIC   Ethnicity IS NULL;
# MAGIC

# COMMAND ----------

