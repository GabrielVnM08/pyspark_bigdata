from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, explode, split, trim, lower
import findspark
import time

findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnaliseSintomas").getOrCreate()
start_time = time.time()

df_2020 = spark.read.csv("./sintomas_2020.csv", header=True, inferSchema=True)
df_2021 = spark.read.csv("./sintomas_2021.csv", header=True, inferSchema=True)
df_2022 = spark.read.csv("./sintomas_2022.csv", header=True, inferSchema=True)


# Casos notificados
total_casos_2020 = df_2020.count()
print("Total de casos notificados em 2020: ", total_casos_2020)

total_casos_2021 = df_2021.count()
print("Total de casos notificados em 2021: ", total_casos_2021)

total_casos_2022 = df_2022.count()
print("Total de casos notificados em 2022: ", total_casos_2022)

total_casos = total_casos_2020 + total_casos_2021 + total_casos_2022
print("Total de casos notificados nos 3 anos: ", total_casos)


# Distribuição por sexo
print("\n2020:")
distribuicao_sexo_2020 = df_2020.groupBy("sexo").agg(count("*").alias("count"))
distribuicao_sexo_2020.show()

print("2021:")
distribuicao_sexo_2021 = df_2021.groupBy("sexo").agg(count("*").alias("count"))
distribuicao_sexo_2021.show()

print("2022:")
distribuicao_sexo_2022 = df_2022.groupBy("sexo").agg(count("*").alias("count"))
distribuicao_sexo_2022.show()

# Top 5 idades mais comum
print("\nIdades mais comuns em 2020")
top_idades_2020 = df_2020.groupBy("idade").agg(count("*").alias("count")).orderBy(desc("count")).limit(5)
top_idades_2020.show()

print("Idades mais comuns em 2021")
top_idades_2021 = df_2021.groupBy("idade").agg(count("*").alias("count")).orderBy(desc("count")).limit(5)
top_idades_2021.show()

print("Idades mais comuns em 2022")
top_idades_2022 = df_2022.groupBy("idade").agg(count("*").alias("count")).orderBy(desc("count")).limit(5)
top_idades_2022.show()


# Sintomas mais comuns
print("Principais sintomas e incidência em 2020\n")

top_sintomas_2020 = df_2020.select(explode(split(col("sintomas"), ",")).alias("sintoma")) \
    .select(trim(lower(col("sintoma"))).alias("sintoma")) \
    .filter(col("sintoma") != "outros") \
    .groupBy("sintoma") \
    .agg(count("*").alias("count_2020")) \
    .orderBy(desc("count_2020")) \
    .collect()

for row in top_sintomas_2020:
    sintoma_2020 = row["sintoma"]
    count_2020= row["count_2020"]
    print(sintoma_2020, count_2020)

print("\nPrincipais sintomas e incidência em 2021\n")

top_sintomas_2021 = df_2021.select(explode(split(col("sintomas"), ",")).alias("sintoma")) \
    .select(trim(lower(col("sintoma"))).alias("sintoma")) \
    .filter(col("sintoma") != "outros") \
    .groupBy("sintoma") \
    .agg(count("*").alias("count_2021")) \
    .orderBy(desc("count_2021")) \
    .collect()

for row in top_sintomas_2021:
    sintoma_2021 = row["sintoma"]
    count_2021 = row["count_2021"]
    print(sintoma_2021, count_2021)

print("\nPrincipais sintomas e incidência em 2022\n")

top_sintomas = df_2022.select(explode(split(col("sintomas"), ",")).alias("sintoma")) \
    .select(trim(lower(col("sintoma"))).alias("sintoma")) \
    .filter(col("sintoma") != "outros") \
    .groupBy("sintoma") \
    .agg(count("*").alias("count")) \
    .orderBy(desc("count")) \
    .collect()

for row in top_sintomas:
    sintoma = row["sintoma"]
    count = row["count"]
    print(sintoma, count)

end_time = time.time()
execution_time = end_time - start_time
print("Tempo de execução:", execution_time, "segundos")

spark.stop()