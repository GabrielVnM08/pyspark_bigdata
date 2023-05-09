from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, explode, split, trim, lower
import findspark
findspark.init()
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnaliseSintomas").getOrCreate()
start_time = time.time()

df = spark.read.csv("./sintomas_2021.csv", header=True, inferSchema=True)

# Exibir os grupos de idades mais comuns
grupos_idades = df.select("idade") \
    .groupBy("idade") \
    .agg(count("*").alias("count_idade")) \
    .orderBy(desc("count_idade")) \
    .collect()

for row in grupos_idades:
    idade = row["idade"]
    count_idade = row["count_idade"]
    print(idade, count_idade)

    # Exibir os sintomas mais comuns por faixa etária
    sintomas_por_idade = df.select("idade", explode(split(col("sintomas"), ",")).alias("sintoma")) \
        .select(trim(lower(col("idade"))).alias("idade"), trim(lower(col("sintoma"))).alias("sintoma")) \
        .filter(col("sintoma") != "outros") \
        .filter(col("idade") == idade) \
        .groupBy("idade", "sintoma") \
        .agg(count("*").alias("count_sintoma")) \
        .orderBy(desc("count_sintoma")) \
        .collect()

    for sintoma_row in sintomas_por_idade:
        sintoma = sintoma_row["sintoma"]
        count_sintoma = sintoma_row["count_sintoma"]
        print(f"  Sintoma: {sintoma}, {count_sintoma} Pacientes")

# Exibir os sintomas mais comuns
top_sintomas = df.select(explode(split(col("sintomas"), ",")).alias("sintoma")) \
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