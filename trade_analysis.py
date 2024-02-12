from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_timestamp, concat, lit, date_trunc, min as spark_min, max as spark_max, avg, first, when, lag
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("Trading Analysis") \
    .config("spark.jars", "/Users/Fran_1/Desktop/programacion/dienteDeSierra/sqlite-jdbc-3.44.0.0.jar") \
    .getOrCreate()


# Ruta de la base de datos SQLite
db_path = "trading_1_spark.db"
tabla = "ETHUSDT_updated"

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlite:{db_path}") \
    .option("dbtable", tabla) \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# Convertir fecha y hora en timestamp
df = df.withColumn("timestamp", to_timestamp(concat(col("fecha"), lit(" "), col("hora")), 'yyyy-MM-dd HH:mm:ss'))

# Función para calcular los valores correspondientes al precio mínimo de cierre por periodo
def calculate_values_for_min_close(period):
    window_spec = Window.partitionBy(date_trunc(period, col("timestamp")))
    min_value_close = spark_min("valor_close").over(window_spec)
    
    df_with_min = df.withColumn("min_value_close", min_value_close)
    return df_with_min \
        .filter(df_with_min["valor_close"] == df_with_min["min_value_close"]) \
        .groupBy(date_trunc(period, col("timestamp")).alias(f"{period}_start")) \
        .agg(
            first("valor_close").alias("min_valor_close"),
            first("rsi").alias("rsi_at_min"),
            first("stochastic").alias("stochastic_at_min"),
            first("stochastic_rsi").alias("stochastic_rsi_at_min")
        )

# Calcular para diferentes periodos
hourly_min = calculate_values_for_min_close("hour")
daily_min = calculate_values_for_min_close("day")
weekly_min = calculate_values_for_min_close("week")
monthly_min = calculate_values_for_min_close("month")

# Mostrar resultados (opcional)
hourly_min.show()
daily_min.show()
weekly_min.show()
monthly_min.show()