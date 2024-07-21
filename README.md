# Estatisca-Pyspark

Esse repositório tem o intuito de ter algumas funções padrões para que possamos utilizar análises estatísticas em pyspark.


## Medidas de Tendência Central

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, stddev, mean, expr, percentile_approx, count
from pyspark.sql.window import Window

def basic_statistics(df, column_name):
    # Calcula a média, desvio padrão e contagem
    stats = df.select(
        mean(col(column_name)).alias('mean'),
        stddev(col(column_name)).alias('stddev'),
        count(col(column_name)).alias('count')
    )

    # Calcula a mediana
    median = df.select(
        percentile_approx(col(column_name), 0.5).alias('median')
    )

    # Calcula a moda
    mode_df = df.groupBy(column_name).count().orderBy('count', ascending=False).limit(1)
    mode = mode_df.select(col(column_name).alias('mode'))

    # Unifica todas as estatísticas em um DataFrame
    result = stats.crossJoin(median).crossJoin(mode)
    
    return result

# Uso da função
basic_stats_df = basic_statistics(df, column_name)
basic_stats_df.show()
```


## Medidas Separatrizes

```
from pyspark.sql.functions import percentile_approx

def separatrices_statistics(df, column_name):
    # Percentis a serem calculados
    percentiles_list = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    
    # Calcula os percentis
    percentiles_exprs = [percentile_approx(col(column_name), p).alias(f'percentile_{int(p*100)}') for p in percentiles_list]
    percentiles = df.select(*percentiles_exprs)
    
    # Calcula a mediana e os quartis separadamente
    quartiles = df.select(
        percentile_approx(col(column_name), 0.25).alias('25th_percentile'),
        percentile_approx(col(column_name), 0.50).alias('median'),
        percentile_approx(col(column_name), 0.75).alias('75th_percentile')
    )

    # Unifica os percentis e quartis em um DataFrame
    result = quartiles.crossJoin(percentiles)

    return result

# Exemplo de uso
separatrices_stats_df = separatrices_statistics(df, column_name)

separatrices_stats_df.show()
```

## Medidas de Dispersão
```
def dispersion_statistics(df, column_name):
    # Calcula a variância
    variance_stat = df.select(
        variance(col(column_name)).alias('variance')
    )
    
    # Calcula a amplitude interquartil (IQR)
    quartiles = df.select(
        percentile_approx(col(column_name), 0.25).alias('25th_percentile'),
        percentile_approx(col(column_name), 0.75).alias('75th_percentile')
    )
    iqr = quartiles.select(
        (col('75th_percentile') - col('25th_percentile')).alias('IQR')
    )
    
    # Unifica a variância e o IQR em um DataFrame
    result = variance_stat.crossJoin(iqr)
    
    return result

# Exemplo de uso
dispersion_stats_df = dispersion_statistics(df, column_name)

dispersion_stats_df.show()
```
## Medidas em conjunto

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, stddev, mean, expr, percentile_approx, count, variance

def compute_all_statistics(df, column_name):
    # Calcula as estatísticas básicas
    basic_stats = df.select(
        max(col(column_name)).alias('max'),
        min(col(column_name)).alias('min'),
        mean(col(column_name)).alias('mean'),
        stddev(col(column_name)).alias('stddev'),
        count(col(column_name)).alias('count')
    )
    
    # Calcula a mediana
    median = df.select(
        percentile_approx(col(column_name), 0.5).alias('median')
    )
    
    # Calcula a moda
    mode_df = df.groupBy(column_name).count().orderBy('count', ascending=False).limit(1)
    mode = mode_df.select(col(column_name).alias('mode'))
    
    # Unifica as estatísticas básicas, mediana e moda
    basic_stats = basic_stats.crossJoin(median).crossJoin(mode)

    # Percentis a serem calculados
    percentiles_list = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    
    # Calcula os percentis
    percentiles_exprs = [percentile_approx(col(column_name), p).alias(f'percentile_{int(p*100)}') for p in percentiles_list]
    percentiles = df.select(*percentiles_exprs)
    
    # Calcula os quartis separadamente
    quartiles = df.select(
        percentile_approx(col(column_name), 0.25).alias('25th_percentile'),
        percentile_approx(col(column_name), 0.75).alias('75th_percentile')
    )
    
    # Unifica os percentis e quartis
    separatrices = quartiles.crossJoin(percentiles)

    # Calcula a variância
    variance_stat = df.select(
        variance(col(column_name)).alias('variance')
    )
    
    # Calcula a amplitude interquartil (IQR)
    iqr = quartiles.select(
        (col('75th_percentile') - col('25th_percentile')).alias('IQR')
    )
    
    # Unifica a variância e o IQR
    dispersion = variance_stat.crossJoin(iqr)

    # Unifica todas as estatísticas em um único DataFrame
    result = basic_stats.crossJoin(separatrices).crossJoin(dispersion)
    
    return result

# Exemplo de uso
spark = SparkSession.builder.appName("StatisticsApp").getOrCreate()

df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,), (5,)], ["values"])
column_name = 'values'
all_stats_df = compute_all_statistics(df, column_name)

all_stats_df.show()
```
# Detecção de outliers

## Detecção Interquantil

```
def detect_outliers(df, column):
    # Calcular Q1 e Q3
    q1 = df.approxQuantile(column, [0.25], 0.01)[0]
    q3 = df.approxQuantile(column, [0.75], 0.01)[0]
    iqr = q3 - q1
    
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    # Filtrar os outliers
    outliers = df.filter((col(column) < lower_bound) | (col(column) > upper_bound))
    return outliers


## Aplicando em uma única coluna

column = "column"
outliers = detect_outliers(df_base_earn, column)

## Aplicando em várias colunas
columns = ["column1", "column2"]

outliers_dict = {}
for column in columns:
    outliers_dict[column] = detect_outliers(df_base_earn, column)

# Exibir os outliers para cada coluna
for column, outliers in outliers_dict.items():
    print(f"Outliers for {column}:")
    outliers.show()

```

## Detecção Z-Score

```
def detect_outliers(df, column_name, threshold=3):
    # Calcular estatísticas descritivas
    stats = df.select(
        mean(col(column_name)).alias("mean"),
        stddev(col(column_name)).alias("stddev")
    ).collect()
    
    mean_value = stats[0]["mean"]
    stddev_value = stats[0]["stddev"]
    
    # Calcular Z-Score e filtrar outliers
    df_with_zscore = df.withColumn("z_score", (col(column_name) - mean_value) / stddev_value)
    outliers = df_with_zscore.filter((col("z_score") > threshold) | (col("z_score") < -threshold))
    
    return outliers

outliers = detect_outliers(df, "value")
outliers.show()
```

