"""
silver-to-gold-aid-demand-forecast-analysis.py

Costruisce TRE tabelle Gold a partire da gold.aid_demand_analysis:

    gold.trend_forecast      — chiave: location_code + variable_name
                               regressione temporale + proiezione
    gold.model_validation    — chiave: location_code + variable_name
                               backtest out-of-sample vs baseline naive
    gold.risk_profile        — chiave: location_code + year
                               posizione relativa tra paesi nello stesso anno

PERCHÉ IL BACKTEST
    Un modello che produce previsioni senza essere validato non è
    forecasting — è estrapolazione. La differenza sta nell'aver verificato,
    su dati che il modello non ha visto, quanto sbaglia davvero.

    L'intervallo di confidenza calcolato sui residui interni al fit è una
    stima ottimistica dell'errore: misura quanto la retta si discosta dai
    punti che ha usato per costruirsi. Il backtest misura quanto sbaglia
    su punti che non ha mai visto — che è la domanda vera.

    Il confronto con la baseline naive è altrettanto necessario: se
    prevedere "l'anno prossimo cresce come l'ultimo anno" sbaglia meno
    della regressione, la regressione non serve. 

"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

spark = get_spark_session("SilverToGold-AidDemandForecastAnalysis")


# PARAMETRI 

MIN_OBSERVATIONS = 8
Z_95 = 1.96

# Anni riservati alla validazione: il modello viene fittato SENZA questi,
# poi confrontato con i valori reali che non ha mai visto.

BACKTEST_HOLDOUT_YEARS = 3

# Minimo di osservazioni nel training set del backtest.

MIN_TRAIN_OBSERVATIONS = 5

# Variabili sottoposte a regressione temporale.
TREND_VARIABLES = [
    "internal_idps",
    "total_displacement_burden",
    "funding_coverage_pct",
    "conflict_events_total",
]

# Variabili sottoposte a backtest.
#
# funding_coverage_pct è esclusa: r² medio 0.20 sul modello finale
# significa che non segue un trend temporale — i finanziamenti umanitari
# dipendono da decisioni politiche annuali e crisi mediatiche, non da una
# traiettoria estrapolabile. Validare un trend che non esiste non aggiunge
# informazione. Resta nel modello finale e nel risk profile.
BACKTEST_VARIABLES = [
    "internal_idps",
    "total_displacement_burden",
    "conflict_events_total",
]

# direction: high_is_worse → valore alto = più rischio
#            low_is_worse  → valore basso = più rischio (scala invertita)
RISK_VARIABLES = {
    "internal_idps": "high_is_worse",
    "total_displacement_burden": "high_is_worse",
    "aid_gap": "high_is_worse",
    "conflict_events_total": "high_is_worse",
    "funding_coverage_pct": "low_is_worse",
    "gdp_per_capita": "low_is_worse",
    "extreme_poverty": "high_is_worse",
}

GOLD_BASE = "s3a://lakehouse/gold"
STAGING_VALIDATION = f"{GOLD_BASE}/_staging_validation"


# LETTURA GOLD 

source = spark.read.format("delta").load(f"{GOLD_BASE}/aid_demand_analysis")

spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://lakehouse/gold'")


def register_and_write(dataframe, table_name, mode="overwrite"):
    """Crea la tabella Delta se non esiste e ci scrive dentro."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.{table_name}
        USING delta
        LOCATION '{GOLD_BASE}/{table_name}'
    """)
    dataframe.write \
        .format("delta") \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/{table_name}")



# FUNZIONE DI REGRESSIONE — parametrizzata
#
# Minimi quadrati in forma chiusa:
#     slope     = Σ[(x-x̄)(y-ȳ)] / Σ[(x-x̄)²]
#     intercept = ȳ - slope × x̄
#
# La chiave è un parametro (key_cols) invece che hardcoded: la stessa
# funzione serve sia il modello finale sia il backtest, e resterebbe valida
# se un giorno la granularità cambiasse (es. admin1).



def fit_linear_regression(df, key_cols, value_col="value", min_obs=MIN_OBSERVATIONS):
    """
    Fitta una regressione lineare value ~ year per ogni gruppo definito da
    key_cols. Restituisce slope, intercept, n, e statistiche di fit.

    FILTRO DI IDONEITÀ — due condizioni, entrambe da verifica empirica:

    1. n >= min_obs
       Verificato che anche al minimo la dispersione temporale è
       sufficiente.

    2. varianza > 0
       
    """

    eligible = df \
        .groupBy(*key_cols) \
        .agg(
            F.count(value_col).alias("n_observations"),
            F.var_samp(value_col).alias("_variance")
        ) \
        .filter(
            (F.col("n_observations") >= min_obs)
            & (F.col("_variance") > 0)
        ) \
        .select(*key_cols, "n_observations")

    data = df.join(eligible, on=key_cols, how="inner")

    w = Window.partitionBy(*key_cols)

    deviations = data \
        .withColumn("_year_mean", F.avg("year").over(w)) \
        .withColumn("_value_mean", F.avg(value_col).over(w)) \
        .withColumn("_xy",
            (F.col("year") - F.col("_year_mean"))
            * (F.col(value_col) - F.col("_value_mean"))
        ) \
        .withColumn("_xx",
            F.pow(F.col("year") - F.col("_year_mean"), 2)
        )

    params = deviations \
        .groupBy(*key_cols) \
        .agg(
            F.first("n_observations").alias("n_observations"),
            F.first("_year_mean").alias("_year_mean"),
            F.first("_value_mean").alias("_value_mean"),
            F.sum("_xy").alias("_sum_xy"),
            F.sum("_xx").alias("_sum_xx")
        ) \
        .withColumn("slope", F.col("_sum_xy") / F.col("_sum_xx")) \
        .withColumn("intercept",
            F.col("_value_mean") - F.col("slope") * F.col("_year_mean")
        )

    residuals = data \
        .join(
            params.select(*key_cols, "slope", "intercept", "_value_mean"),
            on=key_cols, how="inner"
        ) \
        .withColumn("_pred", F.col("intercept") + F.col("slope") * F.col("year")) \
        .withColumn("_res_sq", F.pow(F.col(value_col) - F.col("_pred"), 2)) \
        .withColumn("_tot_sq", F.pow(F.col(value_col) - F.col("_value_mean"), 2))

    fit = residuals \
        .groupBy(*key_cols) \
        .agg(
            F.sum("_res_sq").alias("_ss_residual"),
            F.sum("_tot_sq").alias("_ss_total")
        )

    # r_squared_adjusted penalizza n piccolo:
    #     R²adj = 1 - [(1-R²)(n-1)/(n-p-1)]  con p=1
    # Su n=26 la penalità è trascurabile, su n=8 pesa — proprio dove un R²
    # alto potrebbe essere fortuna dei pochi punti più che trend genuino.
    return params \
        .join(fit, on=key_cols, how="inner") \
        .withColumn("r_squared",
            F.round(1 - (F.col("_ss_residual") / F.col("_ss_total")), 4)
        ) \
        .withColumn("r_squared_adjusted",
            F.round(
                1 - ((1 - F.col("r_squared")) * (F.col("n_observations") - 1)
                     / (F.col("n_observations") - 2)),
                4
            )
        ) \
        .withColumn("standard_error",
            F.round(F.sqrt(F.col("_ss_residual") / (F.col("n_observations") - 2)), 2)
        ) \
        .select(
            *key_cols, "n_observations", "slope", "intercept",
            "r_squared", "r_squared_adjusted", "standard_error"
        )



# LIVELLO 1 — MODELLO FINALE E PROIEZIONE


def build_trend(df, variable_name):
    """Regressione su tutta la serie disponibile + proiezione a 1 anno."""

    data = df.select(
        "location_code", "year",
        F.col(variable_name).alias("value")
    ).filter(F.col("value").isNotNull())

    model = fit_linear_regression(data, ["location_code"])

    last_year = data \
        .groupBy("location_code") \
        .agg(F.max("year").alias("last_observed_year"))

    # L'anno di proiezione è last_observed + 1 per paese, non un anno fisso:
    # serie che finiscono in anni diversi devono proiettare da dove
    # finiscono davvero.
    
    
   
    return model \
        .join(last_year, on="location_code", how="inner") \
        .withColumn("forecast_year", F.col("last_observed_year") + 1) \
        .withColumn("_raw",
            F.col("intercept") + F.col("slope") * F.col("forecast_year")
        ) \
        .withColumn("predicted_value",
            F.round(F.greatest(F.lit(0.0), F.col("_raw")), 2)
        ) \
        .withColumn("predicted_lower_95",
            F.round(
                F.greatest(F.lit(0.0), F.col("_raw") - Z_95 * F.col("standard_error")),
                2
            )
        ) \
        .withColumn("predicted_upper_95",
            F.round(F.col("_raw") + Z_95 * F.col("standard_error"), 2)
        ) \
        .select(
            "location_code",
            F.lit(variable_name).alias("variable_name"),
            F.col("n_observations").cast("int").alias("n_observations"),
            "last_observed_year",
            F.round("slope", 4).alias("slope"),
            F.round("intercept", 4).alias("intercept"),
            "r_squared", "r_squared_adjusted", "standard_error",
            "forecast_year", "predicted_value",
            "predicted_lower_95", "predicted_upper_95",
        )


print("=" * 78)
print("LIVELLO 1 — regressione temporale")
print("=" * 78)

# Materializzazione incrementale, stesso motivo del backtest più sotto:
# quattro build_trend uniti senza scrittura intermedia producono un piano
# di esecuzione troppo profondo per la memoria del container.
STAGING_TREND = f"{GOLD_BASE}/_staging_trend"

for i, variable in enumerate(TREND_VARIABLES):
    frame = build_trend(source, variable)
    frame.write \
        .format("delta") \
        .mode("overwrite" if i == 0 else "append") \
        .option("mergeSchema", "true") \
        .save(STAGING_TREND)
    print(f"  trend {variable}: completato")

trend_forecast = spark.read.format("delta").load(STAGING_TREND)

register_and_write(trend_forecast, "trend_forecast")
print("gold.trend_forecast: scrittura completata")

trend_forecast = spark.read.format("delta").load(f"{GOLD_BASE}/trend_forecast")

print()
print("=" * 78)
print("REPORT — gold.trend_forecast")
print("=" * 78)

for row in trend_forecast.groupBy("variable_name").agg(
    F.count(F.lit(1)).alias("paesi"),
    F.round(F.avg("n_observations"), 1).alias("n_medio"),
    F.round(F.avg("r_squared"), 3).alias("r2_medio"),
    F.round(F.min("r_squared"), 3).alias("r2_min"),
    F.round(F.max("r_squared"), 3).alias("r2_max"),
).collect():
    print(
        f"{row['variable_name']:<28} paesi={row['paesi']:<4} "
        f"n_medio={row['n_medio']:<6} "
        f"r²: medio={row['r2_medio']} min={row['r2_min']} max={row['r2_max']}"
    )



# LIVELLO 1b — BACKTEST OUT-OF-SAMPLE
#
# PROCEDURA
#   1. Per ogni paese, divido la serie in training (tutti gli anni tranne
#      gli ultimi 3) e test (gli ultimi 3).
#   2. Fitto la regressione SOLO sul training.
#   3. Prevedo gli anni di test — che il modello non ha mai visto.
#   4. Confronto con i valori reali e misuro l'errore.
#   5. Faccio la stessa cosa con la baseline naive.
#   6. Confronto i due errori.
#
# METRICHE
#   MAE  — errore medio assoluto, in unità originali (persone, eventi).
#          Interpretabile ma non confrontabile tra paesi di scala diversa.
#   MAPE — errore medio percentuale. Confrontabile tra paesi, ma esplode
#          quando i valori reali sono vicini a zero (stesso problema già
#          gestito con la soglia su yoy_growth in aid_demand_analysis).
#
# BASELINE NAIVE
#   previsione(t) = valore(t-1) + [valore(t-1) - valore(t-2)]
#   Cioè: "continua alla stessa velocità dell'ultimo intervallo osservato".
#   Stessa logica di idps_naive_projection_1y, applicata qui come termine
#   di paragone. Il confronto è il punto: se la regressione non batte
#   questa, non serve.


def build_backtest(df, variable_name):
    """
    Validazione out-of-sample della regressione contro baseline naive.

    NOTA IMPLEMENTATIVA: i risultati intermedi vengono scritti su disco
    invece di restare come piano lazy. Senza, train_model viene ricalcolato
    a ogni join che lo usa — e siccome contiene window function annidate,
    il piano cresce fino a far esplodere il driver (exit -9).
    """

    tmp = f"{GOLD_BASE}/_tmp_{variable_name}"

    #  Serie base, materializzata 
    df.select(
        "location_code", "year",
        F.col(variable_name).alias("value")
    ).filter(F.col("value").isNotNull()) \
     .write.format("delta").mode("overwrite") \
     .option("overwriteSchema", "true").save(f"{tmp}/data")

    data = spark.read.format("delta").load(f"{tmp}/data")

    # Split train/test 
    boundaries = data \
        .groupBy("location_code") \
        .agg(F.max("year").alias("_max_year")) \
        .withColumn("_split_year", F.col("_max_year") - BACKTEST_HOLDOUT_YEARS) \
        .select("location_code", "_split_year")

    data_split = data.join(boundaries, on="location_code", how="inner")

    data_split.filter(F.col("year") <= F.col("_split_year")) \
        .select("location_code", "year", "value") \
        .write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{tmp}/train")

    data_split.filter(F.col("year") > F.col("_split_year")) \
        .select("location_code", "year", "value") \
        .write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{tmp}/test")

    train = spark.read.format("delta").load(f"{tmp}/train")
    test = spark.read.format("delta").load(f"{tmp}/test")

    #  Modello sul training, materializzato
    # Usato due volte più avanti: senza scriverlo, la catena di window
    # function verrebbe ricalcolata a ogni uso.
    fit_linear_regression(train, ["location_code"], min_obs=MIN_TRAIN_OBSERVATIONS) \
        .write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{tmp}/model")

    train_model = spark.read.format("delta").load(f"{tmp}/model")

    # Errori della regressione sul test set
    test \
        .join(
            train_model.select("location_code", "slope", "intercept"),
            on="location_code", how="inner"
        ) \
        .withColumn("_pred",
            F.greatest(F.lit(0.0), F.col("intercept") + F.col("slope") * F.col("year"))
        ) \
        .withColumn("_abs_err", F.abs(F.col("value") - F.col("_pred"))) \
        .withColumn("_pct_err",
            F.when(F.abs(F.col("value")) < 1, None)
             .otherwise(
                 F.abs(F.col("value") - F.col("_pred")) * 100.0 / F.abs(F.col("value"))
             )
        ) \
        .groupBy("location_code") \
        .agg(
            F.count(F.lit(1)).alias("n_test_points"),
            F.avg("_abs_err").alias("regression_mae"),
            F.avg("_pct_err").alias("regression_mape")
        ) \
        .write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{tmp}/reg_err")

    regression_errors = spark.read.format("delta").load(f"{tmp}/reg_err")

    #  Errori della baseline naive 
    w_lag = Window.partitionBy("location_code").orderBy("year")

    data \
        .withColumn("_lag1", F.lag("value", 1).over(w_lag)) \
        .withColumn("_lag2", F.lag("value", 2).over(w_lag)) \
        .join(boundaries, on="location_code", how="inner") \
        .filter(F.col("year") > F.col("_split_year")) \
        .filter(F.col("_lag1").isNotNull() & F.col("_lag2").isNotNull()) \
        .withColumn("_pred",
            F.greatest(F.lit(0.0), F.col("_lag1") + (F.col("_lag1") - F.col("_lag2")))
        ) \
        .withColumn("_abs_err", F.abs(F.col("value") - F.col("_pred"))) \
        .withColumn("_pct_err",
            F.when(F.abs(F.col("value")) < 1, None)
             .otherwise(
                 F.abs(F.col("value") - F.col("_pred")) * 100.0 / F.abs(F.col("value"))
             )
        ) \
        .groupBy("location_code") \
        .agg(
            F.avg("_abs_err").alias("naive_mae"),
            F.avg("_pct_err").alias("naive_mape")
        ) \
        .write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{tmp}/naive_err")

    naive_errors = spark.read.format("delta").load(f"{tmp}/naive_err")

    #  Confronto finale 
    return regression_errors \
        .join(naive_errors, on="location_code", how="inner") \
        .join(
            train_model.select(
                "location_code",
                F.col("n_observations").alias("n_train_observations"),
                F.col("r_squared").alias("train_r_squared")
            ),
            on="location_code", how="inner"
        ) \
        .withColumn("beats_naive", F.col("regression_mae") < F.col("naive_mae")) \
        .withColumn("improvement_pct",
            F.round(
                F.when(F.col("naive_mae") == 0, None)
                 .otherwise(
                     (F.col("naive_mae") - F.col("regression_mae"))
                     * 100.0 / F.col("naive_mae")
                 ),
                2
            )
        ) \
        .select(
            "location_code",
            F.lit(variable_name).alias("variable_name"),
            F.col("n_train_observations").cast("int").alias("n_train_observations"),
            F.col("n_test_points").cast("int").alias("n_test_points"),
            "train_r_squared",
            F.round("regression_mae", 2).alias("regression_mae"),
            F.round("regression_mape", 2).alias("regression_mape"),
            F.round("naive_mae", 2).alias("naive_mae"),
            F.round("naive_mape", 2).alias("naive_mape"),
            "beats_naive",
            "improvement_pct",
        )


print()
print("=" * 78)
print("LIVELLO 1b — backtest out-of-sample")
print("=" * 78)


for i, variable in enumerate(BACKTEST_VARIABLES):
    frame = build_backtest(source, variable)
    frame.write \
        .format("delta") \
        .mode("overwrite" if i == 0 else "append") \
        .option("mergeSchema", "true") \
        .save(STAGING_VALIDATION)
    print(f"  backtest {variable}: completato")

model_validation = spark.read.format("delta").load(STAGING_VALIDATION)

register_and_write(model_validation, "model_validation")
print("gold.model_validation: scrittura completata")

model_validation = spark.read.format("delta").load(f"{GOLD_BASE}/model_validation")

print()
print("=" * 78)
print("REPORT — gold.model_validation (backtest out-of-sample)")
print("=" * 78)

for row in model_validation.groupBy("variable_name").agg(
    F.count(F.lit(1)).alias("paesi"),
    F.sum(F.when(F.col("beats_naive"), 1).otherwise(0)).alias("batte_naive"),
    F.round(F.avg("regression_mape"), 1).alias("mape_regr"),
    F.round(F.avg("naive_mape"), 1).alias("mape_naive"),
).collect():
    paesi = row["paesi"]
    batte = row["batte_naive"]
    pct = round(batte * 100.0 / paesi, 1) if paesi else 0
    print(
        f"{row['variable_name']:<28} paesi={paesi:<4} "
        f"batte_naive={batte}/{paesi} ({pct}%)  "
        f"MAPE: regr={row['mape_regr']}% naive={row['mape_naive']}%"
    )



# LIVELLO 2 — PROFILO DI RISCHIO STRUTTURALE
#
# Posizione relativa di ogni paese rispetto agli altri NELLO STESSO ANNO.
#
#     score(i,t) = [X(i,t) - min(X,t)] / [max(X,t) - min(X,t)]
#

#
# DIREZIONE NON UNIFORME: funding basso = più rischio (scala invertita),
# IDPs alti = più rischio (scala diretta). Senza questa distinzione
# esplicita "score alto" significherebbe cose opposte a seconda della
# colonna.



w_year = Window.partitionBy("year")

risk_profile = source.select(
    "location_code", "year",
    *[F.col(v) for v in RISK_VARIABLES.keys()]
)

for variable, direction in RISK_VARIABLES.items():
    min_v = F.min(variable).over(w_year)
    max_v = F.max(variable).over(w_year)
    span = max_v - min_v

    # NULL quando il valore manca o quando tutti i paesi dell'anno hanno lo
    # stesso valore (span=0 → normalizzazione indefinita, stesso problema
    # algebrico della varianza zero nella regressione).
    norm = F.when(
        F.col(variable).isNull() | (span == 0), None
    ).otherwise((F.col(variable) - min_v) / span)

    if direction == "low_is_worse":
        norm = F.when(norm.isNull(), None).otherwise(1 - norm)

    risk_profile = risk_profile.withColumn(f"score_{variable}", F.round(norm, 4))

score_columns = [f"score_{v}" for v in RISK_VARIABLES.keys()]

# Conteggio delle dimensioni valutabili — "2 su 2" e "2 su 3" non sono la
# stessa cosa, e senza denominatore i paesi con dati parziali risultano
# indistinguibili da quelli con valori bassi.
evaluable = None
for c in score_columns:
    term = F.when(F.col(c).isNotNull(), 1).otherwise(0)
    evaluable = term if evaluable is None else (evaluable + term)

risk_profile = risk_profile \
    .withColumn("evaluable_dimensions", evaluable) \
    .select("location_code", "year", *score_columns, "evaluable_dimensions")

register_and_write(risk_profile, "risk_profile")
print()
print("gold.risk_profile: scrittura completata")

risk_profile = spark.read.format("delta").load(f"{GOLD_BASE}/risk_profile")

print()
print("=" * 78)
print("REPORT — gold.risk_profile")
print("=" * 78)

rp = risk_profile.agg(
    F.count(F.lit(1)).alias("righe"),
    F.countDistinct("location_code").alias("paesi"),
    F.min("year").alias("anno_min"),
    F.max("year").alias("anno_max"),
    F.round(F.avg("evaluable_dimensions"), 2).alias("dim_medie"),
).collect()[0]

print(f"Righe: {rp['righe']}   Paesi: {rp['paesi']}   "
      f"Anni: {rp['anno_min']}-{rp['anno_max']}")
print(f"Dimensioni valutabili in media: {rp['dim_medie']} su {len(RISK_VARIABLES)}")
print("=" * 78)

spark.stop()