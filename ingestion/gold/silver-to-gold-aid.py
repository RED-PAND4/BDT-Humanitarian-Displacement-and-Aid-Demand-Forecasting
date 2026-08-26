"""
silver-to-gold-aid-demand-analysis.py

Costruisce gold.aid_demand_analysis — chiave: location_code + year

SCOPO
    Analisi della domanda di aiuti umanitari per paese e anno, con indicatori
    di trend e proiezione naive. Copre 2000-2025.

"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

spark = get_spark_session("SilverToGold-AidDemandAnalysis")

YEAR_MIN = 2000
YEAR_MAX = 2025

# Soglia minima per calcolare una variazione percentuale.
# Su basi molto piccole le percentuali esplodono e perdono significato
# (es. da 50 a 200 IDPs = +300%). Valore scelto empiricamente.
MIN_BASE_FOR_GROWTH = 1000

# Soglia per considerare un appello sottofinanziato.

UNDERFUNDED_THRESHOLD_PCT = 50


#  HELPERS 

def safe_long(col_name):
    """
    L'API UNHCR restituisce '-' per i dati non disponibili.
    Convertiamo in NULL prima del cast.

    NOTA ARCHITETTURALE: questa pulizia dovrebbe avvenire nel Silver layer.
    È un workaround temporaneo — verificato che i trattini esistono solo in
    silver.solutions (returned_refugees, resettlement, naturalisation).
    """
    return F.when(F.col(col_name) == "-", None) \
             .otherwise(F.col(col_name).cast("long"))


def drop_null_keys(df, *columns):
    """
    Rimuove le righe con una chiave di join nulla.

    Igiene difensiva, non una scelta metodologica: una riga senza codice
    paese o anno non è utilizzabile in nessun calcolo successivo, e senza
    questo filtro un valore nullo si propagherebbe silenziosamente in ogni
    join a valle.
    """
    condition = None
    for c in columns:
        cond = F.col(c).isNotNull()
        condition = cond if condition is None else (condition & cond)
    return df.filter(condition)


#  LETTURA SILVER

population_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/population"),
    "coo_iso", "coa_iso", "year"
)
solutions_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/solutions"),
    "coo_iso", "coa_iso", "year"
)
needs_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/humanitarian_needs"),
    "location_code", "year"
)
conflict_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/conflict_events"),
    "location_code", "year"
)
foodsecurity_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/foodsecurity"),
    "location_code", "year"
)
funding_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/funding"),
    "location_code", "year", "appeal_code"
)

wb_gdp_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_gdp"),
    "location_code", "year"
)
wb_mpm_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_mpm"),
    "location_code", "year"
)
wb_poverty_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_extreme_poverty"),
    "location_code", "year"
)
wb_population_df = drop_null_keys(
    spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_population"),
    "location_code", "year"
)


#  FILTRO AGGREGATI REGIONALI WORLD BANK


valid_countries = population_df.select(
    F.col("coa_iso").alias("location_code")
).union(
    population_df.select(F.col("coo_iso").alias("location_code"))
).distinct()

wb_gdp_df = wb_gdp_df.join(valid_countries, on="location_code", how="inner")
wb_mpm_df = wb_mpm_df.join(valid_countries, on="location_code", how="inner")
wb_poverty_df = wb_poverty_df.join(valid_countries, on="location_code", how="inner")
wb_population_df = wb_population_df.join(valid_countries, on="location_code", how="inner")


# WINDOW SPECIFICATIONS 

w_country = Window.partitionBy("location_code").orderBy("year")


w_country_ma3 = Window.partitionBy("location_code").orderBy("year") \
                      .rangeBetween(-2, 0)

w_year = Window.partitionBy("year")


# BLOCCO 1 — DISPLACEMENT INTERNO (base della tabella)


internal_idps = population_df \
    .filter(F.col("coa_iso") == F.col("coo_iso")) \
    .groupBy(
        F.col("coo_iso").alias("location_code"),
        "year"
    ) \
    .agg(F.sum(safe_long("idps")).alias("internal_idps"))


#  Trend: lag, variazione annua, accelerazione
#
# yoy_growth è NULL quando:
#   - non c'è l'anno precedente
#   - gli anni non sono consecutivi (buchi nella serie UNHCR)
#   - la base è sotto MIN_BASE_FOR_GROWTH


idps_trend = internal_idps \
    .withColumn("internal_idps_lag1", F.lag("internal_idps", 1).over(w_country)) \
    .withColumn("_prev_year", F.lag("year", 1).over(w_country)) \
    .withColumn("_is_consecutive", F.col("year") - F.col("_prev_year") == 1) \
    .withColumn(
        "idps_yoy_growth_pct",
        F.round(
            F.when(
                F.col("internal_idps_lag1").isNull()
                | (F.abs(F.col("internal_idps_lag1")) < MIN_BASE_FOR_GROWTH)
                | (~F.col("_is_consecutive")),
                None
            ).otherwise(
                (F.col("internal_idps") - F.col("internal_idps_lag1"))
                * 100.0 / F.col("internal_idps_lag1")
            ),
            2
        )
    ) \
    .withColumn("_growth_lag1", F.lag("idps_yoy_growth_pct", 1).over(w_country)) \
    .withColumn(
        "idps_acceleration",
        F.round(
            F.when(
                F.col("idps_yoy_growth_pct").isNull()
                | F.col("_growth_lag1").isNull()
                | (~F.col("_is_consecutive")),
                None
            ).otherwise(
                F.col("idps_yoy_growth_pct") - F.col("_growth_lag1")
            ),
            2
        )
    )

#Proiezione naive a 1 anno 

# METODO: estrapolazione lineare su due punti.
#     proiezione(t+1) = valore(t) + [valore(t) - valore(t-1)]


idps_projection = idps_trend \
    .withColumn(
        "idps_moving_avg_3y",
        F.round(F.avg("internal_idps").over(w_country_ma3), 0)
    ) \
    .withColumn(
        "idps_naive_projection_1y",
        F.when(
            F.col("internal_idps_lag1").isNull()
            | (F.col("internal_idps_lag1") == 0)
            | (~F.col("_is_consecutive")),
            None
        ).otherwise(
            F.greatest(
                F.lit(0),
                F.col("internal_idps") + (F.col("internal_idps") - F.col("internal_idps_lag1"))
            )
        )
    ) \
    .withColumn(
        "projection_divergence_pct",
        F.round(
            F.when(
                F.col("idps_naive_projection_1y").isNull()
                | (F.col("idps_moving_avg_3y") == 0),
                None
            ).otherwise(
                F.abs(F.col("idps_naive_projection_1y") - F.col("idps_moving_avg_3y"))
                * 100.0 / F.col("idps_moving_avg_3y")
            ),
            1
        )
    ) \
    .drop("_prev_year", "_is_consecutive", "_growth_lag1")



# BLOCCO 2 — RIFUGIATI OSPITATI E BILANCIO DEI FLUSSI
#
# stock_hosted: rifugiati e richiedenti asilo presenti nel paese a fine anno
# (coa_iso != coo_iso, cioè hanno attraversato un confine).
#
# UNHCR pubblica stock, non flussi. Gli arrivi si ricavano dal bilancio
# demografico:
#     stock(t) = stock(t-1) + arrivi(t) - uscite(t)
#     => arrivi(t) = stock(t) - stock(t-1) + uscite(t)
#
# Un risultato negativo è fisicamente impossibile: significa che le uscite
# reali superano quelle registrate in solutions. In quel caso poniamo
# real_inflow = 0 e attribuiamo la differenza a untracked_outflow.
#
# ATTENZIONE INTERPRETATIVA: untracked_outflow mescola due fenomeni che non
# possiamo separare — movimenti reali non censiti (Iran 2025: 1.33M da
# rimpatri forzati) e artefatti di reporting (Uzbekistan che scende a uno
# stock di 5 persone). Segnala "il bilancio non torna", non conta persone.


stock_hosted = population_df \
    .filter(F.col("coa_iso") != F.col("coo_iso")) \
    .groupBy(
        F.col("coa_iso").alias("location_code"),
        "year"
    ) \
    .agg(
        (F.sum(safe_long("refugees")) + F.sum(safe_long("asylum_seekers")))
        .alias("stock_hosted")
    )

outflow = solutions_df \
    .filter(F.col("coa_iso") != F.col("coo_iso")) \
    .groupBy(
        F.col("coa_iso").alias("location_code"),
        "year"
    ) \
    .agg(
        F.sum(
            F.coalesce(safe_long("returned_refugees"), F.lit(0))
            + F.coalesce(safe_long("resettlement"), F.lit(0))
            + F.coalesce(safe_long("naturalisation"), F.lit(0))
        ).alias("tracked_outflow")
    )

flows = stock_hosted \
    .join(outflow, on=["location_code", "year"], how="left") \
    .withColumn("_stock_lag1", F.lag("stock_hosted", 1).over(w_country)) \
    .withColumn(
        "_balance",
        (F.col("stock_hosted") - F.col("_stock_lag1"))
        + F.coalesce(F.col("tracked_outflow"), F.lit(0))
    ) \
    .withColumn(
        "real_inflow",
        F.when(F.col("_stock_lag1").isNull(), None)
         .otherwise(F.greatest(F.lit(0), F.col("_balance")))
    ) \
    .withColumn(
        "untracked_outflow",
        F.when(F.col("_stock_lag1").isNull(), None)
         .when(F.col("_balance") < 0, F.abs(F.col("_balance")))
         .otherwise(F.lit(0))
    ) \
    .drop("_stock_lag1", "_balance")



# BLOCCO 3 — BISOGNO UMANITARIO 


def needs_metric(status):
    """Estrae una metrica PIN privilegiando category='total', con fallback su NULL."""
    return F.coalesce(
        F.max(
            F.when(
                (F.col("population_status") == status) & (F.col("category") == "total"),
                F.col("population")
            )
        ),
        F.max(
            F.when(
                (F.col("population_status") == status)
                & (F.col("category").isNull() | (F.col("category") == "")),
                F.col("population")
            )
        )
    )

needs = needs_df \
    .filter(
        (F.col("sector_code") == "Intersectoral")
        & (F.col("admin_level") == 0)
    ) \
    .groupBy("location_code", "year") \
    .agg(
        needs_metric("AFF").alias("population_affected"),
        needs_metric("INN").alias("population_in_need"),
        needs_metric("TGT").alias("population_targeted"),
        needs_metric("REA").alias("population_reached")
    ) \
    .withColumn(
        "aid_gap",
        F.when(
            F.col("population_in_need").isNull() | F.col("population_targeted").isNull(),
            None
        ).otherwise(F.col("population_in_need") - F.col("population_targeted"))
    ) \
    .withColumn(
        "aid_gap_ratio",
        F.round(
            F.when(
                F.col("population_targeted").isNull() | (F.col("population_targeted") == 0),
                None
            ).otherwise(F.col("population_in_need") / F.col("population_targeted")),
            3
        )
    ) \
    .withColumn(
        "response_gap",
        F.when(
            F.col("population_reached").isNull() | (F.col("population_reached") == 0),
            None
        ).otherwise(F.col("population_in_need") - F.col("population_reached"))
    )


# Score normalizzati (min-max per anno) 


needs_scored = needs \
    .withColumn("_gap_min", F.min("aid_gap").over(w_year)) \
    .withColumn("_gap_max", F.max("aid_gap").over(w_year)) \
    .withColumn("_ratio_min", F.min("aid_gap_ratio").over(w_year)) \
    .withColumn("_ratio_max", F.max("aid_gap_ratio").over(w_year)) \
    .withColumn(
        "score_gap_absolute",
        F.round(
            F.when(
                F.col("aid_gap").isNull() | (F.col("_gap_max") == F.col("_gap_min")),
                None
            ).otherwise(
                (F.col("aid_gap") - F.col("_gap_min"))
                / (F.col("_gap_max") - F.col("_gap_min"))
            ),
            3
        )
    ) \
    .withColumn(
        "score_gap_ratio",
        F.round(
            F.when(
                F.col("aid_gap_ratio").isNull() | (F.col("_ratio_max") == F.col("_ratio_min")),
                None
            ).otherwise(
                (F.col("aid_gap_ratio") - F.col("_ratio_min"))
                / (F.col("_ratio_max") - F.col("_ratio_min"))
            ),
            3
        )
    ) \
    .drop("_gap_min", "_gap_max", "_ratio_min", "_ratio_max")



# BLOCCO 4 — FINANZIAMENTI UMANITARI (OCHA FTS)


funding_dedup = funding_df \
    .filter(
        F.col("requirements_usd").isNotNull()
        & (F.col("requirements_usd") > 0)
    ) \
    .withColumn(
        "_ingestion_ts",
        F.coalesce(F.col("ingested_at"), F.lit("1900-01-01").cast("timestamp"))
    ) \
    .groupBy("location_code", "year", "appeal_code") \
    .agg(
        F.max_by("requirements_usd", "_ingestion_ts").alias("requirements_usd"),
        F.max_by("funding_usd", "_ingestion_ts").alias("funding_usd")
    )

funding = funding_dedup \
    .groupBy("location_code", "year") \
    .agg(
        F.count("appeal_code").alias("num_appeals"),
        F.sum("requirements_usd").alias("total_required_usd"),
        F.sum("funding_usd").alias("total_received_usd")
    ) \
    .withColumn(
        "funding_gap_usd",
        F.col("total_required_usd") - F.col("total_received_usd")
    ) \
    .withColumn(
        "funding_coverage_pct",
        F.round(F.col("total_received_usd") * 100.0 / F.col("total_required_usd"), 2)
    ) \
    .withColumn(
        "is_funding_anomalous",
        F.col("funding_coverage_pct") > 100
    ) \
    .withColumn("_prev_year", F.lag("year", 1).over(w_country)) \
    .withColumn(
        "funding_coverage_lag1",
        F.when(
            F.col("_prev_year").isNull() | (F.col("year") - F.col("_prev_year") != 1),
            None
        ).otherwise(F.lag("funding_coverage_pct", 1).over(w_country))
    ) \
    .drop("_prev_year")

funding_unspecified = funding_df \
    .filter(F.col("appeal_code") == "Not specified") \
    .withColumn(
        "_ingestion_ts",
        F.coalesce(F.col("ingested_at"), F.lit("1900-01-01").cast("timestamp"))
    ) \
    .groupBy("location_code", "year") \
    .agg(F.max_by("funding_usd", "_ingestion_ts").alias("funding_outside_appeals_usd"))
    

# BLOCCO 5 — CONFLITTI (ACLED via HDX HAPI)

conflict = conflict_df \
    .groupBy("location_code", "year") \
    .agg(
        F.sum("events").alias("conflict_events_total"),
        F.sum("fatalities").alias("conflict_fatalities_total")
    ) \
    .withColumn("_prev_year", F.lag("year", 1).over(w_country)) \
    .withColumn(
        "conflict_events_lag1",
        F.when(
            F.col("_prev_year").isNull() | (F.col("year") - F.col("_prev_year") != 1),
            None
        ).otherwise(F.lag("conflict_events_total", 1).over(w_country))
    ) \
    .drop("_prev_year")


# BLOCCO 6 — SICUREZZA ALIMENTARE (IPC)
#
# ipc_phase è una stringa: '1', '2', '3', '3+', '4', '5', 'all'.
# Le fasi 3 e superiori indicano crisi, emergenza, carestia.
# '3+' va incluso — è la categoria aggregata "3 o peggio".
#
# ipc_type = 'current' esclude le proiezioni ('first projection',
# 'second projection'), che sono previsioni e non stato rilevato.
#
# COSTRUZIONE: partiamo dall'insieme dei paesi che hanno QUALSIASI dato IPC
# in quell'anno, poi marchiamo true/false dentro quell'insieme.


food_security = foodsecurity_df \
    .filter(
        (F.col("ipc_type") == "current")
        & (F.col("admin_level") == 0)
    ) \
    .groupBy("location_code", "year") \
    .agg(
        F.max(
            F.when(F.col("ipc_phase").isin("3", "3+", "4", "5"), 1).otherwise(0)
        ).alias("_food_crisis_int")
    ) \
    .withColumn("has_food_crisis", F.col("_food_crisis_int") == 1) \
    .drop("_food_crisis_int")



# COMPOSIZIONE DELLA MASTER TABLE

# FULL OUTER JOIN tra IDPs interni e rifugiati ospitati


base = idps_projection.join(
    flows,
    on=["location_code", "year"],
    how="full_outer"
)

master = base \
    .join(needs_scored, on=["location_code", "year"], how="left") \
    .join(conflict, on=["location_code", "year"], how="left") \
    .join(food_security, on=["location_code", "year"], how="left") \
    .join(funding, on=["location_code", "year"], how="left") \
    .join(funding_unspecified, on=["location_code", "year"], how="left") \
    .join(
        wb_mpm_df.select("location_code", "year", F.col("mpm_value").alias("mpi")),
        on=["location_code", "year"], how="left"
    ) \
    .join(
        wb_poverty_df.select(
            "location_code", "year",
            F.col("extreme_poverty_value").alias("extreme_poverty")
        ),
        on=["location_code", "year"], how="left"
    ) \
    .join(
        wb_gdp_df.select("location_code", "year", "gdp_per_capita"),
        on=["location_code", "year"], how="left"
    ) \
    .join(
        wb_population_df.select("location_code", "year", "total_population"),
        on=["location_code", "year"], how="left"
    )


# Carico totale di displacement 


master = master \
    .withColumn(
        "total_displacement_burden",
        F.coalesce(F.col("internal_idps"), F.lit(0))
        + F.coalesce(F.col("stock_hosted"), F.lit(0))
    ) \
    .withColumn(
        "displaced_per_1000_inhabitants",
        F.round(
            F.when(
                F.col("total_population").isNull() | (F.col("total_population") == 0),
                None
            ).otherwise(
                F.col("total_displacement_burden") * 1000.0 / F.col("total_population")
            ),
            3
        )
    )



# CONDIZIONI DI RISCHIO

master = master \
    .withColumn(
        "is_idps_rising",
        F.when(F.col("idps_yoy_growth_pct").isNull(), None)
         .otherwise(F.col("idps_yoy_growth_pct") > 0)
    ) \
    .withColumn(
        "is_underfunded",
        F.when(F.col("funding_coverage_pct").isNull(), None)
         .otherwise(F.col("funding_coverage_pct") < UNDERFUNDED_THRESHOLD_PCT)
    ) \
    .withColumn(
        "active_risk_conditions",
        F.when(F.col("is_idps_rising"), 1).otherwise(0)
        + F.when(F.col("is_underfunded"), 1).otherwise(0)
        + F.when(F.col("has_food_crisis"), 1).otherwise(0)
    ) \
    .withColumn(
        "evaluable_risk_conditions",
        F.when(F.col("is_idps_rising").isNotNull(), 1).otherwise(0)
        + F.when(F.col("is_underfunded").isNotNull(), 1).otherwise(0)
        + F.when(F.col("has_food_crisis").isNotNull(), 1).otherwise(0)
    )


#Selezione e ordinamento colonne

master = master.select(
    # Chiave
    "location_code",
    "year",

    # Displacement interno
    "internal_idps",
    "internal_idps_lag1",
    "idps_yoy_growth_pct",
    "idps_acceleration",

    # Proiezione
    "idps_moving_avg_3y",
    "idps_naive_projection_1y",
    "projection_divergence_pct",

    # Flussi transfrontalieri
    "stock_hosted",
    "tracked_outflow",
    "real_inflow",
    "untracked_outflow",

    # Carico complessivo
    "total_displacement_burden",
    "displaced_per_1000_inhabitants",

    # Bisogno umanitario
    "population_affected",
    "population_in_need",
    "population_targeted",
    "population_reached",
    "aid_gap",
    "aid_gap_ratio",
    "response_gap",
    "score_gap_absolute",
    "score_gap_ratio",

    # Conflitti
    "conflict_events_total",
    "conflict_fatalities_total",
    "conflict_events_lag1",

    # Sicurezza alimentare
    "has_food_crisis",

    # Finanziamenti
    "num_appeals",
    "total_required_usd",
    "total_received_usd",
    "funding_gap_usd",
    "funding_coverage_pct",
    "funding_coverage_lag1",
    "is_funding_anomalous",
    "funding_outside_appeals_usd",

    # Contesto socioeconomico
    "gdp_per_capita",
    "mpi",
    "extreme_poverty",
    "total_population",

    # Condizioni di rischio
    "is_idps_rising",
    "is_underfunded",
    "active_risk_conditions",
    "evaluable_risk_conditions",
)

master = master.filter(
    (F.col("year") >= YEAR_MIN) & (F.col("year") <= YEAR_MAX)
)




master.cache()

quality = master.agg(
    F.count(F.lit(1)).alias("total_rows"),
    F.countDistinct("location_code").alias("distinct_countries"),
    F.min("year").alias("year_min"),
    F.max("year").alias("year_max"),
    F.count("internal_idps").alias("internal_idps_non_null"),
    F.count("stock_hosted").alias("stock_hosted_non_null"),
    F.count("population_in_need").alias("population_in_need_non_null"),
    F.count("funding_coverage_pct").alias("funding_coverage_non_null"),
    F.count("mpi").alias("mpi_non_null"),
    F.count("extreme_poverty").alias("extreme_poverty_non_null"),
    F.count("gdp_per_capita").alias("gdp_non_null"),
).collect()[0]

print("=" * 70)
print("REPORT DI QUALITÀ — gold.aid_demand_analysis")
print("=" * 70)
print(f"Righe totali:                {quality['total_rows']}")
print(f"Paesi distinti:              {quality['distinct_countries']}")
print(f"Range anni:                  {quality['year_min']}-{quality['year_max']}")
print(f"internal_idps non-null:      {quality['internal_idps_non_null']}")
print(f"stock_hosted non-null:       {quality['stock_hosted_non_null']}")
print(f"population_in_need non-null: {quality['population_in_need_non_null']}")
print(f"funding_coverage non-null:   {quality['funding_coverage_non_null']}")
print(f"mpi non-null:                {quality['mpi_non_null']}")
print(f"extreme_poverty non-null:    {quality['extreme_poverty_non_null']}")
print(f"gdp_per_capita non-null:     {quality['gdp_non_null']}")
print("=" * 70)

# WRITING

spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://lakehouse/gold'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.aid_demand_analysis
    USING delta
    LOCATION 's3a://lakehouse/gold/aid_demand_analysis'
""")

master.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://lakehouse/gold/aid_demand_analysis")

print("gold.aid_demand_analysis: scrittura completata")

master.unpersist()
spark.stop()