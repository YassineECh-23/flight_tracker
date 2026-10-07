import os
import json
import math
import signal
import threading
from datetime import datetime, timezone

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

from supabase import create_client
from dotenv import load_dotenv


# =========================================================
# CONFIGURATION
# =========================================================

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

load_dotenv()


# =========================================================
# VERSION PYSPARK / KAFKA
# =========================================================

spark_version = pyspark.__version__

kafka_pkg = (
    f"org.apache.spark:"
    f"spark-sql-kafka-0-10_2.12:"
    f"{spark_version}"
)

print(
    f"🚀 Démarrage de PySpark "
    f"(Version: {spark_version})..."
)


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("FlightTracker")
    .config("spark.jars.packages", kafka_pkg)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# GESTION PROPRE DE CTRL+C
# =========================================================

stop_event = threading.Event()


def handle_sigint(signum, frame):
    print("\n🛑 Ctrl+C détecté : arrêt du streaming...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_sigint)


# =========================================================
# SCHEMA DES MESSAGES KAFKA
# =========================================================

schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("vitesse", DoubleType(), True),
    StructField("cap", DoubleType(), True),
    StructField("ts", LongType(), True)
])


# =========================================================
# LECTURE KAFKA
# =========================================================

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "flight-positions"
    )
    .load()
)


# =========================================================
# PARSING JSON
# =========================================================

parsed_df = (
    df
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)


# =========================================================
# NETTOYAGE DES VALEURS
# =========================================================

def clean_value(value):
    """
    Convertit les types NumPy/Pandas vers Python
    et transforme NaN / Infinity en None.
    """

    if value is None:
        return None

    # NumPy / Pandas
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    # Float
    if isinstance(value, float):

        if not math.isfinite(value):
            return None

        return value

    # Entier
    if isinstance(value, int):
        return value

    # Dictionnaire
    if isinstance(value, dict):
        return {
            key: clean_value(val)
            for key, val in value.items()
        }

    # Liste
    if isinstance(value, list):
        return [
            clean_value(val)
            for val in value
        ]

    return value


# =========================================================
# CONVERSION JSON SECURISEE
# =========================================================

def to_json_safe(data):
    """
    Vérifie qu'aucun NaN / Infinity ne reste
    avant l'envoi à Supabase.
    """

    cleaned_data = clean_value(data)

    return json.loads(
        json.dumps(
            cleaned_data,
            allow_nan=False
        )
    )


# =========================================================
# CONVERSION TIMESTAMP
# =========================================================

def convert_timestamp(ts):
    """
    Convertit un timestamp Unix en timestamp ISO
    compatible avec PostgreSQL / Supabase.

    Supporte :
    - secondes
    - millisecondes
    """

    if ts is None:
        return None

    try:

        ts = int(ts)

        # Timestamp en millisecondes
        if ts > 100000000000:

            dt = datetime.fromtimestamp(
                ts / 1000,
                tz=timezone.utc
            )

        # Timestamp en secondes
        else:

            dt = datetime.fromtimestamp(
                ts,
                tz=timezone.utc
            )

        return dt.isoformat()

    except (
        TypeError,
        ValueError,
        OverflowError,
        OSError
    ):

        return None


# =========================================================
# TRAITEMENT D'UN BATCH
# =========================================================

def process_batch(batch_df, batch_id):

    try:

        # ---------------------------------------------
        # Conversion Spark -> Pandas
        # ---------------------------------------------

        pdf = batch_df.toPandas()

        if pdf.empty:
            return

        print(
            f"📦 Batch {batch_id} : "
            f"{len(pdf)} lignes reçues"
        )


        # ---------------------------------------------
        # VERIFICATION SUPABASE
        # ---------------------------------------------

        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")

        if not supabase_url or not supabase_key:

            print(
                "❌ SUPABASE_URL ou SUPABASE_KEY "
                "est manquant dans le fichier .env"
            )

            return


        supabase = create_client(
            supabase_url,
            supabase_key
        )


        # ---------------------------------------------
        # LISTES
        # ---------------------------------------------

        positions = []
        anomalies = []


        # ---------------------------------------------
        # TRAITEMENT DES LIGNES
        # ---------------------------------------------

        for _, row in pdf.iterrows():

            # =========================================
            # NETTOYAGE
            # =========================================

            icao24 = clean_value(row["icao24"])
            lat = clean_value(row["lat"])
            lon = clean_value(row["lon"])
            altitude = clean_value(row["altitude"])
            vitesse = clean_value(row["vitesse"])
            cap = clean_value(row["cap"])
            ts = clean_value(row["ts"])


            # =========================================
            # TIMESTAMP
            # =========================================

            ts = convert_timestamp(ts)


            # =========================================
            # FACT_POSITIONS
            # =========================================

            positions.append({
                "icao24": icao24,
                "lat": lat,
                "lon": lon,
                "altitude": altitude,
                "vitesse": vitesse,
                "cap": cap,
                "ts": ts
            })


            # =========================================
            # DETECTION ANOMALIE
            # =========================================

            try:

                if vitesse is not None:

                    vitesse_float = float(vitesse)

                    # Anomalie si vitesse > 1000 km/h
                    if (
                        math.isfinite(vitesse_float)
                        and vitesse_float > 1000
                    ):

                        anomalies.append({
                            "icao24": icao24,
                            "type_anomalie": "Vitesse excessive",
                            "valeur": vitesse_float,
                            "gravite": "haute",
                            "ts": ts
                        })

            except (
                TypeError,
                ValueError
            ):

                pass


        # =================================================
        # INSERTION FACT_POSITIONS
        # =================================================

        if positions:

            try:

                safe_positions = to_json_safe(
                    positions
                )

                (
                    supabase
                    .table("fact_positions")
                    .insert(safe_positions)
                    .execute()
                )

                print(
                    f"✅ {len(positions)} positions "
                    f"enregistrées."
                )

            except Exception as e:

                print(
                    f"❌ Erreur fact_positions : {e}"
                )


        # =================================================
        # INSERTION FACT_ANOMALIES
        # =================================================

        if anomalies:

            try:

                safe_anomalies = to_json_safe(
                    anomalies
                )

                (
                    supabase
                    .table("fact_anomalies")
                    .insert(safe_anomalies)
                    .execute()
                )

                print(
                    f"⚠️ {len(anomalies)} anomalies "
                    f"enregistrées."
                )

            except Exception as e:

                print(
                    f"❌ Erreur fact_anomalies : {e}"
                )

        else:

            print(
                "✅ Aucune anomalie détectée."
            )


    except Exception as e:

        print(
            f"❌ Erreur batch {batch_id} : {e}"
        )


# =========================================================
# DEMARRAGE DU STREAMING
# =========================================================

print(
    "📡 En attente de données Kafka..."
)


query = (
    parsed_df
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .start()
)


# =========================================================
# ATTENTE
# =========================================================

try:

    while not stop_event.is_set():

        terminated = query.awaitTermination(1)

        if terminated:
            break


except Exception as e:

    print(
        f"\n❌ Erreur du streaming : {e}"
    )


finally:

    print(
        "\n🛑 Arrêt de la requête Spark..."
    )

    try:

        if query.isActive:
            query.stop()

    except Exception:
        pass


    print(
        "🛑 Arrêt de Spark..."
    )

    try:

        spark.stop()

    except Exception:
        pass


    print(
        "✅ Spark arrêté proprement."
    )