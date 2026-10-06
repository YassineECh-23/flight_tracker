import os
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from supabase import create_client, Client
from dotenv import load_dotenv


#
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1" 
#
load_dotenv()

# Récupération dynamique de la version exacte de ton PySpark
spark_version = pyspark.__version__
kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"

# 1. Initialisation de Spark avec le connecteur dynamique
print(f"🚀 Démarrage de PySpark (Version: {spark_version})...")
spark = SparkSession.builder \
    .appName("FlightTracker") \
    .config("spark.jars.packages", kafka_pkg) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Définition du schéma des données JSON
schema = StructType([
    StructField("icao24", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("altitude", DoubleType()),
    StructField("vitesse", DoubleType()),
    StructField("cap", DoubleType()),
    StructField("ts", LongType())
])

# 3. Lecture en continu depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight-positions") \
    .load()

# Convertir la colonne "value" (binaire) en JSON lisible
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 4. Fonction exécutée pour CHAQUE lot de données (Micro-Batch)
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
        
    # Reconnecter Supabase dans les workers Spark
    supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
    pdf = batch_df.toPandas()
    
    positions = []
    anomalies = []

    for _, row in pdf.iterrows():
        # Préparer la position
        positions.append({
            "icao24": row["icao24"],
            "lat": row["lat"],
            "lon": row["lon"],
            "altitude": row["altitude"],
            "vitesse": row["vitesse"],
            "cap": row["cap"]
        })
        
        # 🚨 DÉTECTION D'ANOMALIE : Règle n°3 (Vitesse > 1000 km/h)
        if row["vitesse"] and row["vitesse"] > 1000:
            anomalies.append({
                "icao24": row["icao24"],
                "type_anomalie": "Vitesse excessive (>1000 km/h)",
                "description": f"Enregistré à {round(row['vitesse'], 2)} km/h",
                "lat": row["lat"],
                "lon": row["lon"]
            })

    # Insertion dans Supabase
    try:
        if positions:
            supabase.table("fact_positions").insert(positions).execute()
        if anomalies:
            supabase.table("fact_anomalies").insert(anomalies).execute()
            print(f"⚠️ {len(anomalies)} anomalies détectées et insérées !")
        print(f"✅ Lot {batch_id} : {len(positions)} positions enregistrées.")
    except Exception as e:
        print(f"Erreur d'insertion Supabase : {e}")

# 5. Lancement du flux de traitement
print("📡 En attente de données Kafka...")
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()