from kafka import KafkaProducer
import json

# Initialisation du Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'flight-positions'
message = {"icao24": "test_plane", "vitesse": 850, "lat": 48.8566, "lon": 2.3522}

print(f"Envoi du message vers le topic '{topic}'...")
producer.send(topic, message)

# Forcer l'envoi immédiat
producer.flush()
print("✅ Message envoyé avec succès dans Kafka !")
