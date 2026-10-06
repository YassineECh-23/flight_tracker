import json
import time
import signal
import sys
import logging
import requests
from requests.exceptions import RequestException
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ==========================================
# 1. CONFIGURATION DU LOGGING (PROD)
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("producer.log"), # Trace écrite pour le debug
        logging.StreamHandler(sys.stdout)    # Trace console
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 2. CONSTANTES ET IDENTIFIANTS
# ==========================================
KAFKA_TOPIC = 'flight-positions'
OPENSKY_URL = "https://opensky-network.org/api/states/all"

try:
    with open('credentials.json', 'r') as f:
        creds = json.load(f)
except FileNotFoundError:
    logger.critical("Le fichier credentials.json est introuvable. Arrêt du script.")
    sys.exit(1)

# ==========================================
# 3. INITIALISATION KAFKA AVEC SÉCURITÉ
# ==========================================
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        request_timeout_ms=10000
    )
    logger.info("Connexion au broker Kafka réussie.")
except Exception as e: # <-- Modification ici
    logger.critical(f"Kafka est injoignable : {e}")
    sys.exit(1)
    
# ==========================================
# 4. GRACEFUL SHUTDOWN (Arrêt propre)
# ==========================================
def shutdown_handler(sig, frame):
    logger.info("Signal d'arrêt reçu. Vidage des messages en attente et fermeture de Kafka...")
    producer.flush()
    producer.close()
    logger.info("Fermeture propre terminée.")
    sys.exit(0)

# Capture du Ctrl+C (SIGINT) et de l'arrêt système (SIGTERM)
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ==========================================
# 5. BOUCLE PRINCIPALE (INGESTION)
# ==========================================
logger.info("Démarrage de l'ingestion OpenSky -> Kafka.")

while True:
    try:
        response = requests.get(OPENSKY_URL, auth=(creds['username'], creds['password']), timeout=10)
        
        if response.status_code == 200:
            states = response.json().get('states', [])
            
            if states:
                count = 0
                for s in states:
                    if s[5] is None or s[6] is None:
                        continue
                        
                    vitesse_ms = s[9]
                    vitesse_kmh = (vitesse_ms * 3.6) if vitesse_ms else None
                    
                    vol = {
                        "icao24": s[0],
                        "lat": s[6],
                        "lon": s[5],
                        "altitude": s[7],
                        "vitesse": vitesse_kmh,
                        "cap": s[10],
                        "ts": s[3] or int(time.time())
                    }
                    
                    producer.send(KAFKA_TOPIC, vol)
                    count += 1
                
                producer.flush()
                logger.info(f"{count} avions publiés dans Kafka.")
            else:
                logger.warning("L'API a répondu 200 mais la liste 'states' est vide.")
                
        elif response.status_code == 429:
            logger.warning("Limite d'API atteinte (Rate Limit). Attente prolongée.")
            time.sleep(30) # On patiente plus longtemps si on est bloqué
        else:
            logger.error(f"Erreur API OpenSky: {response.status_code} - {response.text}")
            
    except RequestException as e:
        logger.error(f"Erreur réseau lors de l'appel à OpenSky : {e}")
    except KafkaError as e:
        logger.error(f"Erreur lors de l'envoi à Kafka : {e}")
    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}") # .exception capture toute la stacktrace

    time.sleep(10)