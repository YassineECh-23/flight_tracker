import json
import time
import signal
import sys
import logging
import requests
from requests.exceptions import RequestException
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ==========================================
# 1. CONFIGURATION DU LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 2. CONSTANTES ET CREDENTIALS
# ==========================================
KAFKA_TOPIC = 'flight-positions'
OPENSKY_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

try:
    with open('credentials.json', 'r') as f:
        creds = json.load(f)
except FileNotFoundError:
    logger.critical("credentials.json introuvable.")
    sys.exit(1)

# ==========================================
# 3. FONCTION AUTH OAUTH2
# ==========================================
def get_token():
    client = BackendApplicationClient(client_id=creds['clientId'])
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(
        token_url=TOKEN_URL,
        client_id=creds['clientId'],
        client_secret=creds['clientSecret']
    )
    return token['access_token']

# ==========================================
# 4. INITIALISATION KAFKA
# ==========================================
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        request_timeout_ms=10000
    )
    logger.info("Connexion au broker Kafka réussie.")
except Exception as e:
    logger.critical(f"Kafka est injoignable : {e}")
    sys.exit(1)

# ==========================================
# 5. GRACEFUL SHUTDOWN
# ==========================================
def shutdown_handler(sig, frame):
    logger.info("Arrêt propre en cours...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ==========================================
# 6. BOUCLE PRINCIPALE
# ==========================================
logger.info("Démarrage de l'ingestion OpenSky -> Kafka.")

while True:
    try:
        token = get_token()
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(OPENSKY_URL, headers=headers, timeout=10)

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
                        "lat":    s[6],
                        "lon":    s[5],
                        "altitude": s[7],
                        "vitesse":  vitesse_kmh,
                        "cap":      s[10],
                        "ts":       s[3] or int(time.time())
                    }

                    producer.send(KAFKA_TOPIC, vol)
                    count += 1

                producer.flush()
                logger.info(f"{count} avions publiés dans Kafka.")
            else:
                logger.warning("Réponse 200 mais liste 'states' vide.")

        elif response.status_code == 429:
            logger.warning("Rate limit atteint. Attente 60s.")
            time.sleep(60)
        else:
            logger.error(f"Erreur API OpenSky: {response.status_code} - {response.text}")

    except RequestException as e:
        logger.error(f"Erreur réseau : {e}")
    except KafkaError as e:
        logger.error(f"Erreur Kafka : {e}")
    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}")

    time.sleep(10)