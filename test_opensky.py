import json
import requests

# 1. Charger les identifiants
with open('credentials.json', 'r') as f:
    creds = json.load(f)

# 2. Appel API avec Basic Auth
url = "https://opensky-network.org/api/states/all"
print("Appel de l'API OpenSky en cours (ça peut prendre quelques secondes)...")
response = requests.get(url, auth=(creds['username'], creds['password']))

# 3. Vérification et affichage
if response.status_code == 200:
    data = response.json()
    states = data.get('states', [])
    print(f"✅ Connexion réussie ! Vols en cours dans le monde : {len(states)}")
    print("Voici les 5 premières positions :")
    for avion in states[:5]:
        icao24 = avion[0]
        lat = avion[6]
        lon = avion[5]
        alt = avion[7] # Altitude barométrique
        vitesse = avion[9] # m/s
        print(f"- ICAO24: {icao24} | Lat: {lat}, Lon: {lon} | Alt: {alt}m | Vitesse: {vitesse}m/s")
else:
    print(f"❌ Erreur {response.status_code} : {response.text}")
