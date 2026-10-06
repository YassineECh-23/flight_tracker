import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Charger les variables du fichier .env
load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

# Initialiser le client
supabase: Client = create_client(url, key)

try:
    # Tenter de lire la table fact_positions (vide pour l'instant)
    response = supabase.table('fact_positions').select("*").limit(1).execute()
    print("✅ Connexion à Supabase réussie !")
    print("Données actuelles :", response.data)
except Exception as e:
    print("❌ Erreur de connexion :", e)
