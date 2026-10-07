import os
import pandas as pd
import streamlit as st
from supabase import create_client
from dotenv import load_dotenv

# Configuration de la page
st.set_page_config(page_title="Flight Tracker Live", layout="wide")
st.title("✈️ Dashboard Flight Tracker Temps Réel")

# Connexion à Supabase (Mise en cache pour les performances)
@st.cache_resource
def init_connection():
    load_dotenv()
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

supabase = init_connection()

# Récupération des données
def get_data():
    # 2000 dernières positions pour ne pas surcharger la carte
    pos = supabase.table("fact_positions").select("*").order("ts", desc=True).limit(2000).execute()
    # 50 dernières alertes de vitesse
    anom = supabase.table("fact_anomalies").select("*").order("ts", desc=True).limit(50).execute()
    return pd.DataFrame(pos.data), pd.DataFrame(anom.data)

# Bouton d'actualisation
if st.button("🔄 Rafraîchir les données"):
    st.rerun()

df_positions, df_anomalies = get_data()

# KPIs en haut de page
col1, col2, col3 = st.columns(3)
if not df_positions.empty:
    col1.metric("Vols suivis", len(df_positions["icao24"].unique()))
    col2.metric("Altitude max (m)", round(df_positions["altitude"].max(), 2))
    col3.metric("Anomalies détectées", len(df_anomalies))

# Carte interactive
st.subheader("📍 Carte des vols en direct")
if not df_positions.empty:
    # Streamlit détecte automatiquement les colonnes 'lat' et 'lon'
    st.map(df_positions[["lat", "lon"]].dropna())
else:
    st.warning("Aucune donnée de position disponible.")

# Tableau des anomalies
st.subheader("⚠️ Dernières alertes (Vitesse > 1000 km/h)")
if not df_anomalies.empty:
    st.dataframe(df_anomalies, use_container_width=True)
else:
    st.success("Aucune anomalie critique détectée récemment.")