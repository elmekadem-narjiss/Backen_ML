import redis
import psycopg2
from influxdb_client import InfluxDBClient , Point
from app.config.config import REDIS_HOST, REDIS_PORT, INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, PG_DBNAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT
from app.config.config import MQTT_BROKER, MQTT_TOPIC

# Connexion Redis
def connect_redis():
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.ping()  # Vérifier la connexion
        print("✅ Connexion réussie à Redis")
        return redis_client
    except Exception as e:
        print(f"❌ Erreur Redis : {e}")
        return None

#conn influx
from app.config.config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
from influxdb_client import InfluxDBClient

def connect_influxdb():
    try:
        print("🌐 Tentative de connexion à InfluxDB...")

        # Vérification des variables chargées depuis config.py
        print("🔍 Vérification des variables :")
        print(f"  - INFLUX_URL: {INFLUX_URL}")
        print(f"  - INFLUX_TOKEN: {'Présent' if INFLUX_TOKEN else '❌ Manquant'}")
        print(f"  - INFLUX_ORG: {INFLUX_ORG}")
        print(f"  - INFLUX_BUCKET: {INFLUX_BUCKET}")

        if not INFLUX_TOKEN:
            raise ValueError("❌ Le token INFLUXDB est manquant. Vérifiez config.py !")

        client = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG
        )

        if client.ping():
            print("✅ Connexion réussie à InfluxDB.")
        else:
            print("⚠️ Connexion établie mais problème détecté.")

        return client

    except Exception as e:
        print(f"❌ Erreur de connexion à InfluxDB: {e}")
        return None

# Tester la connexion
client = connect_influxdb()



# Connexion PostgreSQL
def connect_postgresql():
    try:
        pg_conn = psycopg2.connect(
            dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
        )
        pg_cursor = pg_conn.cursor()
        print("✅ Connexion réussie à PostgreSQL")
        return pg_conn, pg_cursor
    except Exception as e:
        print(f"❌ Erreur PostgreSQL : {e}")
        return None, None


# Connexion MQTT
# db_connection.py



import paho.mqtt.client as mqtt

# Exemple de fonction MQTT
# Fonction de connexion MQTT
def connect_mqtt():
    """Se connecter au broker MQTT et s'abonner à un topic."""
    client = mqtt.Client()

    # Définir la fonction de rappel pour les messages reçus
    client.on_message = on_message  # Assigner la fonction on_message ici

    try:
        # Connexion au broker MQTT
        client.connect(MQTT_BROKER, 1883, 60)
        print(f"✅ Connexion réussie au broker MQTT : {MQTT_BROKER}")

        # S'abonner au topic MQTT
        client.subscribe(MQTT_TOPIC)
        print(f"✅ Abonné au topic : {MQTT_TOPIC}")

        # Démarrer la boucle MQTT pour recevoir les messages
        client.loop_start()
    except Exception as e:
        print(f"❌ Erreur lors de la connexion à MQTT : {e}")

# import paho.mqtt.client as mqtt

# Fonction de traitement des messages MQTT
def on_message(client, userdata, message):
    """Traitement des messages reçus sur le topic MQTT."""
    try:
        # Décoder le message reçu
        data = message.payload.decode()

        # Exemple de traitement des données - Ajustez selon votre format de message
        # Supposons que le message soit un JSON avec des données de consommation d'énergie
        energy_data = data.split(",")  # Cela dépend du format de vos données MQTT
        timestamp = energy_data[0]
        energy_consumption = float(energy_data[1])

        # Connexion à InfluxDB et ajout des données
        influx_client = connect_influxdb()
        if influx_client:
            write_api = influx_client.write_api()
            point = Point("energy_consumption") \
                .tag("location", "building_1") \
                .field("energy_consumption", energy_consumption) \
                .time(timestamp)
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"✅ Données envoyées dans InfluxDB : {timestamp}, {energy_consumption}")
    except Exception as e:
        print(f"❌ Erreur lors du traitement du message MQTT : {e}")



# Fonction de connexion MQTT
def connect_mqtt():
    """Se connecter au broker MQTT et s'abonner à un topic."""
    client = mqtt.Client()

    # Définir la fonction de rappel pour les messages reçus
    client.on_message = on_message  # Assigner la fonction on_message à client.on_message

    try:
        # Connexion au broker MQTT
        client.connect(MQTT_BROKER, 1883, 60)
        print(f"✅ Connexion réussie au broker MQTT : {MQTT_BROKER}")

        # S'abonner au topic MQTT
        client.subscribe(MQTT_TOPIC)
        print(f"✅ Abonné au topic : {MQTT_TOPIC}")

        # Démarrer la boucle MQTT pour recevoir les messages
        client.loop_start()
    except Exception as e:
        print(f"❌ Erreur lors de la connexion à MQTT : {e}")


# Appel de la fonction de connexion MQTT
connect_mqtt()