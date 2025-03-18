import redis
import psycopg2
from influxdb_client import InfluxDBClient
from config import REDIS_HOST, REDIS_PORT, INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, PG_DBNAME, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT

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

# Connexion InfluxDB
def connect_influxdb():
    try:
        influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        print("✅ Connexion réussie à InfluxDB")
        return influx_client
    except Exception as e:
        print(f"❌ Erreur InfluxDB : {e}")
        return None

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
