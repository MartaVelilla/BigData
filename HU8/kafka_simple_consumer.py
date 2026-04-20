# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timezone
import requests
from requests.auth import HTTPBasicAuth
from kafka import KafkaConsumer

# ---------------------------
# Configuración Kafka
# ---------------------------
BOOTSTRAP_SERVERS = "192.168.80.34:9092"
GROUP_ID = "gittba05"

# Mapeo de tópicos -> índices (editar según nombres reales de tópicos)
# TOPIC_RAW: tópico que contiene mensajes con 'close' y 'volume' -> índice `gittba_bnb`
# TOPIC_VWAP: tópico que contiene el VWAP calculado por Spark -> índice `gittba_bnb_vwap`
TOPIC_RAW = "gittba_BNB"
TOPIC_VWAP = "gittba_BNB_VWAP"

# ---------------------------
# Configuración Elasticsearch
# ---------------------------
ES_HOST = "192.168.80.37"
ES_PORT = 9201
ES_USER = "elastic"
ES_PASS = "pass4icai"


class ElasticWriter:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.base_url = f"http://{host}:{port}"
        self.auth = HTTPBasicAuth(user, password)
        self.headers = {"Content-Type": "application/json"}

    def write_document(self, index: str, doc: dict) -> bool:
        url = f"{self.base_url}/{index}/_doc"
        try:
            resp = requests.post(url, data=json.dumps(doc), headers=self.headers, auth=self.auth, timeout=5)
            resp.raise_for_status()
            print(f"OK -> index={index} id={resp.json().get('_id')}")
            return True
        except Exception as e:
            print(f"ERROR posting to {index}: {e} - payload={doc}")
            return False


def _now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def main() -> None:
    # Inicializa consumer y writer
    consumer = KafkaConsumer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: v.decode("utf-8") if v else None,
    )

    # Nos subscribimos a ambos tópicos (raw close/volume y vwap)
    consumer.subscribe([TOPIC_RAW, TOPIC_VWAP])

    writer = ElasticWriter(ES_HOST, ES_PORT, ES_USER, ES_PASS)

    print(f"Listening topics: {TOPIC_RAW} (-> gittba_bnb), {TOPIC_VWAP} (-> gittba_bnb_vwap)")

    try:
        for message in consumer:
            topic = message.topic
            raw_value = message.value
            key = message.key

            # Intenta parsear JSON, si no es JSON guardamos el raw
            payload = None
            try:
                payload = json.loads(raw_value) if raw_value else {}
            except Exception:
                payload = {"raw": raw_value}

            # Construye documento según tópico
            if topic == TOPIC_RAW:
                index = "gittba_bnb"
                doc = {
                    "topic": topic,
                    "symbol": payload.get("symbol", payload.get("s", key or "BNBUSDT")),
                    "@timestamp": payload.get("@timestamp") or payload.get("timestamp") or _now_iso(),
                }
                # Campos numéricos opcionales
                if "close" in payload:
                    try:
                        doc["close"] = float(payload["close"])
                    except Exception:
                        doc["close"] = payload["close"]
                if "volume" in payload:
                    try:
                        doc["volume"] = float(payload["volume"])
                    except Exception:
                        doc["volume"] = payload["volume"]

                # Si no encontramos close/volume, guardamos el mensaje original para depuración
                if "close" not in doc and "volume" not in doc:
                    doc["raw"] = raw_value

            elif topic == TOPIC_VWAP:
                index = "gittba_bnb_vwap"
                doc = {
                    "topic": topic,
                    "symbol": payload.get("symbol", payload.get("s", key or "BNBUSDT")),
                    "@timestamp": payload.get("@timestamp") or payload.get("timestamp") or _now_iso(),
                }
                # Campo VWAP esperado
                if "vwap" in payload:
                    try:
                        doc["vwap"] = float(payload["vwap"])
                    except Exception:
                        doc["vwap"] = payload["vwap"]
                else:
                    # A veces la key puede llamarse 'VWAP' o estar en 'value'
                    if "VWAP" in payload:
                        doc["vwap"] = payload.get("VWAP")
                    else:
                        doc["raw"] = raw_value

            else:
                # tópico desconocido: lo ignoramos
                print(f"Skipping unknown topic: {topic}")
                continue

            print(f"Received from topic={topic} -> sending to index={index}: {doc}")
            writer.write_document(index, doc)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()