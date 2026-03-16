#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Producer que conecta a Binance WebSocket (kline 1m) y publica en Kafka
solo cuando la vela está cerrada. El mensaje JSON contiene:

- symbol: símbolo (BNBUSD)
- @timestamp: tiempo de cierre en UTC ISO (ej: 2026-03-09T11:21:00Z)
- timestamp_ms: tiempo de cierre en milisegundos (entero)
- close: precio de cierre (float)
- volume: volumen (float)

Configurar `BOOTSTRAP_SERVERS` y `TOPIC` abajo.
"""

import json
import logging
from datetime import datetime
from binance import ThreadedWebsocketManager, Client
from kafka import KafkaProducer

# --- Configuración ---
BOOTSTRAP_SERVERS = "192.168.80.34:9092"  # Cambia si hace falta
TOPIC = "gittba_BNB"
SYMBOL = "BNBUSD"
INTERVAL = Client.KLINE_INTERVAL_1MINUTE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
    )


def kline_callback_factory(producer: KafkaProducer, topic: str):
    def handle_kline(msg):
        k = msg.get("k", {})
        # `x` indica que la vela está cerrada
        if not k.get("x"):
            return

        # Usar T (close time) provisto por Binance en ms
        ts_ms = k.get("T")
        if ts_ms is None:
            logger.warning("Mensaje kline sin T: %s", k)
            return

        # Formato ISO UTC con Z, obtenido desde el timestamp de Binance
        ts_iso = datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "symbol": SYMBOL,
            "@timestamp": ts_iso,
            "close": float(k.get("c", 0.0)),
            "volume": float(k.get("v", 0.0)),
        }

        key = SYMBOL

        try:
            producer.send(topic, key=key, value=payload, timestamp_ms=ts_ms) # forzar timestamp en mili
            producer.flush()
            logger.info("Publicado en Kafka: %s", payload)
        except Exception as e:
            logger.exception("Error enviando a Kafka: %s", e)

    return handle_kline


def main():
    producer = make_producer()

    twm = ThreadedWebsocketManager()
    twm.start()

    callback = kline_callback_factory(producer, TOPIC)

    logger.info("Iniciando socket kline para %s %s", SYMBOL, INTERVAL)
    twm.start_kline_socket(symbol=SYMBOL, interval=INTERVAL, callback=callback)

    try:
        input("Pulsa ENTER para detener el productor y cerrar sockets\n")
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Cerrando websocket y productor...")
        try:
            twm.stop()
        except Exception:
            pass
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
