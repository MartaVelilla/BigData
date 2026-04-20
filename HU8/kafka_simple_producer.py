# -*- coding: utf-8 -*-

from kafka import KafkaProducer

# Configuración
BOOTSTRAP_SERVERS="192.168.80.34:9092"
TOPIC="gittba_BNB"

def main() -> None:

    # Crea el KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8")
    )

    # Crea el mensaje
    key = 'key1'
    value = "Hello World!"
    print("Mensaje a enviar: ", key + ' ' + value)

    # Envía el mensaje
    producer.send(TOPIC, key=key, value=value)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()