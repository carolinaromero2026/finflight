"""Configuración, columnas y cliente S3 compartidos por collector.py y compact.py."""
import os
import sys

import boto3
from botocore.config import Config

# Las 9 columnas del notebook, en el mismo orden y con los mismos nombres.
# Sus valores se escriben tal como los entrega la librería (incluido "N/A" y ""),
# igual que hacía el notebook con DataFrame(...).to_csv().
COLUMNAS_NOTEBOOK = [
    "fecha_hora_captura", "id", "aerolinea", "vuelo", "latitud", "longitud",
    "altitud", "velocidad_nudos", "rumbo",
]
# Columnas agregadas, siempre AL FINAL para no mover las originales.
COLUMNAS_EXTRA = [
    "captura_utc",          # misma captura que fecha_hora_captura, en UTC ISO-8601 (sin ambigüedad de horario de verano)
    "posicion_utc",         # timestamp de la posición según FR24
    "icao24", "numero", "matricula", "tipo_aeronave", "origen", "destino",
    "velocidad_vertical", "squawk",
    "en_muestra_global",    # True = la fila viene de la misma request única que hacía el notebook
]
COLUMNAS = COLUMNAS_NOTEBOOK + COLUMNAS_EXTRA


def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def require(name):
    v = env(name)
    if v is None:
        sys.exit(f"Falta la variable de entorno {name}")
    return v


PREFIX = (env("S3_PREFIX", "vuelos-csv")).strip("/")


def bucket():
    return require("S3_BUCKET")


def s3_client():
    endpoint = require("S3_ENDPOINT")
    if not endpoint.startswith(("http://", "https://")):
        endpoint = "https://" + endpoint
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=require("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=require("S3_SECRET_ACCESS_KEY"),
        region_name=env("S3_REGION", "auto"),
        config=Config(
            s3={"addressing_style": env("S3_ADDRESSING_STYLE", "auto")},
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=10,
            read_timeout=60,
            # boto3 >= 1.36 agrega checksums CRC por defecto, que algunos
            # proveedores S3-compatibles no aceptan. Solo cuando sea obligatorio.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )