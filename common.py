"""Configuración, esquema y cliente S3 compartidos por collector.py y compact.py."""
import os
import sys

import boto3
import pyarrow as pa
from botocore.config import Config

# Esquema fijo: todos los archivos tienen exactamente estos tipos, así la
# compactación y las consultas (DuckDB/Polars) no sufren por inferencia de tipos.
# float32 en lat/lon: FR24 entrega 4 decimales (~11 m); float32 a 180° tiene
# resolución ~1.5e-5° (~1.7 m), así que no se pierde información y ahorra espacio.
SCHEMA = pa.schema([
    ("captura_utc", pa.timestamp("s", tz="UTC")),   # cuándo corrió el collector
    ("posicion_utc", pa.timestamp("s", tz="UTC")),  # timestamp de la posición según FR24
    ("id", pa.string()),                 # id de vuelo de FR24 (clave de trayectoria)
    ("icao24", pa.string()),             # transponder hex
    ("aerolinea", pa.string()),          # ICAO de aerolínea
    ("vuelo", pa.string()),              # callsign
    ("numero", pa.string()),             # número de vuelo comercial
    ("matricula", pa.string()),
    ("tipo_aeronave", pa.string()),
    ("origen", pa.string()),             # IATA
    ("destino", pa.string()),            # IATA
    ("latitud", pa.float32()),
    ("longitud", pa.float32()),
    ("altitud", pa.int32()),             # ft
    ("velocidad_nudos", pa.int32()),     # kts
    ("rumbo", pa.int16()),               # grados
    ("velocidad_vertical", pa.int32()),  # ft/min
    ("en_tierra", pa.bool_()),
    ("squawk", pa.string()),
])


def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def require(name):
    v = env(name)
    if v is None:
        sys.exit(f"Falta la variable de entorno {name}")
    return v


PREFIX = (env("S3_PREFIX", "vuelos")).strip("/")


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
