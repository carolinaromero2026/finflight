"""Una corrida: descarga posiciones de FR24 y sube un CSV (gzip) al bucket.

Diseñado para Railway cron: hace su trabajo y TERMINA. Si el proceso no sale,
Railway se salta las ejecuciones siguientes, por eso hay un timeout duro.

Formato: las 9 columnas del notebook, idénticas (nombre, orden, valores), y
columnas extra al final. Mismo filtro: solo vuelos con `not vuelo.on_ground`.

Modos (variable MODO):
  global -> solo la request única del notebook (FR24 la corta en ~1500 vuelos).
  tiles  -> la request única del notebook + cuadrantes que se subdividen cuando
            vuelven saturados. Las filas de la request única quedan marcadas con
            en_muestra_global=True y son exactamente las que daría el notebook.
"""
import gzip
import json
import signal
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from FlightRadarAPI import FlightRadar24API

from common import COLUMNAS, PREFIX, bucket, env, s3_client

MODO = env("MODO", "tiles")
TILE_LIMIT = int(env("TILE_LIMIT", "1500"))          # tope observado por request
SPLIT_FRAC = float(env("SPLIT_FRAC", "0.95"))        # subdividir si llega a 95% del tope
MAX_DEPTH = int(env("MAX_DEPTH", "5"))               # 90° / 2^5 ≈ 2.8° el tile más chico
MAX_REQUESTS = int(env("MAX_REQUESTS", "200"))
PAUSA_S = float(env("PAUSA_S", "0.3"))               # pausa entre requests
REINTENTOS = int(env("REINTENTOS", "3"))
TIMEOUT_S = int(env("RUN_TIMEOUT_S", "240"))         # < 300 s del intervalo del cron
HTTP_TIMEOUT_S = int(env("HTTP_TIMEOUT_S", "20"))
# Zona horaria de fecha_hora_captura. El notebook usaba datetime.now() de la
# máquina local; se asume que estaba en hora de Chile continental.
TZ_CAPTURA = ZoneInfo(env("TZ_CAPTURA", "America/Santiago"))


class Timeout(BaseException):
    """BaseException para que ningún `except Exception` de la librería la trague."""


def _alarma(signum, frame):
    raise Timeout()


def pedir(api, bounds=None):
    ultimo = None
    for intento in range(REINTENTOS):
        try:
            return api.get_flights(bounds=bounds)
        except Exception as e:  # Timeout no entra aquí (es BaseException)
            ultimo = e
            time.sleep(2 ** intento)
    raise ultimo


def _fmt(tile):
    n, s, w, e = tile
    return f"{n:.4f},{s:.4f},{w:.4f},{e:.4f}"


def _dividir(tile):
    n, s, w, e = tile
    mlat, mlon = (n + s) / 2, (w + e) / 2
    return [(n, mlat, w, mlon), (n, mlat, mlon, e), (mlat, s, w, mlon), (mlat, s, mlon, e)]


def recolectar_global(api, stats):
    """La misma llamada del notebook: fr_api.get_flights() sin argumentos."""
    stats["requests"] += 1
    return pedir(api)


def recolectar_tiles(api, stats):
    """Devuelve un dict id -> vuelo con la cobertura por cuadrantes."""
    umbral = int(TILE_LIMIT * SPLIT_FRAC)
    # 8 tiles iniciales de 90°x90° (norte, sur, oeste, este)
    pila = [((n, n - 90, w, w + 90), 0) for n in (90, 0) for w in (-180, -90, 0, 90)]
    vuelos = {}
    while pila:
        if stats["requests"] >= MAX_REQUESTS:
            stats["tiles_sin_pedir"] = len(pila)
            break
        tile, prof = pila.pop()
        stats["requests"] += 1
        try:
            res = pedir(api, _fmt(tile))
        except Exception as e:
            stats["errores"] += 1
            stats["ultimo_error"] = repr(e)[:300]
            continue
        finally:
            time.sleep(PAUSA_S)

        if len(res) >= umbral and prof < MAX_DEPTH:
            # Respuesta probablemente truncada: se descarta y se piden los 4 hijos.
            stats["divisiones"] += 1
            pila.extend((hijo, prof + 1) for hijo in _dividir(tile))
            continue
        if len(res) >= umbral:
            stats["tiles_saturados"] += 1
        for v in res:
            vuelos[v.id] = v  # dedup: un vuelo en el borde puede salir en 2 tiles
    return vuelos


def _iso_utc(t):
    try:
        return datetime.fromtimestamp(int(t), timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def fila(v, fecha_hora_captura, captura_utc, en_muestra_global):
    # Las 9 primeras claves son exactamente las del notebook, con los valores
    # crudos de la librería (sin limpiar "N/A" ni ""), para no alterar los datos.
    return {
        "fecha_hora_captura": fecha_hora_captura,
        "id": v.id,
        "aerolinea": v.airline_icao,
        "vuelo": v.callsign,
        "latitud": v.latitude,
        "longitud": v.longitude,
        "altitud": v.altitude,
        "velocidad_nudos": v.ground_speed,
        "rumbo": v.heading,
        # --- extras ---
        "captura_utc": captura_utc,
        "posicion_utc": _iso_utc(getattr(v, "time", None)),
        "icao24": getattr(v, "icao_24bit", None),
        "numero": getattr(v, "number", None),
        "matricula": getattr(v, "registration", None),
        "tipo_aeronave": getattr(v, "aircraft_code", None),
        "origen": getattr(v, "origin_airport_iata", None),
        "destino": getattr(v, "destination_airport_iata", None),
        "velocidad_vertical": getattr(v, "vertical_speed", None),
        "squawk": getattr(v, "squawk", None),
        "en_muestra_global": en_muestra_global,
    }


def construir_filas(globales, por_tiles, captura):
    """Primero las filas de la request global (mismo orden que el notebook),
    luego las que solo aparecieron en los cuadrantes."""
    fecha_local = captura.astimezone(TZ_CAPTURA).strftime("%Y-%m-%d %H:%M:%S")
    captura_utc = captura.isoformat()
    filas, ids_globales = [], set()
    for v in globales:
        ids_globales.add(v.id)
        if not v.on_ground:  # mismo filtro que el notebook
            filas.append(fila(v, fecha_local, captura_utc, True))
    for vid, v in por_tiles.items():
        if vid not in ids_globales and not v.on_ground:
            filas.append(fila(v, fecha_local, captura_utc, False))
    return filas


def main():
    signal.signal(signal.SIGALRM, _alarma)
    signal.alarm(TIMEOUT_S)
    t0 = time.monotonic()
    captura = datetime.now(timezone.utc).replace(microsecond=0)
    stats = {
        "modo": MODO, "captura_utc": captura.isoformat(), "requests": 0, "errores": 0,
        "divisiones": 0, "tiles_saturados": 0, "tiles_sin_pedir": 0, "global_ok": False,
    }
    try:
        api = FlightRadar24API(timeout=HTTP_TIMEOUT_S)
        if MODO not in ("global", "tiles"):
            sys.exit(f"MODO desconocido: {MODO}")

        # 1) La request del notebook, siempre primero.
        try:
            globales = recolectar_global(api, stats)
            stats["global_ok"] = True
        except Exception as e:
            globales = []
            stats["errores"] += 1
            stats["ultimo_error"] = repr(e)[:300]
        # 2) Cobertura adicional por cuadrantes.
        por_tiles = recolectar_tiles(api, stats) if MODO == "tiles" else {}

        filas = construir_filas(globales, por_tiles, captura)
        stats.update(vuelos_globales=len(globales), vuelos_tiles=len(por_tiles), filas=len(filas),
                     filas_muestra_global=sum(1 for f in filas if f["en_muestra_global"]))
        if not filas:
            stats["estado"] = "sin_datos"
            print(json.dumps(stats))
            return 1

        # Mismo serializador que el notebook: DataFrame(lista de dicts).to_csv(index=False)
        csv = pd.DataFrame(filas, columns=COLUMNAS).to_csv(index=False)
        datos = gzip.compress(csv.encode("utf-8"), compresslevel=6)

        key = f"{PREFIX}/fecha={captura:%Y-%m-%d}/{captura:%H%M%S}.csv.gz"
        s3_client().put_object(Bucket=bucket(), Key=key, Body=datos)
        signal.alarm(0)

        stats.update(estado="ok", key=key, bytes=len(datos),
                     duracion_s=round(time.monotonic() - t0, 1))
        print(json.dumps(stats))
        return 0
    except Timeout:
        stats.update(estado="timeout", duracion_s=round(time.monotonic() - t0, 1))
        print(json.dumps(stats))
        return 1


if __name__ == "__main__":
    sys.exit(main())