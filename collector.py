"""Una corrida: descarga posiciones de FR24 y sube un Parquet al bucket.

Diseñado para Railway cron: hace su trabajo y TERMINA. Si el proceso no sale,
Railway se salta las ejecuciones siguientes, por eso hay un timeout duro.

Modos (variable MODO):
  global -> una sola request, igual que el notebook (FR24 la corta en ~1500 vuelos).
  tiles  -> divide el mundo en cuadrantes y subdivide los que vuelven saturados,
            para acercarse a cobertura completa. Más requests por corrida.
"""
import io
import json
import signal
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from FlightRadarAPI import FlightRadar24API

from common import PREFIX, SCHEMA, bucket, env, s3_client

MODO = env("MODO", "tiles")
TILE_LIMIT = int(env("TILE_LIMIT", "1500"))          # tope observado por request
SPLIT_FRAC = float(env("SPLIT_FRAC", "0.95"))        # subdividir si llega a 95% del tope
MAX_DEPTH = int(env("MAX_DEPTH", "5"))               # 90° / 2^5 ≈ 2.8° el tile más chico
MAX_REQUESTS = int(env("MAX_REQUESTS", "200"))
PAUSA_S = float(env("PAUSA_S", "0.3"))               # pausa entre requests
REINTENTOS = int(env("REINTENTOS", "3"))
TIMEOUT_S = int(env("RUN_TIMEOUT_S", "240"))         # < 300 s del intervalo del cron
HTTP_TIMEOUT_S = int(env("HTTP_TIMEOUT_S", "20"))
INCLUIR_EN_TIERRA = env("INCLUIR_EN_TIERRA", "0") == "1"


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
    stats["requests"] += 1
    res = pedir(api)
    if len(res) >= int(TILE_LIMIT * SPLIT_FRAC):
        stats["tiles_saturados"] += 1
    return res


def recolectar_tiles(api, stats):
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
    return list(vuelos.values())


def _str(x):
    return None if x in (None, "", "N/A") else str(x)


def _num(x, cast):
    try:
        return None if x in (None, "", "N/A") else cast(x)
    except (TypeError, ValueError):
        return None


def a_tabla(vuelos, captura):
    cols = {k: [] for k in SCHEMA.names}
    for v in vuelos:
        en_tierra = bool(_num(getattr(v, "on_ground", 0), int))
        if en_tierra and not INCLUIR_EN_TIERRA:
            continue
        t = _num(getattr(v, "time", None), int)
        cols["captura_utc"].append(captura)
        cols["posicion_utc"].append(datetime.fromtimestamp(t, timezone.utc) if t else None)
        cols["id"].append(_str(v.id))
        cols["icao24"].append(_str(getattr(v, "icao_24bit", None)))
        cols["aerolinea"].append(_str(getattr(v, "airline_icao", None)))
        cols["vuelo"].append(_str(getattr(v, "callsign", None)))
        cols["numero"].append(_str(getattr(v, "number", None)))
        cols["matricula"].append(_str(getattr(v, "registration", None)))
        cols["tipo_aeronave"].append(_str(getattr(v, "aircraft_code", None)))
        cols["origen"].append(_str(getattr(v, "origin_airport_iata", None)))
        cols["destino"].append(_str(getattr(v, "destination_airport_iata", None)))
        cols["latitud"].append(_num(getattr(v, "latitude", None), float))
        cols["longitud"].append(_num(getattr(v, "longitude", None), float))
        cols["altitud"].append(_num(getattr(v, "altitude", None), int))
        cols["velocidad_nudos"].append(_num(getattr(v, "ground_speed", None), int))
        cols["rumbo"].append(_num(getattr(v, "heading", None), int))
        cols["velocidad_vertical"].append(_num(getattr(v, "vertical_speed", None), int))
        cols["en_tierra"].append(en_tierra)
        cols["squawk"].append(_str(getattr(v, "squawk", None)))
    return pa.Table.from_pydict(cols, schema=SCHEMA)


def main():
    signal.signal(signal.SIGALRM, _alarma)
    signal.alarm(TIMEOUT_S)
    t0 = time.monotonic()
    captura = datetime.now(timezone.utc).replace(microsecond=0)
    stats = {
        "modo": MODO, "captura_utc": captura.isoformat(), "requests": 0, "errores": 0,
        "divisiones": 0, "tiles_saturados": 0, "tiles_sin_pedir": 0,
    }
    try:
        api = FlightRadar24API(timeout=HTTP_TIMEOUT_S)
        if MODO == "global":
            vuelos = recolectar_global(api, stats)
        elif MODO == "tiles":
            vuelos = recolectar_tiles(api, stats)
        else:
            sys.exit(f"MODO desconocido: {MODO}")

        stats["vuelos_recibidos"] = len(vuelos)
        tabla = a_tabla(vuelos, captura)
        stats["filas"] = tabla.num_rows
        if tabla.num_rows == 0:
            stats["estado"] = "sin_datos"
            print(json.dumps(stats))
            return 1

        # Los stats de la corrida quedan dentro del Parquet: permiten auditar
        # cobertura (tiles saturados, errores) de cada snapshot después.
        tabla = tabla.replace_schema_metadata({"corrida": json.dumps(stats)})
        buf = io.BytesIO()
        pq.write_table(tabla, buf, compression="zstd", compression_level=6)
        datos = buf.getvalue()

        key = f"{PREFIX}/fecha={captura:%Y-%m-%d}/{captura:%H%M%S}.parquet"
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
