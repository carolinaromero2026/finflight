"""Une los CSV de cada día UTC ya cerrado en un solo compactado.csv.gz.

Trabaja a nivel de texto: no parsea ni re-serializa filas, así que los valores
quedan byte a byte como los escribió el collector (mismo resultado que el CSV
anexado del notebook, con un único encabezado). El orden es cronológico.

Idempotente: revisa TODOS los días anteriores a hoy (UTC) que tengan archivos
sin compactar. Escribe el compactado, verifica y recién entonces borra los
originales. Si se cae entre medio, la siguiente corrida detecta qué capturas ya
están en el compactado (por captura_utc) y no las duplica.
"""
import gzip
import io
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd

from common import PREFIX, bucket, env, s3_client

NOMBRE = "compactado.csv.gz"
BORRAR_ORIGINALES = env("BORRAR_ORIGINALES", "1") == "1"


def listar_por_dia(s3, b):
    por_dia = defaultdict(list)
    base = f"{PREFIX}/fecha="
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=base):
        for obj in pagina.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".csv.gz"):
                por_dia[k[len(base):].split("/", 1)[0]].append(k)
    return por_dia


def bajar_texto(s3, b, key):
    return gzip.decompress(s3.get_object(Bucket=b, Key=key)["Body"].read()).decode("utf-8")


def captura_de_key(dia, key):
    # vuelos-csv/fecha=2026-10-01/054005.csv.gz -> 2026-10-01T05:40:05+00:00
    hhmmss = key.rsplit("/", 1)[1].split(".", 1)[0]
    return f"{dia}T{hhmmss[0:2]}:{hhmmss[2:4]}:{hhmmss[4:6]}+00:00"


def compactar_dia(s3, b, dia, keys):
    destino = f"{PREFIX}/fecha={dia}/{NOMBRE}"
    corridas = sorted(k for k in keys if k != destino)  # HHMMSS => orden cronológico

    encabezado, partes, ya_incluidas = None, [], set()
    if destino in keys:
        texto = bajar_texto(s3, b, destino)
        encabezado, cuerpo = texto.split("\n", 1)
        partes.append(cuerpo)
        ya_incluidas = set(pd.read_csv(io.StringIO(texto), usecols=["captura_utc"],
                                       dtype=str)["captura_utc"].unique())

    saltadas = 0
    for k in corridas:
        if captura_de_key(dia, k) in ya_incluidas:
            saltadas += 1  # quedó de una compactación interrumpida
            continue
        texto = bajar_texto(s3, b, k)
        cab, cuerpo = texto.split("\n", 1)
        if encabezado is None:
            encabezado = cab
        elif cab != encabezado:
            raise RuntimeError(f"{dia}: encabezado distinto en {k}; no se mezclan formatos")
        partes.append(cuerpo)

    salida = encabezado + "\n" + "".join(partes)
    filas = salida.count("\n") - 1
    datos = gzip.compress(salida.encode("utf-8"), compresslevel=9)
    if gzip.decompress(datos).decode("utf-8").count("\n") - 1 != filas:
        raise RuntimeError(f"{dia}: verificación local falló")

    s3.put_object(Bucket=b, Key=destino, Body=datos)
    if s3.head_object(Bucket=b, Key=destino)["ContentLength"] != len(datos):
        raise RuntimeError(f"{dia}: tamaño subido no coincide; no se borra nada")

    borrados = 0
    if BORRAR_ORIGINALES:
        for i in range(0, len(corridas), 1000):
            lote = corridas[i:i + 1000]
            s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in lote], "Quiet": True})
            borrados += len(lote)

    return {"dia": dia, "corridas": len(corridas), "saltadas_por_duplicado": saltadas,
            "filas": filas, "bytes": len(datos), "borrados": borrados}


def main():
    s3, b = s3_client(), bucket()
    hoy = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    resultados, errores = [], []
    for dia, keys in sorted(listar_por_dia(s3, b).items()):
        if dia >= hoy:
            continue  # el día en curso sigue recibiendo archivos
        if keys == [f"{PREFIX}/fecha={dia}/{NOMBRE}"]:
            continue  # ya compactado
        try:
            resultados.append(compactar_dia(s3, b, dia, keys))
        except Exception as e:
            errores.append({"dia": dia, "error": repr(e)[:300]})
    print(json.dumps({"compactados": resultados, "errores": errores}))
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main())