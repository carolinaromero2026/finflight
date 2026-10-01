"""Compacta los Parquet de cada día UTC ya cerrado en un solo archivo.

Idempotente: revisa TODOS los días anteriores a hoy (UTC) que tengan más de un
archivo, así que si una corrida falla, la siguiente se pone al día.
Orden de operaciones seguro: escribe el compactado, verifica, y recién entonces
borra los originales. Si se cae entre medio, quedan duplicados temporales que la
siguiente compactación elimina (dedup por id + captura_utc).
"""
import io
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from common import PREFIX, SCHEMA, bucket, env, s3_client

NOMBRE = "compactado.parquet"
BORRAR_ORIGINALES = env("BORRAR_ORIGINALES", "1") == "1"


def listar_por_dia(s3, b):
    por_dia = defaultdict(list)
    base = f"{PREFIX}/fecha="
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=base):
        for obj in pagina.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".parquet"):
                por_dia[k[len(base):].split("/", 1)[0]].append(k)
    return por_dia


def leer(s3, b, key):
    datos = s3.get_object(Bucket=b, Key=key)["Body"].read()
    t = pq.read_table(io.BytesIO(datos))
    return t.select(SCHEMA.names).cast(SCHEMA).replace_schema_metadata(None)


def ordenar_y_deduplicar(t):
    # Ordenar por trayectoria mejora mucho la compresión y las lecturas por vuelo.
    t = t.sort_by([("id", "ascending"), ("captura_utc", "ascending")])
    if t.num_rows < 2:
        return t
    ids = t.column("id").to_numpy(zero_copy_only=False)
    ts = t.column("captura_utc").cast(pa.int64()).to_numpy(zero_copy_only=False)
    dup = np.zeros(t.num_rows, dtype=bool)
    dup[1:] = (ids[1:] == ids[:-1]) & (ts[1:] == ts[:-1])
    return t.filter(pa.array(~dup))


def compactar_dia(s3, b, dia, keys):
    tablas = [leer(s3, b, k) for k in keys]
    filas_entrada = sum(t.num_rows for t in tablas)
    t = ordenar_y_deduplicar(pa.concat_tables(tablas))

    buf = io.BytesIO()
    pq.write_table(t, buf, compression="zstd", compression_level=9, row_group_size=250_000)
    datos = buf.getvalue()
    if pq.read_table(io.BytesIO(datos)).num_rows != t.num_rows:
        raise RuntimeError(f"{dia}: verificación local falló")

    destino = f"{PREFIX}/fecha={dia}/{NOMBRE}"
    s3.put_object(Bucket=b, Key=destino, Body=datos)
    if s3.head_object(Bucket=b, Key=destino)["ContentLength"] != len(datos):
        raise RuntimeError(f"{dia}: tamaño subido no coincide; no se borra nada")

    borrados = 0
    if BORRAR_ORIGINALES:
        viejos = [k for k in keys if k != destino]
        for i in range(0, len(viejos), 1000):
            lote = viejos[i:i + 1000]
            s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in lote], "Quiet": True})
            borrados += len(lote)

    return {"dia": dia, "archivos": len(keys), "filas_entrada": filas_entrada,
            "filas_salida": t.num_rows, "bytes": len(datos), "borrados": borrados}


def main():
    s3, b = s3_client(), bucket()
    hoy = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    resultados, errores = [], []
    for dia, keys in sorted(listar_por_dia(s3, b).items()):
        if dia >= hoy:
            continue  # el día en curso sigue recibiendo archivos
        if len(keys) == 1 and keys[0].endswith("/" + NOMBRE):
            continue  # ya compactado
        try:
            resultados.append(compactar_dia(s3, b, dia, keys))
        except Exception as e:
            errores.append({"dia": dia, "error": repr(e)[:300]})
    print(json.dumps({"compactados": resultados, "errores": errores}))
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main())
