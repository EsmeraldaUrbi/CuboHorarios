# ETL de Horarios Académicos - Extracción, Limpieza y Carga

import os
import re
import pdfplumber
import pandas as pd
import numpy as np
import mysql.connector
from pathlib import Path
from mysql.connector import errorcode


# CONFIGURACIÓN GENERAL
PDFS = [
    "pdfs/PA_OTOÑO_2025_SEMESTRAL_ICC.pdf",
    "pdfs/PA_OTOÑO_2025_SEMESTRAL_ITI.pdf",
    "pdfs/PA_OTOÑO_2025_SEMESTRAL_LCC.pdf",
]

DB_CONFIG = {
    "user": "root",
    "password": "changocome",
    "host": "localhost",
    "database": "horarios",
    "allow_local_infile": True
}


# EXTRACCIÓN

def clean_header(cols):
    # Limpia los encabezados de las tablas detectadas en los PDF.
    #
    # Parámetros:
    #   cols (list): Lista con los nombres de las columnas detectadas en la tabla.
    #
    # Retorna:
    #   list: Lista de encabezados normalizados (minúsculas y sin espacios extra).
    return [re.sub(r"\s+", " ", c).strip().lower() for c in cols]


def extract_tables_pdfplumber(pdf_path):
    # Extrae las tablas válidas de un archivo PDF de horarios.
    #
    # Parámetros:
    #   pdf_path (str): Ruta del archivo PDF a procesar.
    #
    # Retorna:
    #   pd.DataFrame: DataFrame con las filas extraídas del PDF.
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                if not table or len(table) < 2:
                    continue
                header = clean_header(table[0])

                # Verifica si contiene las columnas esperadas
                expected_cols = {"nrc", "clave", "materia", "días", "hora", "profesor", "salón"}
                expected_alt = {"nrc", "clave", "materia", "dias", "hora", "profesor", "salon"}

                if expected_cols.issubset(set(header)) or expected_alt.issubset(set(header)):
                    for row in table[1:]:
                        if row and any(row):
                            rows.append(dict(zip(header, row)))

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def extract_all():
    # Procesa todos los archivos PDF definidos y combina los datos extraídos.
    #
    # Parámetros:
    #   Ninguno (usa la lista global PDFS).
    #
    # Retorna:
    #   pd.DataFrame: DataFrame con todos los datos extraídos y combinados.
    frames = []
    for p in PDFS:
        if not Path(p).exists():
            print(f"[ADVERTENCIA] No se encontró el archivo: {p}")
            continue

        df = extract_tables_pdfplumber(p)
        if not df.empty:
            df["origen_pdf"] = Path(p).name
            frames.append(df)

    if not frames:
        print("[ERROR] No se extrajeron datos de ningún PDF.")
        return pd.DataFrame()

    full = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Extracción completada: {len(full)} filas totales.")
    return full


# TRANSFORMACIÓN

def normalizar_profesor(x: str):
    # Estandariza el formato de los nombres de profesor.
    #
    # Parámetros:
    #   x (str): Nombre del profesor en bruto, tal como viene en el PDF.
    #
    # Retorna:
    #   str: Nombre limpio, con capitalización correcta.
    if not isinstance(x, str):
        return None
    x = re.sub(r"\s+", " ", x).strip().replace(" - ", " ")
    return x.title()


def parse_hora(rango):
    # Convierte un rango de horas en formato legible y calcula su duración.
    #
    # Parámetros:
    #   rango (str): Texto con el formato '07:00-08:59' o similar.
    #
    # Retorna:
    #   pd.Series: Tres valores: hora_inicio, hora_fin, duracion_minutos.
    if not isinstance(rango, str):
        return pd.Series([None, None, None])

    s = re.sub(r"\s+", "", rango.strip())
    m = re.match(r"(\d{1,2}):?(\d{2})[-–](\d{1,2}):?(\d{2})", s)
    if not m:
        return pd.Series([None, None, None])

    h1, m1, h2, m2 = map(int, m.groups())
    start = pd.to_datetime(f"{h1:02d}:{m1:02d}", format="%H:%M", errors="coerce")
    end = pd.to_datetime(f"{h2:02d}:{m2:02d}", format="%H:%M", errors="coerce")

    if pd.isna(start) or pd.isna(end):
        return pd.Series([None, None, None])

    duracion = int((end - start).total_seconds() / 60)
    if duracion <= 0:
        return pd.Series([None, None, None])

    return pd.Series([start.time(), end.time(), duracion])


def explotar_por_dia(df):
    # Divide registros que contienen varios días (por ejemplo 'LMV') en uno por día.
    #
    # Parámetros:
    #   df (pd.DataFrame): DataFrame original con la columna 'días'.
    #
    # Retorna:
    #   pd.DataFrame: Nuevo DataFrame donde cada fila corresponde a un solo día.
    DIA_MAP = {"L": "Lunes", "A": "Martes", "M": "Miercoles", "J": "Jueves", "V": "Viernes", "S": "Sábado"}
    out = []

    for _, row in df.iterrows():
        dias = str(row["días"]).replace(" ", "")
        tokens = dias.split(",") if "," in dias else list(dias)
        for d in tokens:
            r = row.copy()
            r["dia_codigo"] = d
            r["dia_semana"] = DIA_MAP.get(d, d)
            out.append(r)

    return pd.DataFrame(out)


def split_salon(s):
    # Separa el texto del salón en edificio y aula.
    #
    # Parámetros:
    #   s (str): Texto con el formato '1CCO4/203' o similar.
    #
    # Retorna:
    #   pd.Series: Tres valores: edificio, aula y código completo.
    if not isinstance(s, str):
        return pd.Series([None, None, None])
    s = s.strip()
    m = re.match(r"([^/]+)/?(\w+)?", s)
    if not m:
        return pd.Series([s, None, s])
    edificio, aula = m.group(1), m.group(2)
    return pd.Series([edificio, aula, s])


def build_dim(df, col_key, cols_keep, start_id=1, name_id="id"):
    # Crea una tabla de dimensión única con identificadores automáticos.
    #
    # Parámetros:
    #   df (pd.DataFrame): DataFrame base.
    #   col_key (str): Columna clave para identificar valores únicos.
    #   cols_keep (list): Columnas que se conservarán.
    #   start_id (int): Valor inicial del ID (por defecto 1).
    #   name_id (str): Nombre de la columna ID.
    #
    # Retorna:
    #   pd.DataFrame: DataFrame con los valores únicos y su ID correspondiente.
    d = df[cols_keep].drop_duplicates().reset_index(drop=True)
    d.insert(0, name_id, range(start_id, start_id + len(d)))
    return d


def map_id(df, dim, key_cols_df, key_cols_dim, id_col):
    # Asigna identificadores de dimensión a la tabla de hechos.
    #
    # Parámetros:
    #   df (pd.DataFrame): DataFrame de hechos.
    #   dim (pd.DataFrame): DataFrame de la dimensión.
    #   key_cols_df (str o list): Columnas clave en el DataFrame de hechos.
    #   key_cols_dim (str o list): Columnas clave en la dimensión.
    #   id_col (str): Nombre de la columna de ID en la dimensión.
    #
    # Retorna:
    #   np.ndarray: Lista de IDs correspondientes.
    if isinstance(key_cols_df, str):
        key_cols_df = [key_cols_df]
    if isinstance(key_cols_dim, str):
        key_cols_dim = [key_cols_dim]

    df["_key_"] = df[key_cols_df].astype(str).agg("|".join, axis=1)
    dim["_key_"] = dim[key_cols_dim].astype(str).agg("|".join, axis=1)
    merged = df.merge(dim[["_key_", id_col]], on="_key_", how="left", validate="m:1")
    result = merged[id_col].values
    df.drop(columns="_key_", inplace=True, errors="ignore")
    dim.drop(columns="_key_", inplace=True, errors="ignore")
    return result


def transform_all(raw):
    # Ejecuta la limpieza y normalización completa de los datos extraídos.
    #
    # Parámetros:
    #   raw (pd.DataFrame): Datos crudos extraídos de los PDF.
    #
    # Retorna:
    #   tuple: Dimensiones y tabla de hechos listas para cargar en la base de datos.
    raw = raw.rename(columns={"dias": "días", "salon": "salón"})
    raw["profesor"] = raw["profesor"].apply(normalizar_profesor)
    raw.columns = [c.strip().lower() for c in raw.columns]

    # Detección de variaciones de la columna 'hora'
    for variant in ["horario", "hora ", "hora\n", "h"]:
        if variant in raw.columns:
            raw.rename(columns={variant: "hora"}, inplace=True)
            break
    if "hora" not in raw.columns:
        raw["hora"] = None

    raw[["h_inicio", "h_fin", "duracion_min"]] = raw["hora"].apply(parse_hora)

    curated = explotar_por_dia(raw)
    curated[["edificio", "aula", "codigo_salon"]] = curated["salón"].apply(split_salon)

    dim_docente = build_dim(curated, "profesor", ["profesor"], name_id="id_docente")
    dim_materia = build_dim(curated, "materia", ["clave", "materia"], name_id="id_materia")
    dim_espacio = build_dim(curated, "codigo_salon", ["edificio", "aula", "codigo_salon"], name_id="id_espacio")
    dim_tiempo = curated[["dia_codigo", "dia_semana", "h_inicio", "h_fin"]].drop_duplicates().reset_index(drop=True)
    dim_tiempo.insert(0, "id_tiempo", range(1, len(dim_tiempo) + 1))

    hechos = curated.copy()
    hechos["id_docente"] = map_id(hechos, dim_docente, "profesor", "profesor", "id_docente")
    hechos["id_materia"] = map_id(hechos, dim_materia, ["clave", "materia"], ["clave", "materia"], "id_materia")
    hechos["id_espacio"] = map_id(hechos, dim_espacio, "codigo_salon", "codigo_salon", "id_espacio")
    hechos = hechos.merge(dim_tiempo, on=["dia_codigo", "dia_semana", "h_inicio", "h_fin"], how="left")

    hechos_horarios = hechos[[
        "id_docente", "id_materia", "id_espacio", "id_tiempo",
        "nrc", "clave", "días", "duracion_min"
    ]].rename(columns={"días": "seccion"})

    return dim_docente, dim_materia, dim_espacio, dim_tiempo, hechos_horarios


# CARGA

def conectar_mysql():
    # Establece la conexión con la base de datos MySQL.
    #
    # Parámetros:
    #   Ninguno (usa la configuración global DB_CONFIG).
    #
    # Retorna:
    #   tuple: Conexión y cursor de MySQL.
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("[OK] Conexión a MySQL establecida correctamente.")
        return conn, cursor
    except mysql.connector.Error as err:
        print(f"[ERROR] No se pudo conectar a MySQL: {err}")
        raise SystemExit()


def crear_tablas(cursor, conn):
    # Crea las tablas necesarias si aún no existen.
    #
    # Parámetros:
    #   cursor: Cursor activo de MySQL.
    #   conn: Conexión activa de MySQL.
    #
    # Retorna:
    #   None
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_docente (
            id_docente INT PRIMARY KEY,
            nombreCompleto VARCHAR(200)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_materia (
            id_materia INT PRIMARY KEY,
            clave VARCHAR(50),
            nombreMateria VARCHAR(200)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_espacio (
            id_espacio INT PRIMARY KEY,
            edificio VARCHAR(50),
            aula VARCHAR(50),
            codigo_salon VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_tiempo (
            id_tiempo INT PRIMARY KEY,
            dia_codigo VARCHAR(10),
            dia_semana VARCHAR(20),
            h_inicio TIME,
            h_fin TIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS hechos_horarios (
            id_hecho INT AUTO_INCREMENT PRIMARY KEY,
            id_docente INT,
            id_materia INT,
            id_espacio INT,
            id_tiempo INT,
            nrc VARCHAR(20),
            clave VARCHAR(50),
            seccion VARCHAR(50),
            duracion_min INT,
            FOREIGN KEY (id_docente) REFERENCES dim_docente(id_docente),
            FOREIGN KEY (id_materia) REFERENCES dim_materia(id_materia),
            FOREIGN KEY (id_espacio) REFERENCES dim_espacio(id_espacio),
            FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo)
        )
        """
    ]

    for ddl in ddl_statements:
        cursor.execute(ddl)
    conn.commit()
    print("[OK] Tablas creadas o verificadas correctamente.")


def insert_dataframe(df: pd.DataFrame, table_name: str, cursor, conn):
    # Inserta el contenido de un DataFrame en una tabla MySQL.
    #
    # Parámetros:
    #   df (pd.DataFrame): Datos a insertar.
    #   table_name (str): Nombre de la tabla destino.
    #   cursor: Cursor activo de MySQL.
    #   conn: Conexión activa de MySQL.
    #
    # Retorna:
    #   None
    if df.empty:
        print(f"[INFO] {table_name} está vacío, no se inserta nada.")
        return

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    data = [
        tuple(None if pd.isna(x) or str(x).lower() in ["nan", "none", "nat", ""] else x for x in row)
        for row in df.itertuples(index=False, name=None)
    ]

    cursor.executemany(sql, data)
    conn.commit()
    print(f"[OK] {len(df)} filas insertadas en {table_name}.")


# MAIN

def main():
    # Ejecuta el flujo completo del proceso ETL.
    #
    # Flujo general:
    #   1. Extrae los datos desde los archivos PDF.
    #   2. Transforma y limpia la información.
    #   3. Carga los resultados en la base de datos MySQL.
    raw = extract_all()
    if raw.empty:
        print("[ERROR] No se pudo extraer ningún dato. Abortando ETL.")
        return

    dim_docente, dim_materia, dim_espacio, dim_tiempo, hechos_horarios = transform_all(raw)

    conn, cursor = conectar_mysql()
    crear_tablas(cursor, conn)

    insert_dataframe(dim_docente.rename(columns={"profesor": "nombreCompleto"}), "dim_docente", cursor, conn)
    insert_dataframe(dim_materia.rename(columns={"materia": "nombreMateria"}), "dim_materia", cursor, conn)
    insert_dataframe(dim_espacio, "dim_espacio", cursor, conn)
    insert_dataframe(dim_tiempo, "dim_tiempo", cursor, conn)
    insert_dataframe(hechos_horarios, "hechos_horarios", cursor, conn)

    cursor.close()
    conn.close()
    print("[OK] Conexión a MySQL cerrada exitosamente.")


if __name__ == "__main__":
    main()
