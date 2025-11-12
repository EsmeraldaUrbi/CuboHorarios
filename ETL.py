
# ETL de Horarios Académicos - Extracción, Limpieza y Carga


import re
import pdfplumber
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine
from pathlib import Path
from mysql.connector import errorcode


# EXTRACT


PDFS = [
    "pdfs/PA_OTOÑO_2025_SEMESTRAL_ICC.pdf",
    "pdfs/PA_OTOÑO_2025_SEMESTRAL_ITI.pdf",
    "pdfs/PA_OTOÑO_2025_SEMESTRAL_LCC.pdf",
]

def clean_header(cols):
    # Limpia los encabezados del PDF quitando espacios y pasando a minúsculas
    # cols: lista de nombres de columna detectados
    return [re.sub(r"\s+", " ", c).strip().lower() for c in cols]


def extract_tables_pdfplumber(pdf_path):
    # Extrae tablas de un PDF usando pdfplumber
    # pdf_path: ruta del archivo PDF a procesar
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue
                header = clean_header(table[0])
                # Busca las tablas que contengan las columnas esperadas
                if {"nrc","clave","materia","días","hora","profesor","salón"}.issubset(set(header)) or \
                   {"nrc","clave","materia","dias","hora","profesor","salon"}.issubset(set(header)):
                    for r in table[1:]:
                        if r and any(x for x in r):
                            rows.append(dict(zip(header, r)))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def extract_all():
    # Procesa todos los PDFs y concatena los resultados
    frames = []
    for p in PDFS:
        if Path(p).exists():
            df = extract_tables_pdfplumber(p)
            if not df.empty:
                df["origen_pdf"] = Path(p).name
                frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# Extracción inicial de todos los PDF
raw = extract_all()


# TRANSFORM


raw = raw.rename(columns={"dias": "días", "salon": "salón"})

def normalizar_profesor(x: str):
    # Corrige el formato del nombre del profesor
    # x: cadena con el nombre original del PDF
    if not isinstance(x, str):
        return None
    x = re.sub(r"\s+", " ", x).strip().replace(" - ", " ")
    return x.title()

raw["profesor"] = raw["profesor"].apply(normalizar_profesor)
raw.columns = [c.strip().lower() for c in raw.columns]

# Detecta la columna de hora en caso de que venga con otro nombre
for variant in ["horario", "hora ", "hora\n", "h"]:
    if variant in raw.columns:
        raw.rename(columns={variant: "hora"}, inplace=True)
        break
if "hora" not in raw.columns:
    raw["hora"] = None


def parse_hora(rango):
    # Convierte un rango horario tipo "07:00-08:59" en inicio, fin y duración
    # rango: texto con el rango horario
    if not isinstance(rango, str):
        return pd.Series([None, None, None])
    s = re.sub(r"\s+", "", rango.strip())
    patron = r"(\d{1,2}):?(\d{2})-(\d{1,2}):?(\d{2})"
    m = re.match(patron, s)
    if not m:
        return pd.Series([None, None, None])
    h1, m1, h2, m2 = map(int, m.groups())
    start = pd.to_datetime(f"{h1:02d}:{m1:02d}", format="%H:%M", errors="coerce")
    end   = pd.to_datetime(f"{h2:02d}:{m2:02d}", format="%H:%M", errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return pd.Series([None, None, None])
    duracion = int((end - start).total_seconds() / 60)
    if duracion <= 0:
        return pd.Series([None, None, None])
    return pd.Series([start.time(), end.time(), duracion])

raw[["h_inicio", "h_fin", "duracion_min"]] = raw["hora"].apply(parse_hora)

# Mapea letras de días a nombres completos
DIA_MAP = {"L":"Lunes","A":"Martes","M":"Miercoles","J":"Jueves","V":"Viernes","S":"Sábado"}

def explotar_por_dia(df):
    # Divide filas con varios días (ej. "LMV") en una por día
    # df: DataFrame original con columna 'días'
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

curated = explotar_por_dia(raw)


def split_salon(s):
    # Separa el texto del salón en edificio y aula
    # s: texto con formato tipo "1CCO4/203"
    if not isinstance(s, str): 
        return pd.Series([None, None, None])
    s = s.strip()
    m = re.match(r"([^/]+)/?(\w+)?", s)
    if not m: 
        return pd.Series([s, None, s])
    edificio, aula = m.group(1), m.group(2)
    return pd.Series([edificio, aula, s])

curated[["edificio","aula","codigo_salon"]] = curated["salón"].apply(split_salon)


def build_dim(df, col_key, cols_keep, start_id=1, name_id="id"):
    # Crea una tabla de dimensión con IDs automáticos
    # df: datos base, col_key: columna clave, cols_keep: columnas que se conservan
    d = df[cols_keep].drop_duplicates().reset_index(drop=True)
    d.insert(0, name_id, range(start_id, start_id+len(d)))
    return d

dim_docente = build_dim(curated, "profesor", ["profesor"], name_id="id_docente")
dim_materia = build_dim(curated, "materia", ["clave","materia"], name_id="id_materia")
dim_espacio = build_dim(curated, "codigo_salon", ["edificio","aula","codigo_salon"], name_id="id_espacio")
dim_tiempo = curated[["dia_codigo","dia_semana","h_inicio","h_fin"]].drop_duplicates().reset_index(drop=True)
dim_tiempo.insert(0, "id_tiempo", range(1, len(dim_tiempo)+1))


def map_id(df, dim, key_cols_df, key_cols_dim, id_col):
    # Asigna los IDs de las dimensiones a la tabla de hechos
    # df: hechos, dim: tabla dimensión, key_cols_df y key_cols_dim: columnas de enlace
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


# Une IDs con dimensiones
hechos = curated.copy()
hechos["id_docente"] = map_id(hechos, dim_docente, "profesor", "profesor", "id_docente")
hechos["id_materia"] = map_id(hechos, dim_materia, ["clave","materia"], ["clave","materia"], "id_materia")
hechos["id_espacio"] = map_id(hechos, dim_espacio, "codigo_salon", "codigo_salon", "id_espacio")
hechos = hechos.merge(dim_tiempo, on=["dia_codigo","dia_semana","h_inicio","h_fin"], how="left")

hechos_horarios = hechos[[
    "id_docente","id_materia","id_espacio","id_tiempo",
    "nrc","clave","días","duracion_min"
]].rename(columns={"días":"seccion"})


# LOAD


DB_CONFIG = {
    "user": "root",
    "password": "changocome",
    "host": "localhost",
    "database": "horarios",
    "allow_local_infile": True
}

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Conexión a MySQL establecida correctamente.")
except mysql.connector.Error as err:
    print(f"Error de conexión: {err}")
    raise SystemExit()

# Crea las tablas si no existen
ddl_statements = [
    """
    CREATE TABLE IF NOT EXISTS dim_docente (
        id_docente INT PRIMARY KEY,
        nombre_completo VARCHAR(200)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_materia (
        id_materia INT PRIMARY KEY,
        clave VARCHAR(50),
        nombre_materia VARCHAR(200)
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
        hora_inicio TIME,
        hora_fin TIME
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
print("Tablas creadas o verificadas correctamente.")


def insert_dataframe(df: pd.DataFrame, table_name: str):
    # Inserta un DataFrame completo en la tabla indicada
    # df: datos a insertar, table_name: nombre de la tabla
    if df.empty:
        print(f"({table_name} está vacío, no se inserta nada)")
        return
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = [
        tuple(None if pd.isna(x) or str(x).lower() in ["nan","none","nat",""] else x for x in row)
        for row in df.itertuples(index=False, name=None)
    ]
    cursor.executemany(sql, data)
    conn.commit()
    print(f"{len(df)} filas insertadas en {table_name}")


# Inserción de datos
insert_dataframe(dim_docente.rename(columns={"profesor":"nombreCompleto"}), "dim_docente")
insert_dataframe(dim_materia.rename(columns={"materia":"nombreMateria"}), "dim_materia")
insert_dataframe(dim_espacio, "dim_espacio")
insert_dataframe(dim_tiempo, "dim_tiempo")
insert_dataframe(hechos_horarios, "hechos_horarios")

cursor.close()
conn.close()
print("Conexión a MySQL cerrada exitosamente.")
