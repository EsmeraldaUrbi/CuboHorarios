
# --- CUBO OLAP DE HORARIOS - Conexión, Carga y Consultas ---


import os
import datetime
import pandas as pd
from sqlalchemy import create_engine


# CONNECT

def get_engine():
    # Crea la conexión con la base de datos MySQL
    # No recibe argumentos; usa variables de entorno o valores por defecto
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASS = os.getenv("DB_PASS", "changocome")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("DB_NAME", "horarios")
    url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    return create_engine(url)



# LOAD

def cargar_tablas(engine):
    # Carga las tablas desde MySQL como DataFrames
    # engine: conexión SQLAlchemy
    dim_docente = pd.read_sql("SELECT * FROM dim_docente", engine)
    dim_materia = pd.read_sql("SELECT * FROM dim_materia", engine)
    dim_espacio = pd.read_sql("SELECT * FROM dim_espacio", engine)
    dim_tiempo  = pd.read_sql("SELECT * FROM dim_tiempo", engine)
    hechos_horarios = pd.read_sql("SELECT * FROM hechos_horarios", engine)
    return hechos_horarios, dim_docente, dim_materia, dim_espacio, dim_tiempo



# CUBO

class Horario_cubo:
    # Clase que construye el cubo OLAP uniendo hechos y dimensiones
    # Recibe 5 DataFrames: hechos, dim_docente, dim_materia, dim_espacio, dim_tiempo

    def __init__(self, hechos, dim_docente, dim_materia, dim_espacio, dim_tiempo):
        self.hechos = hechos
        self.dim_docente = dim_docente
        self.dim_materia = dim_materia
        self.dim_espacio = dim_espacio
        self.dim_tiempo = dim_tiempo

        # Une todas las tablas en una sola vista del cubo
        self.cubo = (
            hechos
            .merge(dim_docente, on="id_docente", how="left")
            .merge(dim_materia, on="id_materia", how="left")
            .merge(dim_espacio, on="id_espacio", how="left")
            .merge(dim_tiempo, on="id_tiempo", how="left")
        )

        # Limpia columnas duplicadas de "clave"
        if "clave_x" in self.cubo.columns and "clave_y" in self.cubo.columns:
            self.cubo["clave"] = self.cubo["clave_y"].combine_first(self.cubo["clave_x"])
            self.cubo.drop(columns=["clave_x", "clave_y"], inplace=True)
        elif "clave_x" in self.cubo.columns:
            self.cubo.rename(columns={"clave_x": "clave"}, inplace=True)
        elif "clave_y" in self.cubo.columns:
            self.cubo.rename(columns={"clave_y": "clave"}, inplace=True)

        # Convierte las horas a formato datetime.time
        for col in ["h_inicio", "h_fin"]:
            if col in self.cubo.columns:
                def to_time_safe(x):
                    if pd.isna(x) or x in [None, "", "NaT", "None"]:
                        return None
                    if isinstance(x, datetime.time):
                        return x
                    if hasattr(x, "total_seconds"):
                        total = int(x.total_seconds())
                        h, m = divmod(total, 3600)
                        m, s = divmod(m, 60)
                        return datetime.time(h, m, s)
                    try:
                        return pd.to_datetime(str(x), errors="coerce").time()
                    except Exception:
                        return None
                self.cubo[col] = self.cubo[col].apply(to_time_safe)

        # Calcula duración en minutos si no existe
        if "duracion_min" not in self.cubo.columns and {"h_inicio", "h_fin"}.issubset(self.cubo.columns):
            def minutes(a, b):
                if not (isinstance(a, datetime.time) and isinstance(b, datetime.time)):
                    return None
                A = datetime.timedelta(hours=a.hour, minutes=a.minute, seconds=a.second)
                B = datetime.timedelta(hours=b.hour, minutes=b.minute, seconds=b.second)
                return round((B - A).total_seconds() / 60.0, 2)
            self.cubo["duracion_min"] = self.cubo.apply(lambda r: minutes(r["h_inicio"], r["h_fin"]), axis=1)

    
    # SLICE - Horario por docente

    def h_docente(self, nombreDocente):
        # Devuelve todas las clases de un docente específico
        # nombreDocente: parte del nombre del profesor (str)
        def format_docente_display(x):
            if not isinstance(x, str) or not x.strip():
                return x
            partes = x.split()
            if len(partes) < 3:
                return x
            ap1, ap2, nombres = partes[0], partes[1], " ".join(partes[2:])
            apellidos = ap1 if ap1.lower() == ap2.lower() else f"{ap1} {ap2}"
            return f"{nombres} {apellidos}".strip()

        df = self.cubo[self.cubo["nombreCompleto"].str.contains(nombreDocente, case=False, na=False)]
        if df.empty:
            return pd.DataFrame()

        ordenDias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado"]
        df["dia_semana"] = pd.Categorical(df["dia_semana"], categories=ordenDias, ordered=True)
        df["nombreCompleto"] = df["nombreCompleto"].apply(format_docente_display)
        df = df.sort_values(["dia_semana", "h_inicio"])

        cols = ["nombreCompleto", "dia_semana", "h_inicio", "h_fin",
                "nombre_materia", "clave", "codigo_salon", "edificio", "aula"]

        return df[[c for c in cols if c in df.columns]].reset_index(drop=True)

    
    # DICE - Docentes por materia
    
    def docentes_por_materia(self, claveORmateria):
        # Filtra docentes que imparten cierta materia o clave
        # claveORmateria: texto con la clave o nombre de la materia
        cols = self.cubo.columns
        condiciones = []
        if "nombreMateria" in cols:
            condiciones.append(self.cubo["nombreMateria"].str.contains(claveORmateria, case=False, na=False))
        if "clave" in cols:
            condiciones.append(self.cubo["clave"].str.contains(claveORmateria, case=False, na=False))
        if not condiciones:
            return pd.DataFrame()

        filtro = condiciones[0]
        for cond in condiciones[1:]:
            filtro = filtro | cond

        df = self.cubo[filtro]
        if df.empty:
            return pd.DataFrame()

        return (df[["clave", "nombreMateria", "nombreCompleto"]]
                .drop_duplicates()
                .sort_values(["clave", "nombreCompleto"]))

    
    # DICE - Docentes en edificio por hora
    
    def docentes_en_edificio_hora(self, edificio, hora):
        # Muestra docentes presentes en un edificio a cierta hora
        # edificio: nombre o código del edificio / hora: string "HH:MM"
        def to_time_any(x):
            if isinstance(x, datetime.time):
                return x
            try:
                t = pd.to_datetime(str(x), errors="coerce")
                return None if pd.isna(t) else t.time()
            except Exception:
                return None

        hora_ref = to_time_any(hora)
        if hora_ref is None:
            return pd.DataFrame()

        df = self.cubo.dropna(subset=["h_inicio", "h_fin"]).copy()

        mask = (
            df["edificio"].astype(str).str.contains(edificio, case=False, na=False)
        ) & (
            df["h_inicio"].apply(lambda h: isinstance(h, datetime.time) and h <= hora_ref)
        ) & (
            df["h_fin"].apply(lambda h: isinstance(h, datetime.time) and h >= hora_ref)
        )

        df = df.loc[mask].copy()
        if df.empty:
            return pd.DataFrame()

        columnas = ["nombreCompleto", "nombreMateria", "clave", "codigo_salon",
                    "dia_semana", "h_inicio", "h_fin"]
        columnas = [c for c in columnas if c in df.columns]

        df = df[columnas].drop_duplicates().sort_values(["nombreCompleto", "dia_semana", "h_inicio"])
        return df.reset_index(drop=True)

    
    # PIVOT / ROLL-UP - Clases por día y docente
    
    def clases_por_docente_y_dia(self):
        # Genera una tabla dinámica con conteo de clases por docente y día
        required = {"nrc", "nombreCompleto", "dia_semana"}
        if not required.issubset(self.cubo.columns):
            return pd.DataFrame()

        tabla = pd.pivot_table(
            self.cubo,
            values="nrc",
            index="nombreCompleto",
            columns="dia_semana",
            aggfunc="count",
            fill_value=0
        )
        tabla.columns.name = None

        orden_dias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado"]
        for dia in orden_dias:
            if dia not in tabla.columns:
                tabla[dia] = 0

        tabla = tabla[orden_dias]

        tabla["Total"] = tabla.sum(axis=1)

        tabla.index.name = "Nombre"

        return tabla


# BUILD

def build_cubo_from_db():
    # Construye una instancia del cubo usando los datos cargados desde MySQL
    engine = get_engine()
    h, ddoc, dmat, desp, dtime = cargar_tablas(engine)
    return Horario_cubo(h, ddoc, dmat, desp, dtime)
