# --- CUBO OLAP DE HORARIOS - Conexión, Carga y Consultas ---
# Versión OLAP alineada al proyecto (slice, dice, drill-down, roll-up y pivot)

import os
import datetime
import pandas as pd
from sqlalchemy import create_engine


# =========================
# CONNECT
# =========================

def get_engine():
    """Crea la conexión con la base de datos MySQL usando variables de entorno o defaults."""
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASS = os.getenv("DB_PASS", "changocome")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("DB_NAME", "horarios")
    url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    return create_engine(url)


# =========================
# LOAD
# =========================

def cargar_tablas(engine):
    """Carga las tablas desde MySQL como DataFrames."""
    dim_docente = pd.read_sql("SELECT * FROM dim_docente", engine)
    dim_materia = pd.read_sql("SELECT * FROM dim_materia", engine)
    dim_espacio = pd.read_sql("SELECT * FROM dim_espacio", engine)
    dim_tiempo  = pd.read_sql("SELECT * FROM dim_tiempo", engine)
    hechos_horarios = pd.read_sql("SELECT * FROM hechos_horarios", engine)
    return hechos_horarios, dim_docente, dim_materia, dim_espacio, dim_tiempo


# =========================
# UTILS (compat con zip)
# =========================

ORDEN_DIAS = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado"]

def _to_time_safe(x):
    """Convierte a datetime.time o None (similar a to_time_safe del zip)."""
    if pd.isna(x) or x in [None, "", "NaT", "None"]:
        return None
    if isinstance(x, datetime.time):
        return x
    if hasattr(x, "total_seconds"):  # timedelta -> time
        total = int(x.total_seconds())
        h, m = divmod(total, 3600)
        m, s = divmod(m, 60)
        return datetime.time(h, m, s)
    try:
        t = pd.to_datetime(str(x), errors="coerce")
        return None if pd.isna(t) else t.time()
    except Exception:
        return None

def _minutes(a: datetime.time, b: datetime.time):
    if not (isinstance(a, datetime.time) and isinstance(b, datetime.time)):
        return None
    A = datetime.timedelta(hours=a.hour, minutes=a.minute, seconds=a.second)
    B = datetime.timedelta(hours=b.hour, minutes=b.minute, seconds=b.second)
    return round((B - A).total_seconds() / 60.0, 2)


# =========================
# CUBO
# =========================

class Horario_cubo:
    """
    Construye el cubo OLAP uniendo hechos y dimensiones.
    Implementa operaciones OLAP con la misma semántica que el proyecto:
    - slice_* : filtro en 1 dimensión
    - dice_*  : filtro multidimensional
    - drilldown_* : mayor granularidad / orden jerárquico
    - rollup_* : agregación a nivel superior
    - pivot_* : rotación (filas/columnas) / múltiples medidas
    """

    def __init__(self, hechos, dim_docente, dim_materia, dim_espacio, dim_tiempo):
        self.hechos = hechos
        self.dim_docente = dim_docente
        self.dim_materia = dim_materia
        self.dim_espacio = dim_espacio
        self.dim_tiempo = dim_tiempo

        # Vista del cubo
        self.cubo = (
            hechos
            .merge(dim_docente, on="id_docente", how="left")
            .merge(dim_materia, on="id_materia", how="left")
            .merge(dim_espacio, on="id_espacio", how="left")
            .merge(dim_tiempo, on="id_tiempo", how="left")
        )

        # Normaliza "clave" si viene duplicada de merges
        if "clave_x" in self.cubo.columns and "clave_y" in self.cubo.columns:
            self.cubo["clave"] = self.cubo["clave_y"].combine_first(self.cubo["clave_x"])
            self.cubo.drop(columns=["clave_x", "clave_y"], inplace=True)
        elif "clave_x" in self.cubo.columns:
            self.cubo.rename(columns={"clave_x": "clave"}, inplace=True)
        elif "clave_y" in self.cubo.columns:
            self.cubo.rename(columns={"clave_y": "clave"}, inplace=True)

        # Horas seguras
        for col in ["h_inicio", "h_fin"]:
            if col in self.cubo.columns:
                self.cubo[col] = self.cubo[col].apply(_to_time_safe)

        # Duración en minutos si no existe
        if "duracion_min" not in self.cubo.columns and {"h_inicio", "h_fin"}.issubset(self.cubo.columns):
            self.cubo["duracion_min"] = self.cubo.apply(lambda r: _minutes(r["h_inicio"], r["h_fin"]), axis=1)

        # Orden día de semana
        if "dia_semana" in self.cubo.columns:
            self.cubo["dia_semana"] = pd.Categorical(self.cubo["dia_semana"], categories=ORDEN_DIAS, ordered=True)

        # Aliases frecuentes (compat con nombres del zip)
        # nombreMateria / nombre_materia pueden variar entre fuentes
        if "nombreMateria" not in self.cubo.columns and "nombre_materia" in self.cubo.columns:
            self.cubo["nombreMateria"] = self.cubo["nombre_materia"]
    

    # =========================================================
    # SLICE
    # =========================================================

    def slice_por_docente(self, texto_nombre: str) -> pd.DataFrame:
        """
        Slice ~ filtrar por una sola dimensión (docente).
        Objetivo 1: horario semanal de un docente.
        """
        if "nombreCompleto" not in self.cubo.columns:
            return pd.DataFrame()

        df = self.cubo[self.cubo["nombreCompleto"].str.contains(texto_nombre, case=False, na=False)].copy()
        if df.empty:
            return pd.DataFrame()

        # Drill-down implícito en el orden (día -> hora)
        df = self.drilldown_docente_dia_hora(df)

        cols = ["nombreCompleto", "dia_semana", "h_inicio", "h_fin",
                "nombreMateria", "clave", "codigo_salon", "edificio", "aula"]
        cols = [c for c in cols if c in df.columns]
        return df[cols].reset_index(drop=True)

    # =========================================================
    # DICE
    # =========================================================

    def dice_por_materia(self, clave_o_nombre: str) -> pd.DataFrame:
        """
        Dice ~ filtro multidimensional (clave OR nombre de materia).
        Objetivo 2: docentes que dan una materia específica.
        """
        cols = self.cubo.columns
        condiciones = []
        if "nombreMateria" in cols:
            condiciones.append(self.cubo["nombreMateria"].str.contains(clave_o_nombre, case=False, na=False))
        if "clave" in cols:
            condiciones.append(self.cubo["clave"].str.contains(clave_o_nombre, case=False, na=False))
        if not condiciones:
            return pd.DataFrame()

        filtro = condiciones[0]
        for cond in condiciones[1:]:
            filtro = filtro | cond

        df = self.cubo[filtro]
        if df.empty:
            return pd.DataFrame()

        out = (df[["clave", "nombreMateria", "nombreCompleto"]]
               .drop_duplicates()
               .sort_values(["clave", "nombreCompleto"]))
        return out.reset_index(drop=True)

    def dice_en_edificio_hora(self, edificio: str, hora_str: str) -> pd.DataFrame:
        """
        Dice ~ filtro por múltiples dimensiones (espacio + tiempo).
        Objetivo 3: docentes presentes en un mismo edificio en una hora dada.
        """
        hora_ref = _to_time_safe(hora_str)
        if hora_ref is None or not {"h_inicio", "h_fin"}.issubset(self.cubo.columns):
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

        cols = ["nombreCompleto", "nombreMateria", "clave", "codigo_salon",
                "dia_semana", "h_inicio", "h_fin"]
        cols = [c for c in cols if c in df.columns]
        df = df[cols].drop_duplicates().sort_values(["dia_semana", "h_inicio", "nombreCompleto"])
        return df.reset_index(drop=True)

    # =========================================================
    # DRILL-DOWN
    # =========================================================

    def drilldown_docente_dia_hora(self, df_in: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Drill-down ~ mayor granularidad / orden jerárquico Día -> Hora.
        Si no se pasa df, se aplica sobre todo el cubo.
        """
        df = self.cubo if df_in is None else df_in.copy()
        if "dia_semana" in df.columns:
            df["dia_semana"] = pd.Categorical(df["dia_semana"], categories=ORDEN_DIAS, ordered=True)
        order_cols = [c for c in ["dia_semana", "h_inicio", "h_fin"] if c in df.columns]
        if order_cols:
            df = df.sort_values(order_cols)
        return df

    # =========================================================
    # ROLL-UP
    # =========================================================

    def rollup_horas_por_docente(self) -> pd.DataFrame:
        """
        Roll-Up: resumen a nivel superior.
        Muestra las horas totales de clase por docente en toda la semana.
        """
        if not {"nombreCompleto", "duracion_min"}.issubset(self.cubo.columns):
            return pd.DataFrame()

        # Agrupa por docente y suma la duración total de clases
        resumen = (
            self.cubo.groupby("nombreCompleto", dropna=False)["duracion_min"]
            .sum()
            .reset_index()
            .rename(columns={"nombreCompleto": "Nombre", "duracion_min": "Minutos_totales"})
        )

        # Convierte a horas
        resumen["Horas_totales"] = (resumen["Minutos_totales"] / 60).round(2)

        # Ordenar por más horas
        resumen = resumen.sort_values("Horas_totales", ascending=False)
        return resumen

    # =========================================================
    # PIVOT
    # =========================================================

    def pivot_docente_por_dia(self) -> pd.DataFrame:
        """
        Pivot ~ rotación (filas/columnas): Conteo de clases por día y docente.
        (Equivalente a pivot del zip).
        """
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
        for d in ORDEN_DIAS:
            if d not in tabla.columns:
                tabla[d] = 0
        tabla = tabla[ORDEN_DIAS]
        tabla["Total"] = tabla.sum(axis=1)
        tabla.index.name = "Nombre"
        return tabla.reset_index()

    # =========================================================
    # DEMO (ejecuta todas las operaciones como en usocubos.py)
    # =========================================================

    def demo_operaciones(self, ejemplo_docente: str = "", ejemplo_materia: str = "", ejemplo_edificio: str = "", ejemplo_hora: str = "10:00") -> dict:
        """
        Ejecuta una pasada de: slice, dice, drill-down, roll-up y pivot.
        Devuelve dict con resultados (útil para notebooks o pruebas rápidas).
        """
        resultados = {}

        # SLICE: horario semanal de un docente
        if ejemplo_docente:
            resultados["slice_docente"] = self.slice_por_docente(ejemplo_docente)

        # DICE: docentes por materia
        if ejemplo_materia:
            resultados["dice_materia"] = self.dice_por_materia(ejemplo_materia)

        # DICE (multi-dim): docentes en edificio a una hora
        if ejemplo_edificio and ejemplo_hora:
            resultados["dice_edificio_hora"] = self.dice_en_edificio_hora(ejemplo_edificio, ejemplo_hora)

        # DRILL-DOWN: ordenar por día -> hora en todo el cubo
        resultados["drilldown_dia_hora"] = self.drilldown_docente_dia_hora()

        # ROLL-UP: agregación clases por docente y día
        resultados["rollup_docente_dia"] = self.rollup_clases_por_docente_dia()

        # PIVOT: conteo de clases por día y docente
        resultados["pivot_docente_dia"] = self.pivot_docente_por_dia()

        return resultados


# =========================
# BUILD
# =========================

def build_cubo_from_db():
    """Construye una instancia del cubo usando los datos cargados desde MySQL."""
    engine = get_engine()
    h, ddoc, dmat, desp, dtime = cargar_tablas(engine)
    return Horario_cubo(h, ddoc, dmat, desp, dtime)
