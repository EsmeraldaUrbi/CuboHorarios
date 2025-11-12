# --- APLICACIÓN WEB - Visualización del Cubo OLAP de Horarios ---

import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash
from cubo_horarios_olap import build_cubo_from_db  # ✅ usa el nuevo cubo OLAP

# ----------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

# Cargar cubo OLAP al iniciar
cubo = build_cubo_from_db()


# ----------------------------
# RUTA PRINCIPAL
# ----------------------------
@app.route("/")
def index():
    return render_template("index.html")


# =====================================================
# SLICE - Horario por docente
# =====================================================
def _view_docentes():
    resultado = pd.DataFrame()
    nombre_docente = ""

    if request.method == "POST":
        nombre_docente = (request.form.get("nombre_docente") or "").strip()
        if nombre_docente:
            resultado = cubo.slice_por_docente(nombre_docente)
            if resultado is None or resultado.empty:
                flash("No se encontraron clases para ese docente.", "warning")
        else:
            flash("Por favor ingresa un nombre de docente.", "warning")

    return render_template("docentes.html", resultado=resultado, nombre_docente=nombre_docente)


@app.route("/docentes", methods=["GET", "POST"])
@app.route("/slice/docente", methods=["GET", "POST"], endpoint="vista_slice_docente")  # alias para los nuevos templates
def docentes():
    return _view_docentes()


# =====================================================
# DICE - Docentes por materia
# =====================================================
def _view_materias():
    query = ""
    df = pd.DataFrame()

    materias_lista = (
        cubo.dim_materia[["clave", "nombreMateria"]]
        .drop_duplicates()
        .sort_values("nombreMateria")
        .to_dict("records")
    )

    if request.method == "POST":
        query = (request.form.get("materia") or "").strip()
        if query:
            df = cubo.dice_por_materia(query)
            if df is None or df.empty:
                flash(f"No se encontraron docentes para la materia o clave '{query}'.", "warning")
        else:
            flash("Selecciona una materia o clave válida.", "warning")

    return render_template("materias.html", tabla=df, query=query, materias_lista=materias_lista)


@app.route("/materias", methods=["GET", "POST"])
@app.route("/dice/materia", methods=["GET", "POST"], endpoint="vista_dice_materia")  # alias OLAP
def materias():
    return _view_materias()


# =====================================================
# DICE - Docentes en edificio por hora
# =====================================================
def _view_edificios():
    edificio = ""
    hora = ""
    df = pd.DataFrame()

    edificios_lista = cubo.dim_espacio["edificio"].dropna().astype(str).drop_duplicates().sort_values().tolist()
    horas_lista = (
        cubo.cubo["h_inicio"].dropna().astype(str).drop_duplicates().sort_values().tolist()
        if "h_inicio" in cubo.cubo.columns else []
    )

    if request.method == "POST":
        edificio = (request.form.get("edificio") or "").strip()
        hora = (request.form.get("hora") or "").strip()
        if edificio and hora:
            df = cubo.dice_en_edificio_hora(edificio, hora)
            if df is None or df.empty:
                flash(f"No se encontraron docentes en {edificio} a las {hora}.", "warning")
        else:
            flash("Selecciona un edificio y una hora.", "warning")

    return render_template(
        "edificios.html",
        tabla=df,
        edificio=edificio,
        hora=hora,
        edificios_lista=edificios_lista,
        horas_lista=horas_lista,
    )


@app.route("/edificios", methods=["GET", "POST"])
@app.route("/dice/edificio-hora", methods=["GET", "POST"], endpoint="vista_dice_edificio_hora")
def edificios():
    return _view_edificios()


# =====================================================
# ROLL-UP / PIVOT - Estadísticas del cubo
# =====================================================
@app.route("/estadisticas")
@app.route("/rollup", endpoint="vista_rollup")
@app.route("/pivot", endpoint="vista_pivot")
def estadisticas():
    rollup_df = cubo.rollup_clases_por_docente_dia()
    pivot_df = cubo.pivot_docente_por_dia()

    tabla_rollup = (
        None
        if rollup_df is None or rollup_df.empty
        else rollup_df.to_html(
            classes="table table-bordered table-striped align-middle",
            index=False,
            border=0,
            justify="center",
        )
    )

    tabla_pivot = (
        None
        if pivot_df is None or pivot_df.empty
        else pivot_df.to_html(
            classes="table table-bordered table-striped align-middle",
            index=False,
            border=0,
            justify="center",
        )
    )

    return render_template("estadisticas.html", tabla_rollup=tabla_rollup, tabla_pivot=tabla_pivot)


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    app.run(debug=True)
