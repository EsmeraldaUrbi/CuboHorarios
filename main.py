# --- APLICACIÓN WEB - Visualización del Cubo OLAP de Horarios ---


import os
from flask import Flask, render_template, request, redirect, url_for, flash
from cubo import build_cubo_from_db
import pandas as pd


# SETUP

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

# Carga del cubo OLAP al iniciar la app
cubo = build_cubo_from_db()



# RUTAS PRINCIPALES


@app.route("/")
def index():
    # Página principal
    return render_template("index.html")



# DOCENTES

@app.route("/docentes", methods=["GET", "POST"])
def docentes():
    # Muestra el horario completo de los docentes o uno específico
    resultado = None
    nombre_docente = None

    if request.method == "POST":
        nombre_docente = request.form.get("nombre_docente", "").strip()
        if nombre_docente:
            resultado = cubo.h_docente(nombre_docente)
        else:
            resultado = cubo.cubo[[
                "nombreCompleto", "dia_semana", "h_inicio", "h_fin",
                "nombreMateria", "clave", "codigo_salon", "edificio", "aula"
            ]].sort_values(["nombreCompleto", "dia_semana", "h_inicio"])
    else:
        resultado = cubo.cubo[[
            "nombreCompleto", "dia_semana", "h_inicio", "h_fin",
            "nombreMateria", "clave", "codigo_salon", "edificio", "aula"
        ]].sort_values(["nombreCompleto", "dia_semana", "h_inicio"])

    # Orden correcto de días
    orden_dias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado"]
    resultado["dia_semana"] = pd.Categorical(resultado["dia_semana"], categories=orden_dias, ordered=True)
    resultado = resultado.sort_values(["nombreCompleto", "dia_semana", "h_inicio"])

    # Renderiza la plantilla
    return render_template(
        "docentes.html",
        resultado=resultado,
        nombre_docente=nombre_docente
    )



# MATERIAS

@app.route("/materias", methods=["GET", "POST"])
def materias():
    # Lista las materias y los docentes que las imparten
    df = None
    query = ""

    # Lista de materias únicas
    materias_lista = (
        cubo.cubo[["clave", "nombreMateria"]]
        .drop_duplicates()
        .sort_values("nombreMateria")
        .to_dict("records")
    )

    if request.method == "POST":
        query = request.form.get("materia", "").strip()
        df = cubo.docentes_por_materia(query)
        if df is None or df.empty:
            flash(f"No hay docentes para la materia/clave: {query}", "warning")
            return redirect(url_for("materias"))

    return render_template("materias.html", tabla=df, query=query, materias_lista=materias_lista)



# EDIFICIOS
@app.route("/edificios", methods=["GET", "POST"])
def edificios():
    # Muestra los docentes en un edificio a una hora específica
    df = None
    edificio = ""
    hora = ""

    # Listado de edificios disponibles
    edificios_lista = cubo.cubo["edificio"].dropna().drop_duplicates().sort_values().tolist()

    # Lista de horas (formato HH:MM)
    horas_lista = sorted({
        (h.strftime("%H:%M") if hasattr(h, "strftime") else str(h)[:5])
        for h in cubo.cubo["h_inicio"].dropna().tolist()
    })

    if request.method == "POST":
        edificio = request.form.get("edificio", "").strip()
        hora = request.form.get("hora", "").strip()

        if edificio and hora:
            df = cubo.docentes_en_edificio_hora(edificio, hora)
            if df is None or df.empty:
                flash(f"No hay docentes en {edificio} a las {hora}.", "warning")
                return redirect(url_for("edificios"))

    return render_template(
        "edificios.html",
        tabla=df,
        edificio=edificio,
        hora=hora,
        edificios_lista=edificios_lista,
        horas_lista=horas_lista
    )



# ESTADÍSTICAS
@app.route("/estadisticas")
def estadisticas():
    # Obtenemos la tabla dinámica
    piv = cubo.clases_por_docente_y_dia()

    # 🔹 Convertimos el índice a columnas
    piv = piv.reset_index()

    # 🔹 Si el índice tiene nombre, se lo cambiamos directamente
    if hasattr(piv.index, "name") and piv.index.name == "dia_semana":
        piv.index.name = "Día"

    # 🔹 Si hay una columna llamada 'dia_semana', también la renombramos
    if "dia_semana" in piv.columns:
        piv = piv.rename(columns={"dia_semana": "Día"})

    # 🔹 Renombramos 'nombreCompleto' por 'Nombre' si existe
    if "nombreCompleto" in piv.columns:
        piv = piv.rename(columns={"nombreCompleto": "Nombre"})

    # 🔹 Convertimos la tabla en HTML con bordes
    tabla_piv = piv.to_html(
        classes="table table-bordered table-striped table-full",
        index=False,
        border=1,
        justify="center"
    )

    # Renderizamos
    return render_template("estadisticas.html", tabla_piv=tabla_piv)



# MAIN

if __name__ == "__main__":
    # Ejecución local
    # Desde la carpeta "app/", correr con:  flask run  o  python main.py
    app.run(debug=True)
