from flask import Flask, request, redirect, url_for, render_template, flash, session
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import folium
from geopy.distance import geodesic
import ipywidgets as widgets
from IPython.display import display
import re
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

app = Flask(__name__)
app.secret_key = "super_secret_key"  # Clave para sesiones y flash

# Simulamos un state global para almacenar datos a lo largo del flujo
data_store = {}

# ─────────────────────────────────────────────
# Middleware: Requiere autenticación para acceder a las rutas
# ─────────────────────────────────────────────
@app.before_request
def require_login():
    allowed_routes = ['login']
    if request.endpoint not in allowed_routes and not session.get('authenticated'):
        return redirect(url_for('login'))

# ─────────────────────────────────────────────
# RUTA: LOGIN
# ─────────────────────────────────────────────
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password")
        if password == "LUCIANO123":
            session['authenticated'] = True
            flash("Autenticación exitosa.")
            return redirect(url_for("choose_mode"))
        else:
            flash("Contraseña incorrecta. Inténtalo de nuevo.")
            return redirect(url_for("login"))
    return render_template("login.html")

# ─────────────────────────────────────────────
# RUTA: SELECCIÓN DE MODO DE CARGA
# ─────────────────────────────────────────────
@app.route("/choose_mode", methods=["GET"])
def choose_mode():
    return render_template("choose_mode.html")

# ─────────────────────────────────────────────
# RUTA 1: CARGA MANUAL DEL EXCEL (igual que antes)
# ─────────────────────────────────────────────
@app.route("/upload_file", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        if "file" not in request.files:
            flash("❌ No se encontró el archivo en la solicitud.")
            return redirect(request.url)
        file = request.files["file"]
        if file.filename == "":
            flash("❌ No se seleccionó ningún archivo.")
            return redirect(request.url)
        try:
            df = pd.read_excel(file)
            if df.empty:
                flash("❌ El archivo está vacío. Subí un archivo válido.")
                return redirect(request.url)
            else:
                # Verificar columnas requeridas
                required_columns = ["NETA [M3/D]", "GEO_LATITUDE", "GEO_LONGITUDE", "TIEMPO PLANIFICADO"]
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    flash(f"❌ Faltan las siguientes columnas en el archivo: {', '.join(missing_cols)}")
                    return redirect(request.url)
                # Limpieza y conversión
                for col in required_columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce")
                df.dropna(inplace=True)
                if "ZONA" not in df.columns or "POZO" not in df.columns:
                    flash("❌ El archivo debe contener las columnas 'ZONA' y 'POZO'.")
                    return redirect(request.url)
                data_store["df"] = df
                flash("✅ Archivo cargado con éxito. A continuación se muestran los primeros datos:")
                table_html = df.head().to_html(classes="table table-striped", index=False)
                return render_template("upload_success.html", table=table_html)
        except Exception as e:
            flash(f"❌ Error al procesar el archivo: {e}")
            return redirect(request.url)
    return render_template("upload_file.html")

# ─────────────────────────────────────────────
# RUTA 2: GENERACIÓN AUTOMÁTICA DEL EXCEL Y VISTA PREVIA EDITABLE
# ─────────────────────────────────────────────
@app.route("/auto_generate", methods=["GET", "POST"])
def auto_generate():
    if request.method == "POST":
        try:
            rows = int(request.form.get("rows"))
            cols = int(request.form.get("cols"))
            columns = request.form.getlist("columns")
            data = []
            for i in range(rows):
                row_data = []
                for j in range(cols):
                    cell_value = request.form.get(f"cell_{i}_{j}")
                    row_data.append(cell_value)
                data.append(row_data)
            df_editado = pd.DataFrame(data, columns=columns)
            # Realiza conversiones si es necesario, por ejemplo a numérico.
            data_store["df"] = df_editado
            flash("✅ Archivo automático modificado y guardado con éxito.")
            return redirect(url_for("filter_zonas"))
        except Exception as e:
            app.logger.error("Error en auto_generate (POST): %s", e)
            flash(f"❌ Error al procesar la edición del archivo: {e}")
            # En vez de redirigir a request.url, redirige a una página de error o muestra el mensaje.
            return render_template("error.html", error_message=str(e))
    else:
        try:
            # Aquí se integra el código real de obtención del Excel
            conn_str_perdidas    = "oracle+cx_oracle://RY16123:Luciano280@suarbultowp01:1521/pcct"
            conn_str_tiempo      = "oracle+cx_oracle://RY16123:Luciano280@slpazrusora09:1527/PSOL"
            conn_str_coordenadas = "oracle+cx_oracle://RY16123:Luciano280@slpazrusora09:1527/PSOL"
            conn_str_nombrepozo  = "oracle+cx_oracle://RY16123:Luciano280@suarbuworap09:1527/psfu"

            engine_perdidas    = create_engine(conn_str_perdidas)
            engine_tiempo      = create_engine(conn_str_tiempo)
            engine_coordenadas = create_engine(conn_str_coordenadas)
            engine_nombrepozo  = create_engine(conn_str_nombrepozo)

            query_perdidas = """
            SELECT O100108.COMP_S_NAME, O100156.NET_LOSE, O100131.ORG_ENT_DS3, O100131.ORG_ENT_DS4, 
                   O100131.ORG_ENT_DS5, O100156.PROD_DT, O100175.PROD_STATUS_CD, O100175.PROD_STATUS_DS, 
                   O100193.REF_DS, O100156.WAT_LOSE, SUM(O100156.WAT_LOSE) AS SUM_WAT_LOSE, 
                   SUM(O100156.NET_LOSE) AS SUM_NET_LOSE, SUM(O100156.GAS_LOSE) AS SUM_GAS_LOSE
            FROM DISC_ADMIN_TOW.TOW_COMPLETACIONES O100108, DISC_ADMIN_TOW.TOW_JERARQUIA O100131, 
                 DISC_ADMIN_TOW.TOW_PERDIDAS O100156, DISC_ADMIN_TOW.TOW_RUBROS_DE_PARO O100175, 
                 DISC_ADMIN_TOW.TOW_REF_GRAN_RUBRO O100193
            WHERE ( ( O100193.REF_ID = O100175.USER_A1 ) 
                    AND ( O100131.ASSGN_SK = O100108.COMP_SK ) 
                    AND ( O100156.CODIGO_RUBRO = O100175.PROD_STATUS_CD ) 
                    AND ( O100156.COMP_SK = O100108.COMP_SK ) ) 
                  AND ( O100131.ASSGN_SK_TYPE = 'CC' AND O100131.ORG_SK = 1 ) 
                  AND ( O100131.ORG_ENT_DS3 IN ('Las Heras CG - Canadon Escondida','Los Perales','El Guadal', 'Seco Leon - Pico Truncado') ) 
                  AND ( O100156.PROD_DT >= TO_DATE('20240101000000','YYYYMMDDHH24MISS') )
            GROUP BY O100108.COMP_S_NAME, O100156.NET_LOSE, O100131.ORG_ENT_DS3, O100131.ORG_ENT_DS4, 
                     O100131.ORG_ENT_DS5, O100156.PROD_DT, O100175.PROD_STATUS_CD, O100175.PROD_STATUS_DS, 
                     O100193.REF_DS, O100156.WAT_LOSE, O100156.GAS_LOSE
            """
            df_perdidas = pd.read_sql_query(query_perdidas, engine_perdidas)

            query_tiempo = """
            SELECT CD_SITE.loc_region, CD_WELL.battery_directions, CD_WELL.loc_state, 
                   CD_WELL.well_legal_name, DM_EVENT.date_ops_end, DM_EVENT.date_ops_start, 
                   DM_EVENT.event_code, DM_EVENT.status_end, DM_EVENT.event_objective_1, 
                   DM_EVENT.event_objective_2, DM_REPORT_JOURNAL.report_no, 
                   Sum(DM_WELL_PLAN_OP.target_duration) AS SUM_TARGET_DURATION
            FROM DM_WELL_PLAN_OP, DM_WELL_PLAN, DM_EVENT, CD_WELL, CD_SITE, DM_REPORT_JOURNAL
            WHERE (((CD_WELL.battery_directions IN ('El Guadal','Las Heras CG - Canadon Escondida',
                   'Los Perales','Seco Leon - Pico Truncado')))) 
                  AND ((DM_WELL_PLAN.well_id = DM_WELL_PLAN_OP.well_id AND 
                        DM_WELL_PLAN.wellbore_id = DM_WELL_PLAN_OP.wellbore_id AND 
                        DM_WELL_PLAN.well_plan_id = DM_WELL_PLAN_OP.well_plan_id) 
                       AND (DM_EVENT.well_id = DM_WELL_PLAN.well_id AND 
                            DM_EVENT.event_id = DM_WELL_PLAN.event_id) 
                       AND (CD_WELL.well_id = DM_EVENT.well_id) 
                       AND (CD_SITE.site_id = CD_WELL.site_id) 
                       AND (DM_REPORT_JOURNAL.report_journal_id = DM_WELL_PLAN.report_journal_id) 
                       AND (CD_WELL.well_id = DM_REPORT_JOURNAL.well_id) 
                       AND (DM_EVENT.well_id = DM_REPORT_JOURNAL.well_id AND 
                            DM_EVENT.event_id = DM_REPORT_JOURNAL.event_id) 
                       AND (CD_WELL.well_id = DM_WELL_PLAN.well_id))
            GROUP BY CD_SITE.loc_region, CD_WELL.battery_directions, CD_WELL.loc_state, 
                     CD_WELL.well_legal_name, DM_EVENT.date_ops_end, DM_EVENT.date_ops_start, 
                     DM_EVENT.event_code, DM_EVENT.status_end, DM_EVENT.event_objective_1, 
                     DM_EVENT.event_objective_2, DM_REPORT_JOURNAL.report_no
            ORDER BY 4 ASC, 10 ASC, 6 ASC, 5 ASC
            """
            df_tiempo = pd.read_sql_query(query_tiempo, engine_tiempo)

            query_coordenadas = """
            SELECT CD_WELL.api_no, CD_WELL.battery_directions, 
                   (CD_WELL.geo_offset_east * 0.3048000000012) AS OFFSET_EAST,
                   (CD_WELL.geo_offset_north * 0.3048000000012) AS OFFSET_NORTH, 
                   CD_WELL.loc_state, CD_WELL.well_legal_name, 
                   CD_WELL.geo_latitude, CD_WELL.geo_longitude
            FROM CD_WELL, CD_SITE
            WHERE (((CD_WELL.loc_state = 'SANTA CRUZ' ))) 
              AND ((CD_SITE.site_id = CD_WELL.site_id))
            GROUP BY CD_WELL.well_legal_name, CD_WELL.loc_state, CD_WELL.battery_directions, 
                     (CD_WELL.geo_offset_north * 0.3048000000012), 
                     (CD_WELL.geo_offset_east * 0.3048000000012), 
                     CD_WELL.api_no, CD_WELL.geo_latitude, CD_WELL.geo_longitude
            ORDER BY 2 ASC, 6 ASC
            """
            df_coordenadas = pd.read_sql_query(query_coordenadas, engine_coordenadas)

            query_nombre_pozo = """
            SELECT DISTINCT DBU_FIC_ORG_ESTRUCTURAL.NOMBRE_CORTO, 
                   DBU_FIC_ORG_ESTRUCTURAL.NIVEL_3, 
                   FIC_CONTROLES.EFF_DT, 
                   FIC_CONTROLES.PROPOSITO_CONTROL, 
                   DBU_FIC_ORG_ESTRUCTURAL.NOMBRE_POZO, 
                   DBU_FIC_ORG_ESTRUCTURAL.NOMBRE_CORTO_POZO, 
                   FIC_CONTROLES.PROD_GAS, 
                   FIC_CONTROLES.PROD_OIL, 
                   FIC_CONTROLES.PROD_WAT+FIC_CONTROLES.PROD_OIL AS PROD_WAT_OIL
            FROM DISC_ADMINS.DBU_FIC_ORG_ESTRUCTURAL DBU_FIC_ORG_ESTRUCTURAL, 
                 DISC_ADMIN.FIC_CONTROLES FIC_CONTROLES
            WHERE ( FIC_CONTROLES.COMP_SK = DBU_FIC_ORG_ESTRUCTURAL.COMP_SK ) 
                  AND ( FIC_CONTROLES.PROPOSITO_CONTROL = 'Alocación' ) 
                  AND ( FIC_CONTROLES.EFF_DT >= TO_DATE('20220101000000','YYYYMMDDHH24MISS') ) 
                  AND ( DBU_FIC_ORG_ESTRUCTURAL.NIVEL_3 IN ('Las Heras CG - Canadon Escondida',
                       'Los Perales','El Guadal', 'Seco Leon - Pico Truncado') )
            """
            df_nombre_pozo = pd.read_sql_query(query_nombre_pozo, engine_nombrepozo)

            # ── 2) TRANSFORMACIONES ──
            # A) Transformación de la consulta PÉRDIDAS
            df_perdidas = df_perdidas[df_perdidas['ref_ds'] == 'ESPERA TRACTOR']
            df_perdidas['prod_dt'] = pd.to_datetime(df_perdidas['prod_dt']).dt.date
            yesterday = (datetime.now() - timedelta(days=1)).date()
            df_perdidas = df_perdidas[df_perdidas['prod_dt'] == yesterday]
            df_perdidas = df_perdidas[['comp_s_name', 'net_lose', 'wat_lose', 'sum_gas_lose',
                                       'org_ent_ds3', 'prod_dt', 'ref_ds', 'org_ent_ds5']]
            df_perdidas = df_perdidas.groupby(
                ['comp_s_name', 'org_ent_ds3', 'prod_dt', 'ref_ds', 'org_ent_ds5'],
                as_index=False
            ).agg({
                'net_lose': 'sum',
                'wat_lose': 'sum',
                'sum_gas_lose': 'sum'
            })
            df_perdidas.rename(columns={
                'comp_s_name': 'NOMBRE_CORTO',
                'net_lose': 'NETA [M3/D]',
                'wat_lose': 'WAT_LOSE',
                'sum_gas_lose': 'GAS [M3/d]',
                'org_ent_ds3': 'ZONA',
                'ref_ds': 'RUBRO',
                'org_ent_ds5': 'BATERÍA',
                'prod_dt': 'PROD_DT'
            }, inplace=True)
            df_perdidas = df_perdidas[['NOMBRE_CORTO', 'NETA [M3/D]', 'WAT_LOSE', 'GAS [M3/d]',
                                       'ZONA', 'PROD_DT', 'RUBRO', 'BATERÍA']]

            # B) Transformación TIEMPO PLANIFICADO
            df_tiempo = df_tiempo[(df_tiempo['status_end'].isnull()) | (df_tiempo['status_end'].str.strip() == '')]
            df_tiempo['date_ops_start'] = pd.to_datetime(df_tiempo['date_ops_start'])
            df_tiempo.sort_values('date_ops_start', ascending=False, inplace=True)
            df_tiempo = df_tiempo[['well_legal_name', 'date_ops_start', 'sum_target_duration']]
            df_tiempo = df_tiempo.drop_duplicates(subset=['well_legal_name'], keep='first')
            df_tiempo.rename(columns={
                'well_legal_name': 'NOMBRE_POZO',
                'date_ops_start': 'FECHA',
                'sum_target_duration': 'TIEMPO PLANIFICADO'
            }, inplace=True)

            # C) Transformación COORDENADAS
            df_coordenadas = df_coordenadas[['well_legal_name', 'geo_latitude', 'geo_longitude']]
            df_coordenadas.rename(columns={
                'well_legal_name': 'NOMBRE_POZO',
                'geo_latitude': 'GEO_LATITUDE',
                'geo_longitude': 'GEO_LONGITUDE'
            }, inplace=True)

            # D) Transformación NOMBRE DE POZO
            df_nombre_pozo = df_nombre_pozo[['nombre_corto', 'nombre_pozo', 'nombre_corto_pozo']]
            df_nombre_pozo = df_nombre_pozo.drop_duplicates(subset=['nombre_corto'])

            # ── 3) UNIÓN Y ACTUALIZACIÓN DE DATOS ──
            df_perdidas = pd.merge(
                df_perdidas,
                df_nombre_pozo[['nombre_corto', 'nombre_pozo']],
                how='left',
                left_on='NOMBRE_CORTO',
                right_on='nombre_corto'
            )
            df_perdidas['NOMBRE_CORTO'] = df_perdidas['nombre_pozo'].combine_first(df_perdidas['NOMBRE_CORTO'])
            df_perdidas.drop(['nombre_corto', 'nombre_pozo'], axis=1, inplace=True)
            df_perdidas.rename(columns={'NOMBRE_CORTO': 'NOMBRE_POZO'}, inplace=True)

            df_final = pd.merge(df_perdidas, df_tiempo, how='left', on='NOMBRE_POZO')
            df_final = pd.merge(df_final, df_coordenadas, how='left', on='NOMBRE_POZO')
            df_final = df_final[['NOMBRE_POZO',
                                 'NETA [M3/D]',
                                 'WAT_LOSE',
                                 'GAS [M3/d]',
                                 'PROD_DT',
                                 'RUBRO',
                                 'GEO_LATITUDE',
                                 'GEO_LONGITUDE',
                                 'BATERÍA',
                                 'ZONA',
                                 'TIEMPO PLANIFICADO']]
            df_final.rename(columns={'NOMBRE_POZO': 'POZO'}, inplace=True)

            # Almacenamos el DataFrame generado y lo enviamos a la vista previa editable
            data_store["df"] = df_final
            return render_template("auto_preview.html", 
                                   table=df_final.head().to_html(classes="table table-striped", index=False),
                                   columns=list(df_final.columns), 
                                   rows=len(df_final), 
                                   data=df_final.values.tolist())
        except Exception as e:
            app.logger.error("Error en auto_generate (GET): %s", e)
            flash(f"❌ Error al generar el Excel automático: {e}")
            return render_template("error.html", error_message=str(e))

# ─────────────────────────────────────────────
# RUTA 3: FILTRADO DE ZONAS Y SELECCIÓN DE PULLING
# ─────────────────────────────────────────────
@app.route("/filter", methods=["GET", "POST"])
def filter_zonas():
    if "df" not in data_store:
        flash("Debes subir un archivo Excel primero.")
        return redirect(url_for("upload_file"))
    df = data_store["df"]
    zonas_disponibles = sorted(df["ZONA"].unique().tolist())
    if request.method == "POST":
        zonas_seleccionadas = request.form.getlist("zonas")
        pulling_count = request.form.get("pulling_count")
        if not zonas_seleccionadas:
            flash("Debes seleccionar al menos una zona.")
            return redirect(request.url)
        try:
            pulling_count = int(pulling_count)
        except:
            pulling_count = 3
        df_filtrado = df[df["ZONA"].isin(zonas_seleccionadas)].copy()
        data_store["df_filtrado"] = df_filtrado
        pozos = sorted(df_filtrado["POZO"].unique().tolist())
        data_store["pozos_disponibles"] = pozos
        data_store["pulling_count"] = pulling_count
        flash(f"Zonas seleccionadas: {', '.join(zonas_seleccionadas)}")
        return redirect(url_for("select_pulling"))
    checkbox_html = ""
    for zona in zonas_disponibles:
        checkbox_html += f'<input type="checkbox" name="zonas" value="{zona}"> {zona}<br>'
    return render_template("filter_zonas.html", checkbox_html=checkbox_html)

# ─────────────────────────────────────────────
# RUTA 4: SELECCIÓN DE POZOS PARA PULLING
# ─────────────────────────────────────────────
@app.route("/select_pulling", methods=["GET", "POST"])
def select_pulling():
    if "df_filtrado" not in data_store:
        flash("Debes filtrar las zonas primero.")
        return redirect(url_for("filter_zonas"))
    df_filtrado = data_store["df_filtrado"]
    pozos_disponibles = data_store.get("pozos_disponibles", [])
    pulling_count = data_store.get("pulling_count", 3)
    if request.method == "POST":
        pulling_data = {}
        seleccionados = []
        for i in range(1, pulling_count + 1):
            pozo = request.form.get(f"pulling_pozo_{i}")
            tiempo_restante = request.form.get(f"pulling_tiempo_{i}", 0)
            try:
                tiempo_restante = float(tiempo_restante)
            except:
                tiempo_restante = 0.0
            pulling_data[f"Pulling {i}"] = {
                "pozo": pozo,
                "tiempo_restante": tiempo_restante,
            }
            seleccionados.append(pozo)
        if len(seleccionados) != len(set(seleccionados)):
            flash("Error: No puedes seleccionar el mismo pozo para más de un pulling.")
            return redirect(request.url)
        for pulling, data in pulling_data.items():
            pozo = data["pozo"]
            registro = df_filtrado[df_filtrado["POZO"] == pozo].iloc[0]
            data["lat"] = registro["GEO_LATITUDE"]
            data["lon"] = registro["GEO_LONGITUDE"]
        data_store["pulling_data"] = pulling_data
        todos_pozos = sorted(df_filtrado["POZO"].unique().tolist())
        data_store["pozos_disponibles"] = sorted([p for p in todos_pozos if p not in seleccionados])
        flash("Selección de Pulling confirmada.")
        return redirect(url_for("hs_disponibilidad"))
    select_options = ""
    for pozo in data_store.get("pozos_disponibles", []):
        select_options += f'<option value="{pozo}">{pozo}</option>'
    form_html = ""
    for i in range(1, pulling_count + 1):
        form_html += f"""
            <h3>Pulling {i}</h3>
            <label>Pozo para Pulling {i}:</label>
            <select name="pulling_pozo_{i}" class="form-select w-50">
                {select_options}
            </select><br>
            <label>Tiempo restante (h) para Pulling {i}:</label>
            <input type="number" step="0.1" name="pulling_tiempo_{i}" value="0.0" class="form-control w-25"><br>
            <hr>
        """
    return render_template("select_pulling.html", form_html=form_html)

# ─────────────────────────────────────────────
# RUTA 5: INGRESO DE HS DISPONIBILIDAD
# ─────────────────────────────────────────────
@app.route("/hs", methods=["GET", "POST"])
def hs_disponibilidad():
    if "pulling_data" not in data_store:
        flash("Debes seleccionar los pozos para pulling primero.")
        return redirect(url_for("select_pulling"))
    pozos_disponibles = data_store.get("pozos_disponibles", [])
    if not pozos_disponibles:
        flash("No hay pozos disponibles para asignar HS.")
        return redirect(url_for("select_pulling"))
    if request.method == "POST":
        hs_disponibilidad = {}
        for pozo in pozos_disponibles:
            hs_val = request.form.get(f"hs_{pozo}", 0)
            try:
                hs_val = float(hs_val)
            except:
                hs_val = 0.0
            hs_disponibilidad[pozo] = hs_val
        data_store["hs_disponibilidad"] = hs_disponibilidad
        flash("HS Disponibilidad confirmada.")
        return redirect(url_for("assign"))
    form_fields = ""
    for pozo in pozos_disponibles:
        form_fields += f"""
            <div class="mb-3">
              <label>{pozo} (HS):</label>
              <input type="number" step="0.1" name="hs_{pozo}" value="0.0" class="form-control w-25">
            </div>
        """
    return render_template("hs_disponibilidad.html", form_fields=form_fields)

# ─────────────────────────────────────────────
# RUTA 6: EJECUCIÓN DEL PROCESO DE ASIGNACIÓN
# ─────────────────────────────────────────────
@app.route("/assign", methods=["GET"])
def assign():
    if "hs_disponibilidad" not in data_store or not data_store.get("hs_disponibilidad"):
        flash("Debes confirmar la disponibilidad de HS antes de continuar.")
        return redirect(url_for("hs_disponibilidad"))
    df = data_store["df"]
    pulling_data = data_store["pulling_data"]
    hs_disponibilidad = data_store["hs_disponibilidad"]
    matriz_prioridad = []
    pozos_ocupados = set()
    pulling_lista = list(pulling_data.items())

    def calcular_coeficiente(pozo_referencia, pozo_candidato):
        registro_ref = df[df["POZO"] == pozo_referencia].iloc[0]
        registro_cand = df[df["POZO"] == pozo_candidato].iloc[0]
        distancia = geodesic(
            (registro_ref["GEO_LATITUDE"], registro_ref["GEO_LONGITUDE"]),
            (registro_cand["GEO_LATITUDE"], registro_cand["GEO_LONGITUDE"])
        ).kilometers
        neta = registro_cand["NETA [M3/D]"]
        hs_planificadas = registro_cand["TIEMPO PLANIFICADO"]
        coeficiente = neta / (hs_planificadas + (distancia * 0.5))
        return coeficiente, distancia

    def asignar_pozos(pulling_asignaciones, nivel):
        no_asignados = [p for p in data_store["pozos_disponibles"] if p not in pozos_ocupados]
        for pulling, data in pulling_lista:
            pozo_referencia = pulling_asignaciones[pulling][-1][0] if pulling_asignaciones[pulling] else data["pozo"]
            candidatos = []
            for pozo in no_asignados:
                tiempo_acumulado = sum(
                    df[df["POZO"] == p[0]]["TIEMPO PLANIFICADO"].iloc[0]
                    for p in pulling_asignaciones[pulling]
                )
                if hs_disponibilidad.get(pozo, 0) <= (data["tiempo_restante"] + tiempo_acumulado):
                    coef, dist = calcular_coeficiente(pozo_referencia, pozo)
                    candidatos.append((pozo, coef, dist))
            candidatos.sort(key=lambda x: (-x[1], x[2]))
            if candidatos:
                mejor_candidato = candidatos[0]
                pulling_asignaciones[pulling].append(mejor_candidato)
                pozos_ocupados.add(mejor_candidato[0])
                if mejor_candidato[0] in no_asignados:
                    no_asignados.remove(mejor_candidato[0])
            else:
                flash(f"⚠️ No hay pozos disponibles para asignar como {nivel} en {pulling}.")
        return pulling_asignaciones

    pulling_asignaciones = {pulling: [] for pulling, _ in pulling_lista}
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+1")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+2")
    pulling_asignaciones = asignar_pozos(pulling_asignaciones, "N+3")

    for pulling, data in pulling_lista:
        pozo_actual = data["pozo"]
        registro_actual = df[df["POZO"] == pozo_actual].iloc[0]
        neta_actual = registro_actual["NETA [M3/D]"]
        tiempo_restante = data["tiempo_restante"]
        seleccionados = pulling_asignaciones.get(pulling, [])[:3]
        while len(seleccionados) < 3:
            seleccionados.append(("N/A", 1, 1))
        registro_n1 = df[df["POZO"] == seleccionados[0][0]]
        if not registro_n1.empty:
            tiempo_planificado_n1 = registro_n1["TIEMPO PLANIFICADO"].iloc[0]
            neta_n1 = registro_n1["NETA [M3/D]"].iloc[0]
        else:
            tiempo_planificado_n1 = 1
            neta_n1 = 1

        coeficiente_actual = neta_actual / tiempo_restante if tiempo_restante > 0 else 0
        distancia_n1 = seleccionados[0][2]
        coeficiente_n1 = neta_n1 / ((0.5 * distancia_n1) + tiempo_planificado_n1)

        if coeficiente_actual < coeficiente_n1:
            recomendacion = "Abandonar pozo actual y moverse al N+1"
        else:
            recomendacion = "Continuar en pozo actual"

        matriz_prioridad.append([
            pulling,
            pozo_actual,
            neta_actual,
            tiempo_restante,
            seleccionados[0][0],
            seleccionados[0][1],
            seleccionados[0][2],
            seleccionados[1][0],
            seleccionados[1][1],
            seleccionados[1][2],
            seleccionados[2][0],
            seleccionados[2][1],
            seleccionados[2][2],
            recomendacion
        ])

    columns = [
        "Pulling", "Pozo Actual", "Neta Actual", "Tiempo Restante (h)",
        "N+1", "Coeficiente N+1", "Distancia N+1 (km)",
        "N+2", "Coeficiente N+2", "Distancia N+2 (km)",
        "N+3", "Coeficiente N+3", "Distancia N+3 (km)", "Recomendación"
    ]
        
    df_prioridad = pd.DataFrame(matriz_prioridad, columns=columns)
    
    def highlight_reco(val):
        if "Abandonar" in val:
            return "color: red; font-weight: bold;"
        else:
            return "color: green; font-weight: bold;"
    
    df_styled = df_prioridad.style.hide_index().set_properties(
        **{"text-align": "center", "white-space": "nowrap"}
    ).format(precision=2).set_table_styles([
        {"selector": "th", "props": [("background-color", "#f8f9fa"), ("color", "#333"), ("font-weight", "bold"), ("text-align", "center")]},
        {"selector": "td", "props": [("padding", "8px")]},
        {"selector": "tbody tr:nth-child(even)", "props": [("background-color", "#f2f2f2")]},
    ]).applymap(lambda val: "font-weight: bold; color: black;", subset=["Pozo Actual", "N+1", "N+2", "N+3"]).applymap(highlight_reco, subset=["Recomendación"])
    
    table_html = df_styled.render()
    
    flash("Proceso de asignación completado.")
    return render_template("assign_result.html", table=table_html)

if __name__ == "__main__":
    app.run(debug=True)
