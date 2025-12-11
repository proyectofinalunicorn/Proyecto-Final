## IMPORTACION DE BIBLIOTECAS
import pandas as pd
import numpy as np
import yfinance as yf
import requests
import streamlit as st
import os
import time
from datetime import datetime, date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Float, Integer, Text, text
from sqlalchemy.pool import NullPool

##INSTRUCTIVO PARA DESCARGAR REPORTE DE BALANZ

@st.dialog("📥 Cómo descargar el reporte de Balanz")
def mostrar_instructivo():
    st.markdown("""
    Sigue estos pasos para obtener el reporte de Balanz:
    
    **1. Ingresa a Balanz**
    Inicia sesión en tu cuenta desde la web.
    """)
    st.image("res Folder/paso1.png", use_container_width=True)

    st.markdown("""
    **2. Ve a la sección de Reportes**
    """)
    st.image("res Folder/paso2.png", use_container_width=True)
    
    st.markdown("""
    **3. Configura el reporte**
    Es muy importante que selecciones estas opciones exactas:
    * **Reporte:** Resultados del período
    * **Período:** Selecciona el rango de fechas (ej. Desde el inicio de tus inversiones hasta hoy).
    * **Informe:** COMPLETO.
    """)
    st.image("res Folder/paso3.png", use_container_width=True)
    st.image("res Folder/paso4.png", use_container_width=True)

    st.markdown("""
    **4. Descargar**
    Haz clic en el botón **Descargar**. Se descargará un archivo `.xlsx`.
    
    ---
    ✅ **¡Listo!** Ahora cierra esta ventana y sube ese archivo.
    """)

## CREACIÓN DE VARIABLE PARA CONTROLAR QUE EL PROCESO SE EJECUTE CORRECTAMENTE
if 'procesamiento_listo' not in st.session_state:
    st.session_state.procesamiento_listo = False
if 'ultimo_mensaje' not in st.session_state:
    st.session_state.ultimo_mensaje = ""
    

## CREACION DE LA FUNCION PARA PROCESAR LOS DATOS Y GUARDARLOS EN SQL

def procesar_y_guardar_en_sql(archivo_subido, db_host, db_name, db_user, db_pass):
    try:

        ## CREACION DE BARRA DE PROGRESO PARA INDICAR AVANCE
        barra_progreso = st.progress(0, text="Iniciando:")
        
        ## IMPORTACION DE BASE DE DATOS
        st.write(f"Leyendo archivo: {archivo_subido.name}...")
        if archivo_subido.name.endswith(('.xlsx')):
            df = pd.read_excel(archivo_subido, sheet_name="resultados_por_lotes_finales")
        else:
            st.error("Error: Formato de archivo no soportado.")
            return False, "Error de archivo"

        ## RENOMBRAR COLUMNAS
        df.rename(columns = {"Cantidad": "cantidad", 
                             "Descripcion": "descripcion", 
                             "Fecha": "fecha", 
                             "Fecha Lote": "fecha_descarga",
                             "Gastos": "gastos",
                             "Moneda": "moneda",
                             "Operacion": "operacion",
                             "Precio Compra": "precio_compra",
                             "Ticker": "ticker",
                             "Tipo": "tipo",
                             "DolarCCL": "dolar_ccl",
                             "DolarMEP": "dolar_mep",
                             "DolarOficial": "dolar_oficial"},
                  inplace = True)

        ## ELIMINACIÓN DE COLUMNAS INNECESARIAS
        columnas_a_borrar = ["dolar_ccl", "operacion"]
        columnas_existentes = [col for col in columnas_a_borrar if col in df.columns]
        df.drop(columnas_existentes, axis=1, inplace=True)

        ## FILTRO DE DATOS POR TIPO DE ACTIVO
        df_cedears = df[df.tipo == "Cedears"].copy()

        ## CAMBIO DE TIPO DE DATO A FECHA
        df_cedears.fecha = pd.to_datetime(df_cedears.fecha)
        df_cedears.fecha_descarga = pd.to_datetime(df_cedears.fecha_descarga)
        
        # ORDENAR POR FECHA
        if "fecha" in df_cedears.columns:
            df_cedears.sort_values(by="fecha", inplace=True)
            df_cedears.reset_index(drop=True, inplace=True)
        else:
            st.warning("Error: No se pudo ordenar por fecha")

        ## CALCULO DE COSTO EN PESOS ARGENTINOS
        df_cedears["costo_ars"] = (df_cedears.cantidad * df_cedears.precio_compra)+df_cedears.gastos

        ## CALCULO DE COSTO EN USD (SEGÚN FECHA)
        df_cedears["costo_usd"] = np.where(
            df_cedears.fecha < pd.to_datetime("2025-04-15"),
            df_cedears.costo_ars / df_cedears.dolar_mep,
            df_cedears.costo_ars / np.minimum(df_cedears.dolar_oficial, df_cedears.dolar_mep)
        )

        # ACTUALIZACION DE BARRA DE PROGRESO
        barra_progreso.progress(0.10)
        
        ## LISTA UNICA DE ACCIONES
        tickers_unicos = df_cedears.ticker.unique()

        ## CREACIÓN DE DICCIONARIO PARA LA COTIZACION ACTUAL
        cotizacion_actual = {}

        # ACTUALIZACIÓN DE PROCESO
        st.write("Obteniendo cotizaciones...")

        ## CREACION DE VARIABLE PARA IDENTIFICAR SI HUBO UN ERROR AL MOMENTO DE OBTENER LAS COTIZACIONES
        error_cotizaciones = False

        ## OBTENCION DE COTIZACION ACTUALIZADA
        total_tickers = len(tickers_unicos)    
        
        for i, ticker in enumerate(tickers_unicos):
            ticker_argentina = ticker + ".BA"
            try:
                ticker_obj = yf.Ticker(ticker_argentina)
                info = ticker_obj.info
                if 'regularMarketPrice' in info and info['regularMarketPrice'] is not None:
                    precio = info['regularMarketPrice']
                else:
                    precio = ticker_obj.fast_info["last_price"]
                cotizacion_actual[ticker] = precio

                ## SE INCLUYE TIEMPO DE ESPERA EN LA EJECUCIÓN PARA EVITAR ERRORES AL MOMENTO DE OBTENER LA COTIZACION
                time.sleep(0.8)
                
            except Exception as e:
                st.warning(f"Error al obtener la cotización de {ticker}: {e}")
                error_cotizaciones = True

            # ACTUALIZACION DE BARRA DE PROGRESO
            avance = (i + 1) / total_tickers
            barra_progreso.progress(0.10 + (avance * 0.60), text=f"Cotización de {ticker} ({i+1}/{total_tickers})")

        if error_cotizaciones:
            return False, "Error al obtener las cotizaciones. Proceso detenido."


        ## CALCULO DE TENENCIA TOTAL ACTUALIZADA EN PESOS ARGENTINOS
        df_cedears["tenencia_ars"] = (df_cedears.cantidad * df_cedears.ticker.map(cotizacion_actual).fillna(0))*(1-0.006)

        ## CREACION DE FUNCION AUXILIAR PARA OBTENER VALOR DE DOLAR ACTUALIZADO
        def obtener_valores_dolar():
            api_url = "https://dolarapi.com/v1/dolares"
            try:
                response = requests.get(api_url)
                response.raise_for_status()
                data = response.json()
                df_dolar_api = pd.DataFrame(data)
                valor_oficial = df_dolar_api.loc[df_dolar_api['casa'] == 'oficial', 'venta'].values[0]
                valor_mep = df_dolar_api.loc[df_dolar_api['casa'] == 'bolsa', 'venta'].values[0]
                return valor_oficial, valor_mep
            except Exception as e:
                st.error(f"Error al cargar valores de dólar: {e}")
                return None, None

        ## EJECUCIÓN DE LA FUNCIÓN AUXILIAR
        st.write("Obteniendo valor del dólar...")
        dolar_oficial, dolar_mep = obtener_valores_dolar()
        if dolar_oficial is None or dolar_mep is None:
            raise Exception("No se pudo obtener el valor del dólar, el proceso no puede continuar.")

        # ACTUALIZACION DE BARRA DE PROGRESO
        barra_progreso.progress(0.70, text="Cotizaciones obtenidas.")

        ## CALCULO DE TENENCIA TOTAL ACTUALIZADA EN USD (utilizando el tipo de cambio mas bajo)
        df_cedears["tenencia_usd"] = df_cedears.tenencia_ars / np.minimum(dolar_oficial, dolar_mep)

        ## CÁLCULO DE GANANCIA O PERDIDA EN PESOS ARGENTINOS
        df_cedears["resultados_ars"] = df_cedears.tenencia_ars - df_cedears.costo_ars

        ## CÁLCULO DE GANANCIA O PERDIDA EN DOLARES
        df_cedears["resultados_usd"] = df_cedears.tenencia_usd - df_cedears.costo_usd

        ## CÁLCULO DE RENDIMIENTO PORCENTUAL EN PESOS ARGENTINOS
        df_cedears["rendimiento_ars"] = round((df_cedears.tenencia_ars / df_cedears.costo_ars - 1) * 100, 2)

        ## CÁLCULO DE RENDIMIENTO PORCENTUAL EN DOLARES
        df_cedears["rendimiento_usd"] = round((df_cedears.tenencia_usd / df_cedears.costo_usd - 1) * 100, 2)

        ## AGRUPACION DE ACCIONES Y TOTALES
        df_cedears_analisis = df_cedears[["ticker", "cantidad", "costo_ars","costo_usd","tenencia_ars", "tenencia_usd", "resultados_ars", "resultados_usd"]]
        df_cedears_agrupado = df_cedears_analisis.groupby("ticker").sum().round(2)
        df_cedears_agrupado["rendimiento_ars"] = df_cedears_agrupado["resultados_ars"] / df_cedears_agrupado["costo_ars"]
        df_cedears_agrupado["rendimiento_usd"] = df_cedears_agrupado["resultados_usd"] / df_cedears_agrupado["costo_usd"]
        df_cedears_agrupado.reset_index(inplace=True)

        ## MODIFICACION DEL DATAFRAME PARA FILTRAR POR MONEDA
        # AÑADIR FECHA DE EJECUCIÓN
        df_cedears_agrupado['fecha_ejecucion'] = datetime.now().date()

        # MODIFICACIÓN PARA PODER FILTRAR POR TIPO DE MONEDA
        try:
            df_final_largo = pd.wide_to_long(
                df_cedears_agrupado,
                # PREFIJO DE COLUMNA
                stubnames=['costo', 'tenencia', 'resultados', 'rendimiento'],
                # COLUMNAS QUE NO DEBEN PIVOTARSE
                i=['ticker', 'cantidad', 'fecha_ejecucion'],
                # CREACION DE COLUMNA POR TIPO DE MONEDA
                j='moneda',
                # CONECTOR ENTRE PREFIJO Y SUFIJO
                sep='_',
                # SUFIJO DE COLUMNA
                suffix='(ars|usd)'
            )
            # RESET DE INDICES
            df_final_listo = df_final_largo.reset_index()
        except Exception as e:
            st.error(f"Error en wide_to_long: {e}")
            raise e

        # ACTUALIZACION DE BARRA DE PROGRESO
        barra_progreso.progress(0.80, text="Guardando en Base de Datos...")
        st.write("Conectando a la base de datos...")

        ## DATOS DE CONEXION A SUPABASE (SQL)        
        connection_url = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require'

        ## GUARDADO DE DF_CEDEARS CON PRIMARYKEY EN SUPABASE (SQL)
        try:
            engine = create_engine(connection_url, poolclass=NullPool)
            with engine.begin() as connection:
                # ESTRUCTURA DE LA TABLA
                metadata = MetaData()
                table_name = 'cedears'
                cedears_table = Table(
                    table_name,
                    metadata,
                    Column('id_operacion', Integer, primary_key=True, autoincrement=True),
                    Column('cantidad', Float),
                    Column('descripcion', Text),
                    Column('fecha', Date),
                    Column('fecha_descarga', Date),
                    Column('gastos', Float),
                    Column('moneda', String),
                    Column('precio_compra', Float),
                    Column('ticker', String),
                    Column('tipo', String),
                    Column('dolar_mep', Float),
                    Column('dolar_oficial', Float),
                    Column('costo_ars', Float),
                    Column('costo_usd', Float),
                    Column('tenencia_ars', Float),
                    Column('tenencia_usd', Float),
                    Column('resultados_ars', Float),
                    Column('resultados_usd', Float),
                    Column('rendimiento_ars', Float),
                    Column('rendimiento_usd', Float)
                )
                metadata.create_all(engine)
                st.write(f"Tabla '{table_name}' creada.")

                # ELIMINACION DE DATOS EXISTENTES
                connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))
                
                # INSERCIÓN DE DATOS
                df_cedears.to_sql(
                    table_name,
                    connection,
                    if_exists='append',
                    index=False
                )
        except Exception as e:
            raise e

        # ACTUALIZACION DE BARRA DE PROGRESO
        barra_progreso.progress(0.85, text="Guardando en Base de Datos...")

        ## GUARDADO DE DATOS HISTORICOS CEDEARS EN SQL
        try:
            engine_hist = create_engine(connection_url, poolclass=NullPool)
            
            # ESTRUCTURA DE LA TABLA
            metadata_hist = MetaData()
            table_name_hist = 'datos_historicos_cedears'
            historico_table = Table(
                table_name_hist,
                metadata_hist,
                Column('ticker', String, primary_key=True),
                Column('cantidad', Float),
                Column('fecha_ejecucion', Date, primary_key=True),
                Column('moneda', String, primary_key=True),
                Column('costo', Float),
                Column('tenencia', Float),
                Column('resultados', Float),
                Column('rendimiento', Float)
            )
        
            metadata_hist.create_all(engine_hist)
            st.write(f"Tabla '{table_name_hist}' creada.")

            # INSERCIÓN DE DATOS
            with engine_hist.connect() as connection:
                try:
                    if 'df_final_listo' not in locals():
                        st.warning("No existe df_final_listo, omitiendo carga de datos históricos.")
                    else:
                        df_final_listo.to_sql(
                            table_name_hist,
                            connection,
                            if_exists='append',
                            index=False
                        )
                
                # COMPROBACIÓN DE DATOS DUPLICADOS
                except Exception as ex:
                    if "violates unique constraint" in str(ex) or "duplicate key value" in str(ex):
                        st.warning(f"Advertencia: Datos ya cargados el día de hoy en '{table_name_hist}'.")
                    else:
                        raise ex
        except Exception as e:
             raise e

        # ACTUALIZACION DE BARRA DE PROGRESO
        barra_progreso.progress(0.90, text="Guardando en Base de Datos...")
        

        ## GUARDADO DE DATOS HISTORICOS DOLAR EN SQL
        st.write("Guardando histórico del dólar...")
        if dolar_oficial and dolar_mep:
            datos_dolar = [
                {'fecha': date.today(), 'tipo': 'Oficial', 'valor': dolar_oficial},
                {'fecha': date.today(), 'tipo': 'MEP', 'valor': dolar_mep}
            ]
            df_historico_dolar = pd.DataFrame(datos_dolar)

            try:
                engine_dolar = create_engine(connection_url, poolclass=NullPool)
                # ESTRUCTURA DE LA TABLA
                metadata_dolar = MetaData()
                table_name_dolar = 'historico_dolar'
                historico_dolar_table = Table(
                    table_name_dolar, metadata_dolar,
                    Column('fecha', Date, primary_key=True),
                    Column('tipo', String, primary_key=True),
                    Column('valor', Float)
                )
                metadata_dolar.create_all(engine_dolar)

                # INSERCIÓN DE DATOS
                with engine_dolar.connect() as connection:
                    try:
                        df_historico_dolar.to_sql(table_name_dolar, connection, if_exists='append', index=False)
                        st.write(f"Tabla '{table_name_dolar}' creada.")

                    # COMPROBACIÓN DE DATOS DUPLICADOS
                    except Exception as ex:
                        if "violates unique constraint" in str(ex) or "duplicate key value" in str(ex):
                            st.warning(f"Cotización del dólar ya cargada el día de hoy")
                        else:
                            st.error(f"Error SQL Dólar: {ex}")
            except Exception as e:
                st.error(f"Error de conexión Dólar: {e}")
        else:
            st.error("No se pudieron obtener los valores del dólar para guardar.")

        # ACTUALIZACION DE BARRA DE PROGRESO
        barra_progreso.progress(0.95, text="Guardando en Base de Datos...")

        ## FINALIZACIÓN EXITOSA
        barra_progreso.progress(1.0)
        return True, "¡Proceso completado con éxito!"

    ## DETALLE DE ERRORES
    except Exception as e:
        error_message = str(e).lower() 
        
        if "authentication failed" in error_message or "connection to server" in error_message or "duplicate sasl authentication" in error_message:
            return False, "❌ ¡Error de conexión! Revisa tu Host, Usuario, Contraseña y Nombre de Base de Datos."

        elif "worksheet named" in error_message and "not found" in error_message:
            return False, "❌ Error de Excel: No se encontró la hoja 'resultados_por_lotes_finales' en el archivo que subiste. Por favor, revisa el archivo."

        elif "specify an engine manually" in error_message:
            return False, "❌ Error de Archivo: El formato de Excel .xls no es compatible. Por favor, abre el archivo en Excel y guárdalo como .xlsx antes de subirlo."
        
        elif "relation" in error_message and "does not exist" in error_message:
            return False, f"❌ Error de Base de Datos: Una de las tablas no existe. (Detalle: {e})"
            
        else:
            st.error(f"Error detallado: {e}")
            return False, f"Error general en el procesamiento: {e}"

## ------------------------------------------------------------------------------------------------------------------------------------------------------
## CREACIÓN DEL FRONTEND PARA LA PAGINA WEB
st.image("res Folder/logo.png", use_container_width=True)
st.set_page_config(layout="centered", page_title="Análisis de inversiones")
st.title("💰 Análisis de inversiones")
st.write("Sube tu reporte de Balanz y completa los datos de tu Base de Datos de Supabase (PostgreSQL).")
st.write("El reporte a utilizar corresponde a 'Resultados del periodo' e informe 'Completo'")

## INSTRUCTIVO PARA DESCARGAR REPORTE DE BALANZ
if st.button("Ver instructivo de descarga", help="Haz clic para ver cómo bajar el Excel de Balanz"):
            mostrar_instructivo()
st.divider()

# FORMULARIO DE CARGA
with st.form(key="upload_form"):
    
    # CARGADOR DE ARCHIVOS
    uploaded_file = st.file_uploader("1. Sube tu archivo (Excel)", type=["xlsx"])
    
    st.divider()
    
    # CREDENCIALES DE SQL (SUPABASE)
    st.subheader("Credenciales de tu Base de Datos (Supabase)")
    st.info("Si aún no tienes credenciales, [crea tu cuenta gratuita en Supabase](https://supabase.com/dashboard/sign-up).")
    ## INSTRUCTIVO PARA OBTENER CREDENCIALES DE SUPABASE
    with st.expander("ℹ️ Ver instructivo: ¿Cómo obtengo estos datos?"):
        st.write("""
        1. Crea tu cuenta y proyecto en Supabase.
        2. Entra a tu proyecto.
        3. Ve al botón "Connect" que se encuentra en la parte superior de la pantalla:
        """)
        st.image("res Folder/captura_supabase.png", use_container_width=True)
        st.write("""
        4. Selecciona el Method "Session pooler":
        """)
        st.image("res Folder/captura_supabase_2.png", use_container_width=True)
        st.write("""
        5. Abre la opción "View parameters":
        """)
        st.image("res Folder/captura_supabase_3.png", use_container_width=True)
        st.write("""
        6. Ahí encontrarás el **Host**, **Database name** y **User**.
        7. *Nota: La contraseña es la que creaste al iniciar el proyecto. Para modificarla podes ingresar a "Database Settings" desde la parte inferior de la pantalla.*
        """)
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host (Servidor)", placeholder="aws.xxxxxxxx.supabase.com")
        db_user = st.text_input("Usuario")
    with col2:
        db_name = st.text_input("Nombre de la Base de Datos", "postgres")
        db_pass = st.text_input("Contraseña", type="password")

    st.divider()

    ## BOTON PARA INICIAR PROCESO
    submit_button = st.form_submit_button(
        label="🚀 Procesar y Cargar Datos", 
        use_container_width=True
    )

## ------------------------------------------------------------------------------------------------------------------------------------------------------
## EJECUCIÓN DEL CÓDIGO AL PRESIONAR EL BOTÓN

if submit_button:

    ## ACTUALIZACIÓN DE VARIABLE DE CONTROL 
    st.session_state.procesamiento_listo = False
    
    ## VERIFICA QUE LOS CAMPOS HAYAN SIDO COMPLETADOS
    if uploaded_file is not None and db_host and db_name and db_user and db_pass:

        ## COMPRUEBA LA CONEXIÓN A SQL 
        try:            
            url_check = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}?sslmode=require"
            engine_check = create_engine(url_check)
            with engine_check.connect() as conn:
                conn.execute(text("SELECT 1"))
                
        ## SI LA CONEXIÓN FALLA DETIENE EL PROCESO
        except Exception as e:
                st.error(f"❌ Error en las credenciales de Supabase")
                st.stop()
        
        ## ACTUALIZACIÓN DE PROCESO
        with st.spinner('Procesando archivo y conectando a Supabase... Esto puede tardar varios segundos...'):
            
            ## EJECUCIÓN DE LA FUNCION PRINCIPAL
            exito, mensaje = procesar_y_guardar_en_sql(
                uploaded_file, 
                db_host, 
                db_name, 
                db_user, 
                db_pass
            )
        
        ## RESULTADO EXITOSO
        if exito:
            st.session_state.procesamiento_listo = True
            st.session_state.ultimo_mensaje = mensaje
            st.success(mensaje)
            
            ## BOTÓN DE DESCARGA DEL INFORME DE POWER BI
            st.subheader("¡Tus datos están listos!")
            st.write("El siguiente paso es descargar tu plantilla de Power BI. Ábrela, introduce tus credenciales de Supabase (las mismas que usaste aquí) y haz clic en 'Actualizar'.")
            template_file_name = "res Folder/Reporte de inversiones - Power BI.pbit" 
            with open(template_file_name, "rb") as f:
                file_data = f.read()
            st.download_button(
                label="📥 Descargar el informe de Power BI",
                data=file_data,
                file_name="Reporte de inversiones - Power BI.pbit",
                mime="application/vnd.ms-powerbi.template",
                use_container_width=True
            )

            ## INSTRUCTIVO PARA SOLUCIONAR POSIBLE ERROR DENTRO DE POWER BI
            with st.expander("ℹ️ Solución de error en Power BI:"):
                st.write("""
                1. En caso de presentar el siguiente error deberás seguir los pasos detallados a continuación:
                """)
                st.image("res Folder/error1.png", use_container_width=True)
                st.write("""
                2. Ingresa a "Archivo", "Opciones y Configuración", y posteriormente a "Configuración de origen de datos":
                """)
                st.image("res Folder/error2.png", use_container_width=True)
                st.write("""
                3. Selecciona "Editar permisos":
                """)
                st.image("res Folder/error3.png", use_container_width=True)
                st.write("""
                4. Destilda la opción "Cifrar conexiones":
                """)
                st.image("res Folder/error4.png", use_container_width=True)
                st.write("""
                5. Por último, selecciona "Actualizar" en la pantalla de Inicio para obtener los datos:
                """)
                st.image("res Folder/error5.png", use_container_width=True)
                
     ## MENSAJES DE ERROR ANTE FALLA EN EL PROCESO           
        else:
            st.error(mensaje)
            
    else:
        st.warning("Por favor, completa TODOS los campos y sube un archivo.")







































