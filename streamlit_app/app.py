import streamlit as st
import os
from urllib.parse import quote
import numpy as np
import pandas as pd
import json
import time
import pytz
import logging
from datetime import date, datetime, timedelta
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import connection as cn


#############################################################
################### CONFIGURATION ###########################
#############################################################
# Esconder e importa de manera segura las creedenciales

# Credenciales mysql remoto
DATABASE = st.secrets["AWS_MYSQL"]["DATABASE"]
HOST = st.secrets["AWS_MYSQL"]["HOST"]
USER = st.secrets["AWS_MYSQL"]["USER"]
PASSWORD = st.secrets["AWS_MYSQL"]["USER_PASSWORD"]
PORT = st.secrets["AWS_MYSQL"]["PORT"]
USER_KEY = st.secrets["COORDINADOR"]["USER_KEY"]

#Informacion API flask
API_HOST = st.secrets["API"]["HOST"]
API_PORT = st.secrets["API"]["PORT"]


# Establecer motor de base de datos
engine, metadata = cn.establecer_engine(DATABASE, USER, PASSWORD, HOST, PORT, verbose=True)


CONN_STATUS = engine is not None

st.set_page_config(layout="wide")

# Get date in format YYYY-MM-DD and current hour

# Specify the timezone for Chile
chile_tz = pytz.timezone('America/Santiago')

# Create a datetime object in Chile's timezone
chile_datetime = datetime.now(chile_tz)

fecha = chile_datetime.strftime("%Y-%m-%d")
hora = chile_datetime.strftime("%H:%M:%S")


# round hora to nearest hour
hora = hora.split(':')
hora_redondeada = f'{hora[0]}:00:00'
hora_redondeada_cmg_programados = f'{hora[0]}:00'

naive_datetime = chile_datetime.astimezone().replace(tzinfo=None)
unixtime = int(time.mktime(naive_datetime.timetuple()))

def get_json_costo_marginal_online(fecha_gte, fecha_lte, barras, user_key=USER_KEY , verbose=False):
    """ Realiza un request para obtener costos marginales de las barras ingresadas. Devuelve una lista de diccionarios con
    la información solicitada. Los datos se filtran por barra, y solo se incluyen las filas que corresponden a barras
    especificadas en la lista barras.

    Args:
        fecha_gte (str): Fecha de inicio del rango en formato YYYY-MM-DD.
        fecha_lte (str): Fecha de término del rango en formato YYYY-MM-DD.
        user_key (str): Clave de usuario para autenticar la solicitud.
        barras (list): Lista con los nombres de las barras a incluir.

    Returns:
        list: Lista de diccionarios con la información solicitada para las barras especificadas. Si se produce
        un error durante la solicitud, se devuelve una lista vacía.
    """
    try:
        with requests.Session() as session:
            SITE_URL = f'https://www.coordinador.cl/wp-json/costo-marginal/v1/data/?fecha__gte={fecha_gte}&fecha__lte={fecha_lte}&user_key={user_key}'
            response = session.get(SITE_URL, timeout=15)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                if verbose:
                    print(f"Request successful: {response.status_code}")
            else:
                if verbose:
                    print(f"Request failed: {response.status_code}")
                return []

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out")
        return []

    except requests.exceptions.RequestException as error:
        print(f"Error: {error}")
        return []

    if not json_data:
        print('Error: empty JSON response')
        return []

    filtered_data = [n for n in json_data if n['barra'] in barras]
    return filtered_data

def get_costo_marginal_online_hora(fecha_gte, fecha_lte, barras, hora_in, user_key=USER_KEY):
    """
    Obtiene los valores del costo marginal de las barras en una hora específica.

    Args:
        fecha_gte (str): Fecha de inicio en formato "YYYY-MM-DD".
        fecha_lte (str): Fecha de fin en formato "YYYY-MM-DD".
        user_key (str): Clave de usuario para acceder a la API.
        barras (list): Lista de las barras cuyos valores de costo marginal se desean obtener.
        hora (str, optional): Hora en formato "HH:MM:SS". El valor por defecto es "17:00:00".

    Returns:
        dict: Diccionario con las barras como llaves y los valores de costo marginal como valores.
    """
    json_raw = get_json_costo_marginal_online(
        fecha_gte, fecha_lte, barras, user_key)
    if not json_raw:
        print('Error: empty JSON response')
        return {}
    
    fecha_cutoff = datetime.strptime(f'{fecha_lte} {hora_in}', '%Y-%m-%d %H:%M:%S')
    selected_data = [row for row in json_raw if datetime.strptime(row['fecha'], '%Y-%m-%d %H:%M:%S') == fecha_cutoff]
    out_dict = {row['barra']: row['cmg'] for row in selected_data}

    return out_dict

def get_central(name_central, host=API_HOST, port=API_PORT):
    '''
    Usa request API para obtener la ultima entrada de la central inputada
    
    '''
    url = f"http://{host}:{port}/central/{name_central}"
   
    try:
        response = requests.get(url , timeout= 10)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"error": "No central entries found"}
        else:
            return {"error": "Failed to retrieve central entry"}
            
    except requests.RequestException as e:
        return {"error": f"Request failed: {e}"}

def get_cmg_programados(name_central, date_in, host=API_HOST, port=API_PORT):
    """
    Retrieves the entry for the central in the 'cmg_programados' table for the given date.

    Args:
        name_central (str): The name of the central.
        date (str): The date in the format "YYYY-MM-DD".

    Returns:
        dict: A dictionary containing the central entry's information for the given date.
              If no entry is found, an error message is returned.
    """
    url = f"http://{host}:{port}/cmg_programados/{name_central}/{date_in}"

    response = requests.get(url, timeout= 10)
    response_data = json.loads(response.text)

    if response.status_code == 200:
        return response_data
    else:
        return {"error": "Failed to retrieve central entry"}

def insert_central(name_central, editor, data, host=API_HOST, port=API_PORT):

    url = f"http://{host}:{port}/central/insert/{quote(name_central)}/{quote(editor)}"
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.put(url, headers=headers, json=data, timeout=15)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"error": "No central entries found"}
        else:
            return (f"Failed to insert central entry. Response content: {response.content}")
            
    except requests.RequestException as e:
        st.write(f"Request failed: {e}")
        return {"error": f"Request failed: {e}"}

def reformat_to_iso(date_string):
    # Parse the date_string using strptime with the given format
    dt_object = datetime.strptime(date_string, '%d.%m.%y %H:%M:%S')
    
    # Return the reformatted string using strftime
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')


#############################################################
###################  Consultas    ###########################
#############################################################

with cn.establecer_session(engine) as session:
    # last row tracking_cmg
    tracking_cmg_last_row = cn.query_last_ins_tracking_coordinador(session)
    ultimo_tracking = tracking_cmg_last_row[1]
    ultimo_mod_rio = tracking_cmg_last_row[3]

    # get last entry cmg_tiempo_real , afecto_desacople, central_referencia
    central_referencia_charrua, desacople_charrua, cmg_charrua = cn.query_values_last_desacople_bool(
        session, barra_transmision='CHARRUA__220')

    if desacople_charrua:
        afecto_desacople_charrua = 'Activo'
    else:
        afecto_desacople_charrua = 'No Activo'

    central_referencia_quillota, desacople_quillota, cmg_quillota = cn.query_values_last_desacople_bool(
        session, barra_transmision='QUILLOTA__220')

    if desacople_quillota:
        afecto_desacople_quillota = 'Activo'
    else:
        afecto_desacople_quillota = 'No Activo'

    cmg_charrua = round(float(cmg_charrua) , 2)
    cmg_quillota = round(float(cmg_quillota) , 2)
    
    # consulta de datos cmg_ponderado 48 horas previas
    cmg_ponderado_96h = pd.DataFrame(cn.query_cmg_ponderado_by_time(session, unixtime, 96))
    cmg_ponderado_96h['timestamp'] = pd.to_datetime(cmg_ponderado_96h["timestamp"], format="%d.%m.%y %H:%M:%S")
    cmg_ponderado_96h.drop(['unix_time'], axis=1, inplace=True)

    # consulta estado central 
    last_row_la = cn.query_last_row_central(session, 'Los Angeles') 
    last_row_q = cn.query_last_row_central(session, 'Quillota')

    estado_generacion_la =  last_row_la[2]
    estado_generacion_q = last_row_q[2]

    costo_operacional_la = round(float(last_row_la[8]),2)
    costo_operacional_la_base = costo_operacional_la - round(float(last_row_la[10]),2)
    costo_operacional_q = round(float(last_row_q[8]),2)
    costo_operacional_q_base = costo_operacional_q - round(float(last_row_q[10]),2)

    # Consultar ultimas entradas de table Central: 
    df_central = cn.query_central_table(session, num_entries= 20)
    df_central['margen_garantia'] = df_central['margen_garantia'].astype(float)
    df_central_mod = cn.query_central_table_modifications(session, num_entries= 20)
    df_central_mod['margen_garantia'] = df_central_mod['margen_garantia'].astype(float)
    df_central_mod_co = df_central_mod.loc[:,['nombre' , 'costo_operacional','fecha_registro']]
    df_central_mod_co['fecha_registro'] = df_central_mod_co['fecha_registro'].apply(reformat_to_iso)
    # Eliminar todas las entradas que tenga mas de 96 horas.
    df_central_mod_co['fecha_registro'] = pd.to_datetime(df_central_mod_co['fecha_registro'], format='%Y-%m-%d %H:%M:%S')

    # Filter out rows where the date is more than 4 days ago
    four_days_ago = chile_datetime - timedelta(days=4)
    four_days_ago = four_days_ago.replace(tzinfo=None)    

    filtered_df = df_central_mod_co[df_central_mod_co['fecha_registro'] > four_days_ago]
    
    # Hacer merge entre df_central y cmg_ponderado
    cmg_ponderado = cmg_ponderado_96h.copy()
    cmg_ponderado['timestamp'] = cmg_ponderado['timestamp'].astype(str)
    cmg_ponderado[['fecha', 'hora']] = cmg_ponderado['timestamp'].str.split(' ', expand=True)
    cmg_ponderado['central'] = cmg_ponderado['barra_transmision'].replace({'CHARRUA__220':'Los Angeles' , 'QUILLOTA__220' : 'Quillota'})

    cmg_ponderado_la = cmg_ponderado[cmg_ponderado['barra_transmision'] == 'CHARRUA__220']
    cmg_ponderado_quillota = cmg_ponderado[cmg_ponderado['barra_transmision'] == 'QUILLOTA__220']
    row_cmg_quillota = round(float(cmg_ponderado_quillota.iloc[-1]['cmg_ponderado']),2)
    row_cmg_la = round(float(cmg_ponderado_la.iloc[-1]['cmg_ponderado']),2)


    df_central_to_merge = df_central.copy()
    df_central_to_merge[['fecha', 'hora']] = df_central_to_merge['fecha_registro'].str.split(' ', expand=True)
    # df_central_to_merge['hora'] = pd.to_datetime(df_central_to_merge['hora']).dt.floor('H').dt.time
    df_central_to_merge['hora'] = pd.to_datetime(df_central_to_merge['hora'], format='%H:%M:%S').dt.floor('H').dt.time


    # Reformat the 'fecha' column in cmg_ponderado
    cmg_ponderado['fecha'] = pd.to_datetime(cmg_ponderado['fecha'], format='%Y-%m-%d')

    # Reformat the 'fecha' column in df_central_to_merge
    df_central_to_merge['fecha'] = pd.to_datetime(df_central_to_merge['fecha'], format='%d.%m.%y')

    # Rename the 'nombre' column in df_central_to_merge to 'central'
    df_central_to_merge.rename(columns={'nombre': 'central'}, inplace=True)


    # Perform the merge on 'hora', 'fecha', and 'central' columns
    cmg_ponderado['hora'] = cmg_ponderado['hora'].astype(str)
    cmg_ponderado['fecha'] = cmg_ponderado['fecha'].astype(str)

    
    df_central_to_merge['hora'] = df_central_to_merge['hora'].astype(str)
    df_central_to_merge['fecha'] = df_central_to_merge['fecha'].astype(str)

    merged_df = pd.merge(cmg_ponderado, df_central_to_merge, on=['hora', 'fecha', 'central'], how='inner')
    merged_df.drop(['timestamp', 'fecha_registro', 'external_update', 'editor', 'barra_transmision','id'], axis=1, inplace=True)
    merged_df = merged_df[['central','costo_operacional','generando','cmg_ponderado','fecha', 'hora','margen_garantia','factor_motor','tasa_proveedor', 'porcentaje_brent', 'tasa_central', 'precio_brent','fecha_referencia_brent' ]]

  
############# Queries externas #############
cmg_programados_quillota = get_cmg_programados('Quillota' , date_in= fecha)
cmg_programados_la = get_cmg_programados('Los Angeles' , date_in= fecha)
cmg_online = get_costo_marginal_online_hora(fecha_gte=fecha, fecha_lte=fecha, barras=['Quillota' , 'Charrua'], hora_in=hora_redondeada, user_key=USER_KEY)

# check if cmg_online is empty
if not cmg_online:
    cmg_online = {'Charrua': 'Not Available', 'Quillota': 'Not Available'}
else:
    cmg_online = {key : round(cmg_online[key], 2) for key in cmg_online}

#########################################################
################### WEBSITE DESIGN ######################
#########################################################
tab1, tab2, tab3 = st.tabs(["Monitoreo", "Atributos", "Descarga Archivos"])

with tab1:
    st.header("Monitoreo")
    ################# Header #################
    col_a, col_b = st.columns((1, 2))

    with col_a:

        TRACKING_TITLE = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1.3rem;"> Última Actualización: {ultimo_tracking}</a></p>'
        st.markdown(TRACKING_TITLE, unsafe_allow_html=True)

        if CONN_STATUS:
            CONNECTION_MD = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Green; font-size:1rem;"> Connected to MySQL server: {CONN_STATUS} </a></p>'
        else:
            CONNECTION_MD = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Red; font-size:1rem;"> Connected to MySQL server: {CONN_STATUS} </a></p>'
        
        TRACKING_RIO = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1.3rem;"> Última Modificación CEN: {ultimo_mod_rio}</a></p>'

        st.markdown(TRACKING_RIO, unsafe_allow_html=True)
        
        st.markdown(CONNECTION_MD, unsafe_allow_html=True)

        st.markdown("""<hr style="height:3px; border:none;color:#333;background-color:#333;" /> """,unsafe_allow_html=True)


    ################## Body ##################

    col1, col2 = st.columns((1, 1))


    ################## DATOS Charrua - Los Angeles ##############################################
    with col1:
        COL1_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:2rem; text-align:center;"> Los Angeles </p>'

        st.markdown(COL1_TITLE, unsafe_allow_html=True)

        if estado_generacion_la:
            GENERANDO_LA = '<p style="font-family:sans-serif; font-weight: bold; color:Green; font-size:1.5rem;"> GENERANDO </p>'
        else:
            GENERANDO_LA = '<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> APAGADO </p>'

        st.markdown(GENERANDO_LA, unsafe_allow_html=True)

        col1_1, col2_1 = st.columns((1, 1))

        with col1_1:

            str_cmg_calculado_charrua= f'<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> CMg Calculado - {row_cmg_la} </p>'
            st.markdown(str_cmg_calculado_charrua, unsafe_allow_html=True)

        with col2_1:
            str_co_la= f'<p style="font-family:sans-serif; font-weight: bold; font-size:1.5rem;"> Costo Operacional - {costo_operacional_la} </p>'
            st.markdown(str_co_la, unsafe_allow_html=True)

        m1, m2  = st.columns(2)
        m1.metric(f"Costo marginal Online - {hora_redondeada}", cmg_online['Charrua'])
        if hora_redondeada_cmg_programados in cmg_programados_la:
            m2.metric(f"Costo marginal Programado - {hora_redondeada}", round(float(cmg_programados_la[hora_redondeada_cmg_programados]),2))
        else:
            st.error(f"Data for time {hora_redondeada_cmg_programados} not found in cmg_programados_la.")


        m3, m4  = st.columns(2)
        m3.metric("Central referencia", central_referencia_charrua)
        m4.metric(label="Zona en desacople", value=afecto_desacople_charrua)


    ################## DATOS Quillota ##############################################

    with col2:
        COL2_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:2rem; text-align:center;"> Quillota </p>'
        st.markdown(COL2_TITLE, unsafe_allow_html=True)

        if estado_generacion_q:
            GENERANDO_Q = '<p style="font-family:sans-serif; font-weight: bold; color:Green; font-size:1.5rem;"> GENERANDO </p>'
        else:
            GENERANDO_Q = '<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> APAGADO </p>'

        st.markdown(GENERANDO_Q, unsafe_allow_html=True)

        col1_1, col2_1 = st.columns((1, 1))

        with col1_1:

            str_cmg_calculado_quillota= f'<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> CMg Calculado - {row_cmg_quillota} </p>'
            st.markdown(str_cmg_calculado_quillota, unsafe_allow_html=True)

        with col2_1:
            str_co_quillota= f'<p style="font-family:sans-serif; font-weight: bold; font-size:1.5rem;"> Costo Operacional - {costo_operacional_q} </p>'
            st.markdown(str_co_quillota, unsafe_allow_html=True)

       
        m1, m2  = st.columns(2)
        m1.metric(f"Costo marginal Online - {hora_redondeada}", cmg_online['Quillota'])
        if hora_redondeada_cmg_programados in cmg_programados_quillota:
            m2.metric(f"Costo marginal Programado - {hora_redondeada}", round(float(cmg_programados_quillota[hora_redondeada_cmg_programados]),2))
        else:
            st.error(f"Data for time {hora_redondeada_cmg_programados} not found in cmg_programados_quillota.")

        m3, m4  = st.columns(2)
        m3.metric("Central referencia", central_referencia_quillota)
        m4.metric(label="Zona en desacople", value=afecto_desacople_quillota)


    ################## GRAFICO ##################

    with st.container():

        costo_operacional_plot_lineas_la = False
        costo_operacional_plot_lineas_quillota = False
        if filtered_df.empty:
            costo_operacional_plot_lineas_quillota = True
            costo_operacional_plot_lineas_la = True
        else:
            cmg_ponderado_96h = pd.concat([cmg_ponderado_96h, filtered_df], axis=1)
            if filtered_df[filtered_df['nombre'] == 'Quillota'].empty:
                costo_operacional_plot_lineas_quillota = True
            if filtered_df[filtered_df['nombre'] == 'Los Angeles'].empty:   
                costo_operacional_plot_lineas_la = True

        st.markdown("""<hr style="height:3px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

        col_left, col_center, col_right = st.columns([1,4,1])

        with col_center:
            # Create the Seaborn lineplot
            plt.figure(figsize=(10, 6))
            sns.lineplot(data=cmg_ponderado_96h, x="timestamp", y="cmg_ponderado", hue="barra_transmision", style="barra_transmision")

            # Set y-axis limits
            max_value = max(cmg_ponderado_96h["cmg_ponderado"].max(), costo_operacional_la, costo_operacional_q)
            margin = 2
            plt.ylim(0, max_value + margin)

            # add two horizontal lines
            if costo_operacional_plot_lineas_quillota:
                plt.axhline(y=costo_operacional_q, color='b', linestyle='--', label='CO - Quillota')
            if costo_operacional_plot_lineas_la:
                plt.axhline(y=costo_operacional_la, color='r', linestyle='--', label='CO - Los Angeles')

            # Move legend outside the plot
            plt.legend(loc="upper left", bbox_to_anchor=(1, 1))

            # Set plot title and labels
            plt.xlabel("Fecha")
            plt.ylabel("CMg")

            # Show the plot
            st.pyplot(plt.gcf())


        col1, col2 = st.columns((1, 1))

        with col1:
            st.write('Tracking CMg ponderado - DataFrame: Ultimas 5 horas')
            cmg_ponderado_96h['cmg_ponderado'] = cmg_ponderado_96h['cmg_ponderado'].round(2)
            cmg_ponderado_96h['Central'] = cmg_ponderado_96h['barra_transmision'].replace({'CHARRUA__220':'Los Angeles' , 'QUILLOTA__220' : 'Quillota'})
            cmg_ponderado_96h = cmg_ponderado_96h.rename(columns={'barra_transmision': 'Alimentador', 'timestamp' : 'Fecha y Hora', 'cmg_ponderado' : 'CMg Ponderado'})
            st.dataframe(cmg_ponderado_96h.tail(10), use_container_width=True)

        with col2:
            st.write('Ultimos movimientos Encendido/Apagado')
            st.dataframe(merged_df)

################## Modificaci'on de parametros ##################

with tab2:
    st.header("Modificación de Parametros")
    col_a, col_b = st.columns((1, 2))
   
    with col_a:
        st.markdown("($$Costo Operacional = ((Porcentaje Brent * Precio Brent) + Tasa Proveedor) * Factor Motor + Tasa Central + Margen de Garantia$$)", unsafe_allow_html=True)

        editor = st.text_input('Ingresar Nombre de persona realizando cambio de atributos', 'Cristian Valls')
        central_seleccion = st.radio("Seleccionar central a modificar:", ('Los Angeles', 'Quillota'))
        options = st.multiselect('Seleccionar atributos a modificar', ['Porcentaje Brent', 'Tasa Proveedor', 'Factor Motor', 'Tasa Central', 'Margen Garantia'], ['Margen Garantia'])

        dict_data = {}

        if 'Porcentaje Brent' in options:
            porcentaje_brent = st.number_input('Porcentaje Brent [ej: 0.14]:', value=0.0)
            dict_data['porcentaje_brent'] = porcentaje_brent
        if 'Tasa Proveedor' in options:
            tasa_proveedor = st.number_input('Tasa de proveedor [ej: 4.12]:', value=0.0)
            dict_data['tasa_proveedor'] = tasa_proveedor
        if 'Factor Motor' in options:
            factor_motor = st.number_input('Factor motor [ej: 10.12]:', value=0.0)
            dict_data['factor_motor'] = factor_motor
        if 'Tasa Central' in options:
            tasa_central = st.number_input('Tasa Central [ej: 8.8]:', value=0.0)
            dict_data['tasa_central'] = tasa_central
        if 'Margen Garantia' in options:
            margen_garantia = st.number_input('Margen Garantia [ej: -25.0]:', value=0.0)
            dict_data['margen_garantia'] = margen_garantia

        if st.button('Submit'):

            try:
                st.write((insert_central(central_seleccion, editor, dict_data, host=API_HOST, port=API_PORT)))
                st.write(f'Atributos de central {central_seleccion} modificados')

            except Exception as error:
                st.write(f'Insert error: {error}')
                st.error(f'Error occurred during insert: {error}')
    
    with col_b:

        la_co_sin_margen = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1.1rem;"> Los Angeles - Costo Operacional Basal: {costo_operacional_la_base}</a></p>'
        st.markdown(la_co_sin_margen, unsafe_allow_html=True)
        
        quillota_co_sin_margen = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1.1rem;"> Quillota - Costo Operacional Basal: {costo_operacional_q_base}</a></p>'
        st.markdown(quillota_co_sin_margen, unsafe_allow_html=True)

        st.write('Ultimos cambios de atributos')
        st.dataframe(df_central_mod)


################## Descarga de Datos ##################

with tab3:
    central_seleccion = st.radio("Seleccionar central para descargar datos", ('Los Angeles', 'Quillota'))
    if central_seleccion == 'Los Angeles':
        SELECCIONAR = 'CHARRUA__220'
    else:
        SELECCIONAR = 'QUILLOTA__220'

    date_calculate = st.date_input(
        "Seleccionar periodo CMg ponderados para descargar",
        value=datetime(2023, 6, 6).date(),
        min_value=datetime(2023, 5, 1).date(),
        max_value=datetime.now().date()
    )

    # Convert date_calculate to a Unix timestamp
    datetime_obj = datetime.combine(date_calculate, datetime.min.time())
    unix_timestamp = int(datetime_obj.timestamp())
    unix_time_delta = unixtime - unix_timestamp
    horas_delta = (unixtime - unix_timestamp) / 3600

    with cn.establecer_session(engine) as session:
        cmg_ponderado_descarga = pd.DataFrame(cn.query_cmg_ponderado_by_time(session, unixtime, horas_delta))
        cmg_tiempo_real_descarga = pd.DataFrame(cn.get_cmg_tiempo_real(session, unix_time_delta))

    @st.cache_data
    def convert_df(df):
        'seleccionar central a descargar y convertir a csv'
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        df = df[df['barra_transmision'] == SELECCIONAR]
        return df.to_csv().encode('utf-8')

    csv = convert_df(cmg_ponderado_descarga)

    st.download_button(
        label="Descargar costos marginales ponderados por hora",
        data=csv,
        file_name='costos_programados.csv',
        mime='text/csv'
    )

    csv_2 = convert_df(cmg_tiempo_real_descarga)

    st.download_button(
        label="Descargar costos marginales en tiempo real",
        data=csv_2,
        file_name='costos_programados.csv',
        mime='text/csv'
    )

################## footer ##################

with st.container():
    st.markdown("""<hr style="height:2px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

    HEADER_TITLE = '<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Blue; font-size:1rem;"> <a href="https://github.com/CFVALLS">Author: Cristian Valls </a></p>'
    st.markdown(HEADER_TITLE, unsafe_allow_html=True)
