import streamlit as st
import os
import numpy as np
import pandas as pd
import json
import time
from datetime import datetime
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

# Establecer motor de base de datos
engine, metadata = cn.establecer_engine(
    DATABASE, USER, PASSWORD, HOST, PORT, verbose=True)

CONN_STATUS = engine is not None

st.set_page_config(layout="wide")

# Get date in format YYYY-MM-DD and current hour
now = datetime.now()
fecha = now.strftime("%Y-%m-%d")
print(fecha)
hora = now.strftime("%H:%M:%S")

# round hora to nearest hour
hora = hora.split(':')
hora_redondeada = f'{hora[0]}:00:00'

# get unixtime from datetime. 
unixtime = int(time.mktime(now.timetuple()))

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

#############################################################
###################  Consultas    ###########################
#############################################################

with cn.establecer_session(engine) as session:
    # last row tracking_cmg
    tracking_cmg_last_row = cn.query_last_ins_tracking_coordinador(session)
    ultimo_tracking = tracking_cmg_last_row[1]

    # get last entry cmg_tiempo_real , afecto_desacople, central_referencia
    central_referencia_charrua, afecto_desacople_charrua, cmg_charrua = cn.query_values_last_desacople_bool(
        session, barra_transmision='CHARRUA__220')
    central_referencia_quillota, afecto_desacople_quillota, cmg_quillota = cn.query_values_last_desacople_bool(
        session, barra_transmision='QUILLOTA__220')
    
    # consulta de datos cmg_ponderado 48 horas previas
    cmg_ponderado_48h = pd.DataFrame(cn.query_cmg_ponderado_by_time(session, unixtime, 72))
    cmg_ponderado_48h['timestamp'] = pd.to_datetime(cmg_ponderado_48h["timestamp"], format="%d.%m.%y %H:%M:%S")

############# Queries externas #############
cmg_online = get_costo_marginal_online_hora(fecha_gte=fecha, fecha_lte=fecha, barras=['Quillota' , 'Charrua'], hora_in=hora_redondeada, user_key=USER_KEY)

# check if cmg_online is empty
if not cmg_online:
    cmg_online = {'Charrua': 'Not Available', 'Quillota': 'Not Available'}


#########################################################
################### WEBSITE DESIGN ######################
#########################################################

################## TITLE ##################

col1, col2 = st.columns((1, 1))

with col1:
    # CHARRUA
    COL1_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:2rem;"> Zona - Los Angeles </p>'
    st.markdown(COL1_TITLE, unsafe_allow_html=True)
    st.markdown("""<hr style="height:5px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

    st.metric(label="Zona en desacople", value=afecto_desacople_charrua)
    st.metric("Costo marginal calculado", float(cmg_charrua))
    st.metric(f"Costo marginal Online - {hora_redondeada}", cmg_online['Charrua'])
    st.metric("Central referencia", central_referencia_charrua)


    GRAFICO_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:1rem;"> CMG grafico </p>'
    st.markdown(GRAFICO_TITLE, unsafe_allow_html=True)

with col2:
    COL2_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:2rem;"> Zona - Quillota </p>'
    st.markdown(COL2_TITLE, unsafe_allow_html=True)
    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

    st.metric(label="Zona en desacople", value=afecto_desacople_quillota)
    st.metric("Costo marginal calculado", float(cmg_quillota))
    st.metric(f"Costo marginal Online - {hora_redondeada}", cmg_online['Quillota'])
    st.metric("Central referencia", central_referencia_quillota)

################## GRAFICO ##################

with st.container():
    
    st.markdown("""<hr style="height:2px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.write('Tracking cmg_ponderado - DataFrame:')
        st.dataframe(cmg_ponderado_48h.tail(10), use_container_width=True)

    with col2:

        # Create the Seaborn lineplot
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=cmg_ponderado_48h, x="timestamp", y="cmg_ponderado", hue="barra_transmision", style="barra_transmision", markers=True)

        # Set plot title and labels
        plt.title("CMG vs Timestamp")
        plt.xlabel("Timestamp")
        plt.ylabel("CMG")

        # Show the plot
        st.pyplot(plt.gcf())


################## footer ##################

with st.container():
    st.markdown("""<hr style="height:2px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

    HEADER_TITLE = '<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Blue; font-size:1rem;"> <a href="https://github.com/CFVALLS">Author: Cristian Valls </a></p>'
    st.markdown(HEADER_TITLE, unsafe_allow_html=True)

    STATUS_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:1rem;"> Datos Actuales </p>'
    st.markdown(STATUS_TITLE, unsafe_allow_html=True)

    if CONN_STATUS:
        CONNECTION_MD = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Green; font-size:1rem;"> Connected to MySQL server: {CONN_STATUS} </a></p>'
    else:
        CONNECTION_MD = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Red; font-size:1rem;"> Connected to MySQL server: {CONN_STATUS} </a></p>'
    st.markdown(CONNECTION_MD, unsafe_allow_html=True)

    TRACKING_TITLE = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1rem;"> Ultima consulta: {ultimo_tracking} </a></p>'
    st.markdown(TRACKING_TITLE, unsafe_allow_html=True)
    st.markdown("""<hr style="height:2px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)
