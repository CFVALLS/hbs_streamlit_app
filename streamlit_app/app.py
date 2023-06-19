import streamlit as st
import os
import numpy as np
import pandas as pd
import json
import time
import pytz

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

#Informacion API flask
API_HOST = st.secrets["API"]["HOST"]
API_PORT = st.secrets["API"]["PORT"]


# Establecer motor de base de datos
engine, metadata = cn.establecer_engine(
    DATABASE, USER, PASSWORD, HOST, PORT, verbose=True)

CONN_STATUS = engine is not None

st.set_page_config(layout="wide")

# Get date in format YYYY-MM-DD and current hour

# Specify the timezone for Chile
chile_tz = pytz.timezone('America/Santiago')

# Create a datetime object in Chile's timezone
chile_datetime = datetime.now(chile_tz)

fecha = chile_datetime.strftime("%Y-%m-%d")
print(fecha)
hora = chile_datetime.strftime("%H:%M:%S")
print(hora)

# round hora to nearest hour
hora = hora.split(':')
hora_redondeada = f'{hora[0]}:00:00'

# get unixtime from datetime. 
unixtime = int(time.mktime(chile_datetime.timetuple()))

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
    url = f"http://{API_HOST}:{API_PORT}/central/{name_central}"
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"error": "No central entries found"}
        else:
            return {"error": "Failed to retrieve central entry"}
            
    except requests.RequestException as e:
        return {"error": f"Request failed: {e}"}


def insert_central(name_central, editor, data, host=API_HOST, port=API_PORT):
    """
    ejemplo:
        data = {
            "porcentaje_brent": 0.1411,
            "tasa_proveedor": 5.8,
            "factor_motor": 10.41,
            "tasa_central": 7.2,
            "margen_garantia": 0
        }

        response = insert_central("Los Angeles", data)

    """
    url = f"http://{API_HOST}:{API_PORT}/central/insert/{name_central}/{editor}"
    
    try:
        response = requests.put(url, json=data)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"error": "No central entries found"}
        else:
            return {"error": "Failed to insert central entry"}
            
    except requests.RequestException as e:
        return {"error": f"Request failed: {e}"}



#############################################################
###################  Consultas    ###########################
#############################################################

with cn.establecer_session(engine) as session:
    # last row tracking_cmg
    tracking_cmg_last_row = cn.query_last_ins_tracking_coordinador(session)
    ultimo_tracking = tracking_cmg_last_row[1]
    ultimo_mod_rio = tracking_cmg_last_row[3]

    # get last entry cmg_tiempo_real , afecto_desacople, central_referencia
    central_referencia_charrua, afecto_desacople_charrua, cmg_charrua = cn.query_values_last_desacople_bool(
        session, barra_transmision='CHARRUA__220')
    central_referencia_quillota, afecto_desacople_quillota, cmg_quillota = cn.query_values_last_desacople_bool(
        session, barra_transmision='QUILLOTA__220')

    cmg_charrua = round(float(cmg_charrua) , 1)
    cmg_quillota = round(float(cmg_quillota) , 1)
    
    # consulta de datos cmg_ponderado 48 horas previas
    cmg_ponderado_48h = pd.DataFrame(cn.query_cmg_ponderado_by_time(session, unixtime, 72))
    cmg_ponderado_48h['timestamp'] = pd.to_datetime(cmg_ponderado_48h["timestamp"], format="%d.%m.%y %H:%M:%S")
    cmg_ponderado_48h.drop(['unix_time'], axis=1, inplace=True)

    # consulta estado central 
    # [227, 'Los Angeles', False, Decimal('5.5000'), Decimal('0.1166'), Decimal('7.2000'), Decimal('84.640'), '2023-04', Decimal('146.649'), '11.05.23 13:50:42', Decimal('-25.000'), Decimal('10.700')]
    last_row_la = cn.query_last_row_central(session, 'Los Angeles') 
    last_row_q = cn.query_last_row_central(session, 'Quillota')

    estado_generacion_la =  last_row_la[2]
    estado_generacion_q = last_row_q[2]

    costo_operacional_la = round(float(last_row_la[8]),1)
    costo_operacional_q = round(float(last_row_q[8]),1)


    # Consultar ultimas entradas de table Central: 

    df_central = cn.query_central_table(session, num_entries= 10)
    df_central_mod = cn.cn.query_central_table_modifications(session, num_entries= 10)



############# Queries externas #############

cmg_online = get_costo_marginal_online_hora(fecha_gte=fecha, fecha_lte=fecha, barras=['Quillota' , 'Charrua'], hora_in=hora_redondeada, user_key=USER_KEY)

# check if cmg_online is empty
if not cmg_online:
    cmg_online = {'Charrua': 'Not Available', 'Quillota': 'Not Available'}


#########################################################
################### WEBSITE DESIGN ######################
#########################################################
tab1, tab2 = st.tabs(["Monitoreo", "Atributos"])

with tab1:
    st.header("Monitoreo")
    ################# Header #################
    col_a, col_b = st.columns((1, 2))

    with col_a:

        TRACKING_TITLE = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1.3rem;"> Ultima Actualizacion: {ultimo_tracking}</a></p>'
        st.markdown(TRACKING_TITLE, unsafe_allow_html=True)

        if CONN_STATUS:
            CONNECTION_MD = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Green; font-size:1rem;"> Connected to MySQL server: {CONN_STATUS} </a></p>'
        else:
            CONNECTION_MD = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Red; font-size:1rem;"> Connected to MySQL server: {CONN_STATUS} </a></p>'
        
        TRACKING_RIO = f'<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; font-size:1.3rem;"> Ultima Modificacion RIO.xls: {ultimo_mod_rio}</a></p>'

        st.markdown(TRACKING_RIO, unsafe_allow_html=True)
        
        st.markdown(CONNECTION_MD, unsafe_allow_html=True)

        st.markdown("""<hr style="height:3px; border:none;color:#333;background-color:#333;" /> """,unsafe_allow_html=True)




    ################## Body ##################

    col1, col2 = st.columns((1, 1))


    ################## DATOS Charrua - Los Angeles ##############################################
    with col1:
        COL1_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:2rem;"> Zona - Los Angeles </p>'
        st.markdown(COL1_TITLE, unsafe_allow_html=True)

        if estado_generacion_la:
            GENERANDO_LA = '<p style="font-family:sans-serif; font-weight: bold; color:Green; font-size:1.5rem;"> GENERANDO </p>'
        else:
            GENERANDO_LA = '<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> APAGADO </p>'

        st.markdown(GENERANDO_LA, unsafe_allow_html=True)

        col1_1, col2_1 = st.columns((1, 1))

        with col1_1:

            str_cmg_calculado_charrua= f'<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> CMG Calculado - {cmg_charrua} </p>'
            st.markdown(str_cmg_calculado_charrua, unsafe_allow_html=True)

        with col2_1:
            str_co_la= f'<p style="font-family:sans-serif; font-weight: bold; font-size:1.5rem;"> Costo Operacional - {costo_operacional_la} </p>'
            st.markdown(str_co_la, unsafe_allow_html=True)


        m1, m2  = st.columns(2)
        m1.metric(label="Zona en desacople", value=afecto_desacople_charrua)
        m2.metric(f"Costo marginal Online - {hora_redondeada}", cmg_online['Charrua'])
        st.metric("Central referencia", central_referencia_charrua)


    ################## DATOS Quillota ##############################################

    with col2:
        COL2_TITLE = '<p style="font-family:sans-serif; font-weight: bold; color:#050a30; font-size:2rem;"> Zona - Quillota </p>'
        st.markdown(COL2_TITLE, unsafe_allow_html=True)

        if estado_generacion_q:
            GENERANDO_Q = '<p style="font-family:sans-serif; font-weight: bold; color:Green; font-size:1.5rem;"> GENERANDO </p>'
        else:
            GENERANDO_Q = '<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> APAGADO </p>'

        st.markdown(GENERANDO_Q, unsafe_allow_html=True)

        col1_1, col2_1 = st.columns((1, 1))

        with col1_1:

            str_cmg_calculado_quillota= f'<p style="font-family:sans-serif; font-weight: bold; color:#ff2400; font-size:1.5rem;"> CMG Calculado - {cmg_quillota} </p>'
            st.markdown(str_cmg_calculado_quillota, unsafe_allow_html=True)

        with col2_1:
            str_co_quillota= f'<p style="font-family:sans-serif; font-weight: bold; font-size:1.5rem;"> Costo Operacional - {costo_operacional_q} </p>'
            st.markdown(str_co_quillota, unsafe_allow_html=True)

       
        m1, m2  = st.columns(2)
        m1.metric(label="Zona en desacople", value=afecto_desacople_quillota)
        m2.metric(f"Costo marginal Online - {hora_redondeada}", cmg_online['Quillota'])
        st.metric("Central referencia", central_referencia_quillota)


    ################## GRAFICO ##################

    with st.container():

        st.markdown("""<hr style="height:3px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

        col_left, col_center, col_right = st.columns([1,4,1])

        with col_center:

            # Create the Seaborn lineplot
            plt.figure(figsize=(10, 6))
            sns.lineplot(data=cmg_ponderado_48h, x="timestamp", y="cmg_ponderado", hue="barra_transmision", style="barra_transmision", markers=True)
            
            # add two horizontal lines
            plt.axhline(y=costo_operacional_la, color='r', linestyle='--', label='CO - Los Angeles')
            plt.axhline(y=costo_operacional_q, color='b', linestyle='--', label='CO - Quillota')

            # Manually add the legend
            plt.legend()

            # Set plot title and labels
            plt.xlabel("Timestamp")
            plt.ylabel("CMG")

            # Show the plot
            st.pyplot(plt.gcf())
    

        col1, col2 = st.columns((1, 1))

        with col1:
            st.write('Tracking cmg_ponderado - DataFrame: Ultimas 5 horas')
            st.dataframe(cmg_ponderado_48h.tail(10), use_container_width=True)

        with col2:
            st.write('Ultimos movimientos Encendido/Apagado')
            st.dataframe(df_central, use_container_width=True)




with tab2:
   st.header("Modificacion de Parametros")
   col_a, col_b = st.columns((1, 2))

   with col_a:
        st.markdown("""<hr style="height:3px; border:none;color:#333;background-color:#333;" /> """,unsafe_allow_html=True)

        st.latex(body = r''' Costo Operacional = ((Porcentaje Brent * Precio Brent) + Tasa Proveedor) * Factor Motor + Tasa Central + Margen de Garantia ''' )

        editor = st.text_input('Ingresar Nombre de persona realizando cambio de atributos', 'Cristian Valls')

        central_seleccion = st.radio("Seleccionar central a modificar: ",('Los Angeles', 'Quillota'))

        options = st.multiselect('Seleccionar atributos a modificar',['Porcentaje Brent', 'Tasa Proveedor', 'Factor Motor', 'Tasa Central', 'Margen Garantia'],['Margen Garantia'])

        dict_data = {}
        if 'Porcentaje Brent' in options:
                porcentaje_brent = st.number_input('Porcentaje Brent [ej: 0.14]:', value = 0.0)
                dict_data['porcentaje_brent'] = porcentaje_brent
        if 'Tasa Proveedor' in options:
                tasa_proveedor = st.number_input('Tasa de proveedor [ej: 4.12]:', value = 0.0)
                dict_data['tasa_proveedor'] = tasa_proveedor
        if 'Factor Motor' in options:
                factor_motor = st.number_input('Factor motor [ej: 10.12]:', value = 0.0)
                dict_data['factor_motor'] = factor_motor
        if 'Tasa Central' in options:
                tasa_central = st.number_input('Tasa Central [ej: 8.8]:', value = 0.0)
                dict_data['tasa_central'] = tasa_central
        if 'Margen Garantia' in options:
                margen_garantia = st.number_input('Margen Garantia [ej: -25.0]:', value = 0.0)
                dict_data['margen_garantia'] = margen_garantia

  
        if st.button('Submit'):
            st.write(dict_data)
            insert_central(central_seleccion, editor ,dict_data, host=API_HOST, port=API_PORT)
            st.write(f'Atributos de central {central_seleccion} modificados')
            st.session_state.disabled = True

   with col_b:
        st.write('Ultimos cambios de atributos')
        st.dataframe(df_central_mod, use_container_width=True)


################## footer ##################

with st.container():
    st.markdown("""<hr style="height:2px; border:none;color:#333;background-color:#333;" /> """,
                unsafe_allow_html=True)

    HEADER_TITLE = '<p style="font-family:sans-serif; font-weight: bold; text-align: left; vertical-align: text-bottom; color:Blue; font-size:1rem;"> <a href="https://github.com/CFVALLS">Author: Cristian Valls </a></p>'
    st.markdown(HEADER_TITLE, unsafe_allow_html=True)

