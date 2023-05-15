"""
Author: Cristian Valls
Date: 22-03-2023
Description: Script para establecer conexion con base de datos MySQL
"""

# general modules
import os
import numpy as np
import pandas as pd
from datetime import timedelta

# mysql
import mysql.connector
from mysql.connector import Error

# sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, select, MetaData, desc, asc
from sqlalchemy import Column, Integer, String, Boolean, Text, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound

#########################################################################
###################           Settings         ##########################
#########################################################################

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# log_dir = os.path.join(parent_dir, 'log')
# connection_path = os.path.join(log_dir, 'connection.log')

#########################################################################
##############                Classes                 ###################
#########################################################################

Base = declarative_base()

class TrackingCoordinador(Base):
    """
    Representa la tabla 'tracking_coordinador' en la base de datos.   
    """
    __tablename__ = 'tracking_coordinador'

    id = Column(Integer, primary_key=True)
    timestamp = Column(Text)
    archivo_rio = Column(Text)
    last_modification = Column(Text)
    rio_mod = Column(Boolean)

    def as_dict(self):
        "return a dictionary representation of the object"
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class CmgTiempoReal(Base):
    """
    Representa la tabla 'cmg_tiempo_real' en la base de datos.

    Atributos:
        id_tracking (int): Es la clave primaria de la tabla.
        barra_transmision (str): Nombre de la barra de transmisión. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        año (int): Representa el año.
        mes (int): Representa el mes.
        dia (int): Representa el día.
        hora (str): Representa la hora. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        unix_time (int): Representa el tiempo unix.
        desacople_bool (bool): Un valor booleano para el desacople.
        cmg (DECIMAL(7,3)): Representa el valor cmg con precisión decimal de 7 dígitos en total, de los cuales 3 son decimales.
        central_referencia (str): Referencia de la central. En MySQL se utiliza 'text' que puede representarse como Text en SQLAlchemy.
    """
    __tablename__ = 'cmg_tiempo_real'

    id_tracking = Column(Integer, primary_key=True)
    # tinytext puede ser representado como un String
    barra_transmision = Column(String(255))
    año = Column(Integer)
    mes = Column(Integer)
    dia = Column(Integer)
    # tinytext puede ser representado como un String
    hora = Column(String(255))
    unix_time = Column(Integer)
    desacople_bool = Column(Boolean)
    cmg = Column(DECIMAL(7, 3))
    central_referencia = Column(Text)

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class CmgPonderado(Base):
    """
    Representa la tabla 'cmg_ponderado' en la base de datos.   
    """
    __tablename__ = 'cmg_ponderado'

    id = Column(Integer, primary_key=True)
    # tinytext puede ser representado como un String
    barra_transmision = Column(String(255))
    # tinytext puede ser representado como un String
    timestamp = Column(String(255))
    unix_time = Column(Integer)
    cmg_ponderado = Column(DECIMAL(7, 4))

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class CentralTable(Base):
    """
    Representa la tabla 'central' en la base de datos.   
    """
    __tablename__ = 'central'

    id = Column(Integer, primary_key=True)
    nombre = Column(String(255))  # tinytext can be represented as a String
    generando = Column(Boolean)
    tasa_proveedor = Column(DECIMAL(7, 4))
    porcentaje_brent = Column(DECIMAL(7, 4))
    tasa_central = Column(DECIMAL(7, 4))
    precio_brent = Column(DECIMAL(7, 3))
    fecha_referencia_brent = Column(Text)
    costo_operacional = Column(DECIMAL(7, 3))
    fecha_registro = Column(Text)

    __table_args__ = {}

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]


#########################################################################
###################           functions         #########################
#########################################################################

def establecer_engine(database_in, user_in, password_in, host_in, port_in, verbose=False, pool_size=5, max_overflow=10, pool_timeout=30, pool_recycle=1800):
    """
    Establecer un motor de SQLAlchemy para conectarse a la base de datos de MySQL.

    Parametros:
        databse_in: nombre de la base de datos a la que se quiere conectar
        user_in: nombre de usuario
        password_in: contraseña del usuario
        host_in: direccion del host
        port_in: puerto de conexion
        verbose: si es True, imprime un mensaje cuando se conecta correctamente. Por defecto es False.
        pool_size: The number of connections to keep open. Default is 5.
        max_overflow: The number of connections to allow in connection pool overflow. Default is 10.
        pool_timeout: Specifies the connection timeout in seconds for the pool. Default is 30.
        pool_recycle: Specifies the maximum number of seconds between connections to the pool. Default is 1800.
    Returns:
        engine: objeto de conexion a la base de datos
        metadata: objeto de metadata para la base de datos
    """
    try:
        connection_string = f"mysql+mysqlconnector://{user_in}:{password_in}@{host_in}:{port_in}/{database_in}"
        #connection_string = f'mysql://{user_in}:{password_in}@{host_in}:{port_in}/{database_in}'

        engine = create_engine(
            connection_string,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle)

        metadata_out = MetaData(bind=engine)

        if verbose:
            print("Connection to MySQL DB successful")

        return engine, metadata_out

    except Exception as error:
            logging.error(f"Error while connecting to MySQL: {error}")
            if verbose:
                print("Coul not connect to MySQL DB successful")
            return None, None

def establecer_session(engine_in):
    """
    Crea y retorna una nueva sesión de SQLAlchemy.

    Parámetros:
    - engine_in: motor de SQLAlchemy.

    Retorna:
    - sesión de SQLAlchemy.
    """
    session_in = sessionmaker(bind=engine_in)
    return session_in()

def check_unixtime_barra_row_exists(session_in, metadata_in, unix_time, barra_transmision, tabla_in):
    """
    Verifica si existe una entrada en la tabla 'cmg_tiempo_real' u otra inputada, con un unix_time específico para una barra de transmision.

    Args:
        session_in (sqlalchemy.session): Conexión a la base de datos MySQL.
        metadata (sqlalchemy.MetaData): Objeto metadata para la base de datos.
        unix_time (int): El tiempo unix que se desea buscar en la tabla 'cmg_tiempo_real'.
        barra_transmision (str): Nombre de la barra de transmision para la que se desea obtener la información.
        tabla_in (str, optional): Nombre de la tabla en la que se desea buscar. Defaults to 'cmg_ponderado'.

    Returns:
        bool: True si existe una entrada con el unix_time especificado, False en caso contrario.
    """
    try:
        tabla = Table(tabla_in, metadata_in, autoload=True)

        # Query check whether an entry with the specified unix_time exists for barras_transmision
        query = select([tabla]).where(tabla.c.unix_time == unix_time).where(tabla.c.barra_transmision == barra_transmision)
        result = session_in.execute(query)
        exists = result.fetchone()

        # Return True if exists equals to True (the entry with the specific unix_time exists)
        if exists:
            logging.debug(f"Entry with unix_time {unix_time} found for {barra_transmision}")
            return True
        else:
            logging.debug(f"No entry with unix_time {unix_time} found for {barra_transmision}")
            return False

    except Exception as exception:
        logging.error(f"Error while checking unix_time in table: {exception}")
        return False

#########################################################################
##############            inserts con sessions              #############
#########################################################################

def insert_row_tracking_coordinador(session_in, row_in):
    """
    Agregar fila a la tabla tracking_coordinador_mod

    Parametros:
        row: fila a insertar
        session: SQLAlchemy Session object

    Return:
        ID de fila insertada
    """
    try:
        # Define a new TrackingCoordinador object
        new_tracking = TrackingCoordinador(
            timestamp=row_in[0], archivo_rio=row_in[1], last_modification=row_in[2], rio_mod=row_in[3])

        logging.info(f"Inserting row: {new_tracking.as_list()}")

        # Add the new object to the session
        session_in.add(new_tracking)

        # Return the ID of the inserted row
        return new_tracking.id

    except Exception as exception:
        # In case of error, make sure to rollback the session to avoid any inconsistent state
        session_in.rollback()
        logging.error(f"Error while inserting row into table: {exception}")
        raise

def insert_row_cmg_tiempo_real(session_in, row_in):
    """
    Insertar fila en cmg_tiempo_real
    Parametros:
        session: SQLAlchemy session object
        row: fila a insertar
    Return:
        ID de fila insertada
    """
    try:
        # Define a new CmgTiempoReal object
        new_cmg_tiempo_real = CmgTiempoReal(
            barra_transmision=row_in[0],
            año=row_in[1],
            mes=row_in[2],
            dia=row_in[3],
            hora=row_in[4],
            unix_time=row_in[5],
            desacople_bool=row_in[6],
            cmg=row_in[7],
            central_referencia=row_in[8]
        )

        # Add the new object to the session
        session_in.add(new_cmg_tiempo_real)

        # Return the ID of the inserted row
        return new_cmg_tiempo_real.id_tracking

    except Exception as exception:
        # In case of error, make sure to rollback the session to avoid any inconsistent state
        session_in.rollback()
        logging.error(f"Error while inserting row into table: {exception}")
        raise

def insert_row_cmg_ponderadon(session_in, row_in):
    """
    Insertar fila en cmg_ponderado
    Parametros:
        session: SQLAlchemy session object
        row: fila a insertar
    Return:
        ID de fila insertada
    """
    try:
        # Define a new CmgPonderado object
        new_cmg_ponderado = CmgPonderado(
            barra_transmision=row_in[0],
            timestamp=row_in[1],
            unix_time=row_in[2],
            cmg_ponderado=row_in[3]
        )
        # Add the new object to the session
        session_in.add(new_cmg_ponderado)

        # Return the ID of the inserted row
        return new_cmg_ponderado.id

    except Exception as exception:
        # In case of error, make sure to rollback the session to avoid any inconsistent state
        session_in.rollback()
        logging.error(f"Error while inserting row into table: {exception}")
        raise

def insert_or_replace_row_cmg_ponderado(session_in, barra_transmision, unix_time, cmg_ponderado):
    """
    Inserta una fila en la tabla cmg_ponderado si la fila no existe, o reemplaza una fila existente con los mismos
    valores de central y unix_time.

    Args:
        session (sqlalchemy.orm.Session): SQLAlchemy Session object.
        barra_transmision (str): Nombre de la barra de transmision para la que se desea obtener la información.
        unix_time (int): El tiempo unix que se desea buscar en la tabla 'cmg_tiempo_real'.
        cmg_ponderado (float): El cmg ponderado que se desea insertar en la tabla 'cmg_ponderado'.

    Returns:
        int: El id de la fila insertada o reemplazada.

    Raises:
        TypeError: Si alguno de los argumentos no es del tipo esperado.
        ValueError: Si alguno de los argumentos no tiene el valor esperado.
    """

    timestamp = screener.get_timestamp_from_unix_time(float(unix_time))

    try:
        try:
            # Try to get the existing row
            existing_row = session_in.query(CmgPonderado).filter_by(
                barra_transmision=barra_transmision, unix_time=unix_time).one()
            # Update the row
            existing_row.timestamp = timestamp
            existing_row.cmg_ponderado = cmg_ponderado
        except NoResultFound:
            # The row does not exist, insert a new row
            new_row = CmgPonderado(barra_transmision=barra_transmision,
                                   timestamp=timestamp, unix_time=unix_time, cmg_ponderado=cmg_ponderado)
            session_in.add(new_row)

    except TypeError as typee:
        logging.error(f"Invalid argument types: {typee}")
        session_in.rollback()

    except ValueError as valuee:
        logging.error(f"Invalid argument values: {valuee}")
        session_in.rollback()

    except Exception as othererror:
        logging.error(f"Error while inserting row into table: {othererror}")
        session_in.rollback()

#########################################################################
##############            query functions             ###################
#########################################################################

def query_last_ins_tracking_coordinador(session_in):
    """
    Retorna la última fila insertada en la tabla tracking_coordinador
    Parametros:
        session: SQLAlchemy Session object
    Return:
        row: ultima fila insertada
    """
    try:
        row_out = session_in.query(TrackingCoordinador).order_by(
            desc(TrackingCoordinador.id)).first()
        return row_out.as_list()

    except Exception as exception:
        logging.error(
            f"Error while querying last inserted row from table: {exception}")
        raise

def query_values_last_desacople_bool(session_in, barra_transmision):
    """
    Recupera la última entrada de "desacople_bool" para una "barra de transmision" específica en la tabla "cmg_tiempo_real".

    Parámetros:
    barra_transimision (str): La barra para buscar en la tabla "cmg_tiempo_real".

    Retorna:
    central_referencia (str): La referencia de la central.
    afecto_desacople (bool): Un valor booleano para el desacople.
    cmg (float): El valor cmg.  
    
    Retorna None si no se encuentra ningún resultado.
    """

    try:
        # Query to get the last "desacople_bool" entry for the specified "barra_transmision"
        result = session_in.query(CmgTiempoReal.central_referencia, CmgTiempoReal.desacople_bool, CmgTiempoReal.cmg).filter_by(
            barra_transmision=barra_transmision).order_by(desc(CmgTiempoReal.id_tracking)).first()

        if result is not None:
            central_referencia = result[0]
            afecto_desacople = result[1]
            cmg = result[2]

        return central_referencia, afecto_desacople, cmg

    except Exception as exception:
        logging.error(
            f"Error while getting last desacople_bool for {barra_transmision}: {exception}")
        return None

def query_previous_modification_tracking_coordinador(session_in):
    """
    Recupera la pen-última fila de la tabla "tracking_coordinador" con el valor "rio_mod" en True.
    Args:
        session_in (sqlalchemy.orm.session.Session): SQLAlchemy Session object.

    Returns:
        tuple o None: Retorna una tupla con los valores de la fila seleccionada, o None si no se seleccionan filas.

    """
    try:
        result = session_in.query(TrackingCoordinador).filter_by(
            rio_mod=True).order_by(desc(TrackingCoordinador.id)).limit(2)
        rows = result.all()

        if len(rows) == 2:
            return rows[1].as_list()

    except Exception as exception:
        logging.error(
            f"Error while getting previous modification: {exception}")
        return None

def evaluar_cmg_hora(session_in, unix_time_in, barra_transmision_in="CHARRUA__220"):
    """
    Obtiene el costo marginal horario promedio para una central dada en la base de datos.

    Args:
        session (sqlalchemy.orm.session.Session): SQLAlchemy Session object.
        unix_time_in (int): Tiempo UNIX en segundos.
        barra_transmision_in (str, optional): Nombre de la central a consultar. Por defecto es "CHARRUA__220".

    Returns:
        cmg_hora_out (float): Costo marginal horario promedio para la central y hora especificada.

    Raises:
        ValueError: Si el valor de `unix_time_in` es inválido.
        RuntimeError: Si ocurre un error durante la ejecución de la consulta o el cálculo del costo marginal horario.
    """

    # Definir parametros de consulta en base de datos.
    duration = 3599

    try:
        # Query to get all rows between unix_time and unix_time + duration
        rows = session_in.query(CmgTiempoReal).filter(
            CmgTiempoReal.unix_time >= unix_time_in,
            CmgTiempoReal.unix_time <= unix_time_in + duration,
            CmgTiempoReal.barra_transmision == barra_transmision_in
        ).all()

        # Calculate weighted average of cmg values
        arr_intermediario = np.array(
            [(row.unix_time - unix_time_in) for row in rows] + [duration+1])
        arr_weight = np.diff(arr_intermediario) / (duration+1)
        arr_cmg = np.array([float(row.cmg) for row in rows])

        cmg_hora_out = np.sum(np.multiply(arr_weight, arr_cmg))

        return cmg_hora_out

    except ValueError:
        logging.error("El valor de 'unix_time_in' es inválido.")
        raise

    except Exception as error:

        logging.error(
            f"Ocurrió un error durante la ejecución de la consulta o el cálculo del costo marginal horario: {error}")
        raise RuntimeError(
            "Error al ejecutar la consulta o calcular el costo marginal horario.")

def evaluar_modificacion_rio(session_in, timestamp):
    """ Evalua si hubo una modificacion posterior a el timestamp ingresado.

    Args:
        engine_in: SQLAlchemy engine object
        timestamp (str): timestamp

    Returns:
        bool: True si hubo una modificacion posterior a el timestamp ingresado, FALSE en caso contrario.
    """
    try:
        last_row = query_last_ins_tracking_coordinador(session_in)
        ultima_entrada = last_row[3]

        if ultima_entrada == timestamp:
            return False
        else:
            return True

    except Exception as exception:
        logging.error(f"Error while getting last modification: {exception}")
        return False

def query_cmg_ponderado_by_time(session_in, unixtime, delta_hours=48):
    """
    Recupera la última entrada de "cmg_ponderado" para todas las  "barra_transmision" en la tabla "cmg_ponderado" que tengan un unixtime 48 horas menor al unixtime inputado.

    Args:
        session_in (sqlalchemy.orm.session.Session): SQLAlchemy Session object.
        unixtime (int): El tiempo unix que se desea buscar en la tabla 'cmg_ponderado'.
        delta_hours (int, optional): Cantidad de horas previas a la hora de referencia. Por defecto es 48.

    Returns:
        dict: Un diccionario con las barras de transmisión como llaves y los valores de cmg_ponderado como valores.
    """
    try:
        unixtime_minus_delta = unixtime - (delta_hours * 3600)
        query = session_in.query(CmgPonderado).filter(CmgPonderado.unix_time >= unixtime_minus_delta).all()
        entries = [{ 
            'barra_transmision': row.barra_transmision,
            'timestamp': row.timestamp,
            'unix_time': row.unix_time,
            'cmg_ponderado': float(row.cmg_ponderado)
        } for row in query]
        return entries
    
    except Exception as e:
        logging.error(f"Error while getting cmg_ponderado entries: {e}")
        return None
    
def query_last_row_central(session_in, name_central):
    """
    Retrieves the last entry from the 'central' table based on the provided name.

    Args:
        session (sqlalchemy.orm.session.Session): SQLAlchemy Session object.
        name (str): The name to search for in the 'central' table.

    Returns:
        CentralTable: The last entry matching the provided name, or None if not found.
    """
    try:
        last_entry = session_in.query(CentralTable).filter_by(nombre=name_central).order_by(desc(CentralTable.id)).first()
        return last_entry.as_list() if last_entry is not None else None
    except Exception as e:
        logging.error(f"Error while getting last entry by name: {e}")
        return None

def query_central_table(session_in, num_entries=6):
    """
    Retrieves the specified number of entries from the 'central' table.

    Args:
        session_in (sqlalchemy.orm.session.Session): SQLAlchemy Session object.
        num_entries (int): Number of entries to retrieve.

    Returns:
        pd.DataFrame: DataFrame containing the retrieved entries.
    """
    try:
        query = session_in.query(CentralTable).order_by(desc(CentralTable.id)).limit(num_entries)
        entries = query.all()
        if entries:
            data = [entry.as_list() for entry in entries]
            df = pd.DataFrame(data, columns=CentralTable.__table__.columns.keys())
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        logging.error(f"Error while retrieving entries from 'central' table: {e}")
        return None


##################################################################################
##################### FUNCION PARA sintetizar subrutinas #########################
##################################################################################

def process_and_insert_data(barra_transimsion_in, timestamp_rio_mod, df_tco, df_fp, df_rio, session_in, bool_desacople=False):
    """
    procesa dataframes importados e inserta datos en base de datos.
    """
    try:
        # 6.4) Obtain cmg_central
        flt_cmg_corregido, central_ref = screener.get_cmg_corregido(
            timestamp_in=timestamp_rio_mod, df_tco_in=df_tco, df_fp_in=df_fp, df_rio_in=df_rio, central_in=barra_transimsion_in)

        int_year, int_month, int_day, str_time, int_unix_time = screener.timestamp_decomp(
            timestamp_rio_mod)

        # 6.5) Insert records into the database

        insert_row_cmg_tiempo_real(session_in, row_in=[
                                   barra_transimsion_in, int_year, int_month, int_day, str_time, int_unix_time, bool_desacople, flt_cmg_corregido, central_ref])

    except Exception as exception:
        print(exception)

def registro_inicio_hora(auth, path, session_in, barra_transmision, timestamp_current_hour, metadata):
    """
    Registra el inicio de hora en la tabla de seguimiento de cmg_ponderado.

    Args:
        AUTH (tuple): Credenciales de autenticación para acceder al servidor de coordinación.
        PATH (str): Ruta en el servidor de coordinación donde se almacenan los archivos necesarios.
        session_in: sqlalchemy session object.
        barra_transmision (list of str): Lista con los códigos de barra de transmisión.
        timestamp_current_hour (int): Timestamp del inicio de hora.

    Returns:
        None.

    Raises:
        Exception: Si no se pudo descargar o importar algún archivo necesario para el cálculo del CMG corregido.
    """
    # redondear el timestamp hacia abajo al inicio de la hora
    try:
        datestamp = screener.get_date()

        timestamp_current_hour_rd = screener.round_down_timestamp(
            timestamp_current_hour)

        int_year, int_month, int_day, str_time, unixtime_current_hour = screener.timestamp_decomp(
            timestamp_current_hour_rd)
    except Exception as exception:
        logging.error(
            f"Error al redondear el timestamp hacia abajo al inicio de la hora. error: {exception}")
        raise
    
    for barra in barra_transmision:
        # verificar si ya se encuentra la hora actual en la tabla cmg_ponderado para esta barra_transmision
        if not check_unixtime_barra_row_exists(session_in=session_in, metadata_in=metadata,unix_time=unixtime_current_hour, barra_transmision=barra, tabla_in="cmg_ponderado"):

            try:
                # descargar el archivo HTML de coordinación
                html_coordinador, _ = screener.get_html_coordinador(auth, path, timestamp_in=datestamp)

            except Exception as exception:
                html_coordinador = None
                logging.error(f"Error al descargar el archivo de coordinación para la fecha {datestamp}. error: {exception}")
                raise

            try:
                # evaluar el archivo HTML para verificar la disponibilidad del archivo RIO del día actual
                if html_coordinador is not None:
                    disponible_rio_hoy, str_rio_filename, _ = screener.eval_html_coordinador(
                        html_in=html_coordinador)
                else:
                    disponible_rio_hoy = False

                # obtener la última entrada para la barra_transmision actual
                ref_central, bool_desacople, cmg_pasado = query_values_last_desacople_bool(
                    session_in, barra)

                if not disponible_rio_hoy:
                    # si el archivo RIO del día actual no está disponible, copiar cmg_pasado como el valor actual de CMG
                    row_cmg_tiempo_real = (barra, int_year, int_month, int_day, str_time,
                                            unixtime_current_hour, bool_desacople, cmg_pasado, ref_central)

                    insert_row_cmg_tiempo_real(
                        session_in, row_cmg_tiempo_real)

                else:
                    # descargar e importar los archivos necesarios para el cálculo de CMG corregido
                    df_rio, df_tco, df_fp, arr_temp_files = screener.download_and_import_files(
                        str_rio_filename)

                    # obtener el valor corregido de CMG y la central de referencia
                    flt_cmg_corregido, central_ref = screener.get_cmg_corregido(
                        timestamp_in=timestamp_current_hour, df_tco_in=df_tco, df_fp_in=df_fp, df_rio_in=df_rio, central_ref=ref_central, central_in=barra)

                    # insertar la entrada en la tabla cmg_tiempo_real
                    row_tracking_cmg = (barra, int_year, int_month, int_day, str_time,
                                        unixtime_current_hour, bool_desacople, flt_cmg_corregido, central_ref)
                    insert_row_cmg_tiempo_real(session_in, row_tracking_cmg)

                    # eliminar los archivos temporales si es necesario
                    if arr_temp_files is not None:
                        for file in arr_temp_files:
                            screener.delete_temp_file(file_name=file)

            except Exception as exception:
                logging.error(f" Error en eval_html_coordinador. error: {exception}")
                raise
        else:
            # No es necesario agregar una nueva entrada en la tabla cmg_tiempo_real
            pass


if __name__ == "__main__":

    print('helo')
    # host = os.environ.get("MYSQL_HOST")
    # database = os.environ.get("MYSQL_DATABASE")
    # user = os.environ.get("MYSQL_USER")
    # password = os.environ.get("MYSQL_USER_PASSWORD")
    # port = os.environ.get("MYSQL_PORT")

    # cnx, metadata = establecer_engine(
    #     database, user, password, host, port, verbose=True)

    # # open a session to use the connection
    # with establecer_session(cnx) as session:

    #     # # insert_row_cmg_tiempo_real_session(session, row_in= ['QUILLOTA__220', 2021, 1, 1, '24.04.23 10:00:00', 1682344800, 0, 190.1000, 'QUILLOTA__220'])
    #     # row_in = ['24.04.23 10:15:40' , 'RIO230424.xls', '24.04.23 10:02:35',  0]
    #     # insert_row_tracking_coordinador_session(session, row_in )

    #     print(evaluar_modificacion_rio(session, '24.04.23 10:02:35'))

    #     session.commit()

    # cnx.dispose()
