import pandas as pd
import mysql.connector
from mysql.connector import Error
import os # Para manejar rutas de archivos si es necesario

# --- Configuración de la Base de Datos ---
# *** Importante: No dejes contraseñas aquí en código de producción. ***
# Considera usar variables de entorno o un archivo de configuración seguro.
DB_CONFIG = {
    'host': 'localhost', # O la IP de tu servidor MySQL
    'database': 'GPON',
    'user': 'root', # Generalmente 'root' o el usuario que creaste
    'password': 'Rielecom2-' # La contraseña que usas para conectar a MySQL
}

# --- Rutas de los Archivos Excel ---
# Asegúrate de que estos archivos estén en la misma carpeta que tu script
# o proporciona la ruta completa
EXCEL_FILE_V2 = 'Reporte GPON V2 para CRA_20250422.xlsx'
EXCEL_FILE_V1 = 'Reporte GPON V1 para CRA_20250422.xlsx'

# --- Función para conectar a la Base de Datos ---
def create_db_connection(host, database, user, password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        if connection.is_connected():
            print(f"Conexión exitosa a la base de datos '{database}'")
        return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# --- Función para leer una hoja específica de un archivo Excel ---
def read_excel_sheet(file_path, sheet_name):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"Lectura exitosa de la hoja '{sheet_name}' del archivo '{file_path}'. Filas encontradas: {len(df)}")
        # Opcional: Limpiar nombres de columnas (ej. quitar espacios, convertir a minúsculas)
        # df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        return df
    except FileNotFoundError:
        print(f"Error: El archivo '{file_path}' no encontrado.")
        return None
    except Exception as e:
        print(f"Error al leer la hoja '{sheet_name}' del archivo '{file_path}': {e}")
        return None
    

    # --- Función para importar datos a la tabla Gabinete ---
def insert_gabinetes(connection, cursor, dataframe):
    print("Iniciando importación de Gabinetes...")
    sql = """
    INSERT IGNORE INTO Gabinete (  -- <--- ¡Aquí está el nombre de la tabla!
        id_gabinete, tipo_gabinete, nombre_gabinete, lado_gabinete,
        direccion, comuna, total_puertas_splitters, puertas_working_deactivated,
        puertas_en_analisis, puertas_en_a_operativo, puertas_en_reserva,
        puertas_splitters_libres, puertas_splitters_en_servicio, lon, lat
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # %s es un placeholder para el conector de MySQL en Python

    rows_to_insert = []
    

    # Itera sobre las filas del DataFrame de pandas
    for index, row in dataframe.iterrows():
        # Mapea las columnas del DataFrame a la lista de valores para la sentencia SQL
        # Asegúrate del orden correcto según la sentencia SQL arriba
        rows_to_insert.append((
            row['ID_GABINETE'],
            row['TIPO_GABINETE'],
            row['NOMBRE_GABINETE'],
            row['LADO_GABINETE'],
            row['DIRECCION_GABINETE'],
            row['COMUNA_GABINETE'],
            row['TOTAL_PUERTAS_SPLITTERS'],
            row['PUERTAS_WORKING_DEACTIVATED'],
            row['PUERTAS_EN_ANALISIS'],
            row['PUERTAS_EN_A_OPERATIVO'],
            row['PUERTAS_EN_RESERVA'],
            row['PUERTAS_SPLITTERS_LIBRES'],
            row['PUERTAS_SPLITTERS_EN_SERVICIO'],
            row['LON'],
            row['LAT']
        ))

    try:
        # Ejecuta la inserción de múltiples filas
        cursor.executemany(sql, rows_to_insert)
        connection.commit() # Confirma la transacción (guarda los cambios)
        print(f"Importación de {len(rows_to_insert)} Gabinetes completada con éxito.")
    except Error as e:
        connection.rollback() # Deshace la transacción en caso de error
        print(f"Error al importar Gabinetes: {e}")
    except Exception as e:
        connection.rollback()
        print(f"Error inesperado al procesar Gabinetes: {e}")

# --- Funciones de Inserción (las crearemos una por una) ---
# Desde hoja 'Splitters' del Archivo V2
# Esta función asume que la tabla Gabinete ya está populada (requiere id_gabinete para el FK)
# --- Función para importar datos a la tabla Splitter_Gabinete_V2 (VERSION FINAL AJUSTADA) ---
# Desde hoja 'Splitters' del Archivo V2
# Esta función mapea SOLO las 18 columnas que realmente se encontraron en el DataFrame
# Basado en la salida 'Columnas disponibles en DataFrame'
def insert_splitter_gabinete_v2(connection, cursor, dataframe):
    print("Iniciando importación de Splitters Gabinete (V2)...")
    # Lista las 18 columnas que realmente existen en el DataFrame (y en la DB después de DROP)
    # Asegúrate que esta lista coincida con el ALTER TABLE DROP que ejecutaste
    sql = """
    INSERT IGNORE INTO Splitter_Gabinete_V2 (
        id_splitter, nombre_splitter, id_gabinete, lado_gabinete, comuna_splitter,
        total_puertas, puertas_working_deactivated, puertas_en_analisis,
        puertas_en_a_operativo, puertas_en_reserva, puertas_libres, puertas_en_servicio,
        olt, pta_olt, id_port_odf, distancia_a_olt, id_lugar_hub_de_olt,
        nombre_lugar_hub
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 18 columnas en SQL, 18 placeholders %s.

    rows_to_insert = []
    # Los nombres de las columnas en row['...'] DEBEN coincidir con los que Pandas encontró en el Excel
    # Según la salida 'Columnas disponibles en DataFrame', son estos 18 nombres EXACTOS.

    for index, row in dataframe.iterrows():
        try:
            rows_to_insert.append((
                row['ID_SPLITTER'],               # 1
                row['NOMBRE_SPLITTER'],           # 2
                row['ID_GABINETE'],              # 3
                row['LADO_GABINETE'],            # 4
                row['COMUNA_SPLITTER'],          # 5
                row['TOTAL_PUERTAS'],            # 6
                row['PUERTAS_WORKING_DEACTIVATED'], # 7
                row['PUERTAS_EN_ANALISIS'],      # 8
                row['PUERTAS_EN_A_OPERATIVO'],   # 9
                row['PUERTAS_EN_RESERVA'],       # 10
                row['PUERTAS_LIBRES'],           # 11
                row['PUERTAS_EN_SERVICIO'],      # 12
                row['OLT'],                      # 13
                row['PTA_OLT'],                  # 14
                row['ID_PORT_ODF'],              # 15
                row['DISTANCIA_A_OLT'],          # 16
                row['ID_LUGAR_HUB_DE_OLT'],      # 17
                row['NOMBRE_LUGAR_HUB']          # 18
            ))
        except KeyError as e:
            # Este bloque de error solo debería activarse si falta alguna de las 18 columnas listadas arriba en row[...]
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Splitters Gabinete (V2).")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Splitters' (V2).")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist())
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Splitter Gabinete (V2): {e}")
             print("Datos de la fila:", row.to_dict())
             return # Detiene la importación


    try:
        # DEBUG PRINTS (Puedes quitarlos si quieres, pero son útiles para verificar)
        print("Sentencia SQL para Splitter Gabinete V2:")
        print(sql)
        print("\nPrimeras 5 filas de datos a insertar (Splitter Gabinete V2):")
        for i, row_data in enumerate(rows_to_insert[:5]):
            print(f"Fila {i}: {row_data}")
        print("-" * 20)
        # FIN DEBUG PRINTS

        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Splitters Gabinete (V2) completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar Splitters Gabinete (V2): {e}")
    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar Splitters Gabinete (V2): {e}")
# --- Función para importar datos a la tabla Doble_Conector ---
def insert_doble_conectores(connection, cursor, dataframe):
    print("Iniciando importación de Doble Conectores...")
    # Esta sentencia SQL lista las 23 columnas que existen en la tabla Doble_Conector
    # Y tiene exactamente 23 marcadores de posición (%s)
    sql = """
    INSERT IGNORE INTO Doble_Conector (
        id_dc, id_gabinete, nombre_dc, direccion, comuna, lon, lat,
        puertas_totales_entrada, puertas_entrada_libres_buenos, puertas_entrada_en_servicio,
        id_edificio, id_xygo, piso,
        puertas_entrada_habilitadas, puertas_working_deactivated, puertas_en_analisis,
        puertas_en_a_operativo, puertas_en_reserva, cuenta_entrada,
        cuenta_entrada_2, tipo_gabinete_2, ID_GABINETE_2, NOMBRE_GABINETE_2 -- <--- Columnas _2 añadidas
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Usamos INSERT IGNORE para saltar duplicados en la clave primaria (id_dc)

    rows_to_insert = []


    # Itera sobre las filas del DataFrame de pandas
    for index, row in dataframe.iterrows():

        try:
            rows_to_insert.append((
                row['ID_DC'],                               # 1 -> id_dc (PK)
                row['ID_GABINETE'],                         # 2 -> id_gabinete (FK a Gabinete)
                row['NOMBRE_DC'],                           # 3 -> nombre_dc
                row['DIRECCION'],                           # 4 -> direccion
                row['COMUNA'],                              # 5 -> comuna
                row['LON'],                                 # 6 -> lon (Excel tiene LON)
                row['LAN'],                                 # 7 -> lat (Excel tiene LAN)
                row['PUERTAS_TOTALES_ENTRADA'],             # 8 -> puertas_totales_entrada
                row['PUERTAS_ENTRADA_LIBRES_BUENOS'],       # 9 -> puertas_entrada_libres_buenos
                row['PUERTAS_ENTRADA_EN_SERVICIO'],         # 10 -> puertas_entrada_en_servicio
                row['ID_EDIFICIO'],                         # 11 -> id_edificio
                row['ID_XYGO'],                             # 12 -> id_xygo         <--- Elemento añadido
                row['PISO'],                                # 13 -> piso          <--- Elemento añadido
                row['PUERTAS_ENTRADA_HABILITADAS'],         # 14 -> puertas_entrada_habilitadas <--- Elemento añadido
                row['PUERTAS_WORKING_DEACTIVATED'],         # 15 -> puertas_working_deactivated <--- Elemento añadido
                row['PUERTAS_EN_ANALISIS'],                 # 16 -> puertas_en_analisis     <--- Elemento añadido
                row['PUERTAS_EN_A_OPERATIVO'],              # 17 -> puertas_en_a_operativo  <--- Elemento añadido
                row['PUERTAS_EN_RESERVA'],                  # 18 -> puertas_en_reserva    <--- Elemento añadido
                row['CUENTA_ENTRADA'],                       # 19 -> cuenta_entrada
                row['CUENTA_ENTRADA_2'],                     # 20 -> cuenta_entrada_2
                row['TIPO_GABINETE_2'],                      # 21 -> tipo_gabinete_2
                row['ID_GABINETE_2'],                        # 22 -> ID_GABINETE_2
                row['NOMBRE_GABINETE_2']                     # 23 -> NOMBRE_GABINETE_2
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Doble Conectores.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Doble_Conectores'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Doble Conector: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación

    try:
       
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Doble Conectores completada con éxito.")

    except Error as e:
        connection.rollback() # Deshace la transacción en caso de error
        print(f"Error al importar Doble Conectores: {e}")
        

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado al procesar Doble Conectores: {e}")


        # --- Función para importar datos a la tabla Splitter_Primario ---
def insert_splitter_primarios(connection, cursor, dataframe):
    print("Iniciando importación de Splitters Primarios...")
    # Lista las 20 columnas que debe tener la tabla Splitter_Primario
    sql = """
    INSERT IGNORE INTO Splitter_Primario (
        id_sp, id_gabinete, nombre_splitter, puertas_totales, puertas_working_deactivated,
        puertas_en_analisis, puertas_en_a_operativo, puertas_en_reserva, puertas_libres,
        puertas_en_servicio, id_contenedor, tipo_contenedor, direccion_contenedor,
        piso_contenedor, comuna_contenedor, id_puerta_odf, longitud_principal,
        nombre_hub, nombre_olt, puerta_olt
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 20 columnas, 20 placeholders %s

    rows_to_insert = []
    # Asegúrate de que los nombres de las columnas en el DataFrame (row['...'])
    # coincidan exactamente con los nombres en tu hoja de Excel (Splitters_Primarios)

    for index, row in dataframe.iterrows():
        # Implementar la lógica condicional para id_gabinete
        id_gabinete_val = None # Por defecto, es NULL en la base de datos
        try:
            # Verifica si las columnas de contenedor existen y no son nulas en esta fila
            if pd.notna(row['TIPO_CONTENEDOR']) and pd.notna(row['ID_CONTENEDOR']):
                 # Usa str() para manejar posibles tipos mixtos o NaN antes de lower()
                 if str(row['TIPO_CONTENEDOR']).strip().lower() == 'gabinete':
                     # Si el tipo es 'Gabinete', usa el ID del contenedor para el FK de Gabinete
                     id_gabinete_val = row['ID_CONTENEDOR']
                 # Puedes añadir otras condiciones elif para otros tipos de contenedores si los modelaste
        except KeyError as e:
             # Si TIPO_CONTENEDOR o ID_CONTENEDOR no existen en el DataFrame, maneja el error
             print(f"Advertencia: Columna de contenedor faltante ('{e}') en la fila {index} de Splitters Primarios. id_gabinete será NULL para esta fila.")
             # Continuamos, id_gabinete_val ya es None


        try:
            rows_to_insert.append((
                row['ID_SPLITTER'],              # -> id_sp (PK)
                id_gabinete_val,                 # -> id_gabinete (FK, condicional)
                row['NOMBRE_SPLITTER'],          # -> nombre_splitter
                row['PUERTAS_TOTALES'],          # -> puertas_totales
                row['PUERTAS_WORKING_DEACTIVATED'], # -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],      # -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],   # -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],       # -> puertas_en_reserva
                row['PUERTAS_LIBRES'],           # -> puertas_libres
                row['PUERTAS_EN_SERVICIO'],      # -> puertas_en_servicio
                row['ID_CONTENEDOR'],            # -> id_contenedor
                row['TIPO_CONTENEDOR'],          # -> tipo_contenedor
                row['DIRECCION_CONTENEDOR'],     # -> direccion_contenedor
                row['PISO_CONTENEDOR'],          # -> piso_contenedor
                row['COMUNA_CONTENEDOR'],        # -> comuna_contenedor
                row['ID_PUERTA_ODF'],            # -> id_puerta_odf
                row['LONGITUD_PRINCIPAL'],       # -> longitud_principal (Asumimos tipo numérico/decimal)
                row['NOMBRE_HUB'],               # -> nombre_hub
                row['NOMBRE_OLT'],               # -> nombre_olt
                row['PUERTA_OLT']                # -> puerta_olt
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Splitters Primarios.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Splitters_Primarios'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Splitter Primario: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Splitters Primarios completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar Splitters Primarios: {e}")
       

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar Splitters Primarios: {e}")


        # --- Función para importar datos a la tabla Caja_Doble_Conector ---
def insert_caja_doble_conectores(connection, cursor, dataframe):
    print("Iniciando importación de Cajas de Doble Conector...")
    # Lista las 21 columnas que debe tener la tabla Caja_Doble_Conector
    sql = """
    INSERT IGNORE INTO Caja_Doble_Conector (
        id_caja_dc, id_dc, nombre_caja, fijacion_caja, piso_caja, cuenta_asignada_caja,
        puertas_totales_caja, puertas_habilitadas_caja, puertas_working_deactivated,
        puertas_en_analisis, puertas_en_a_operativo, puertas_en_reserva,
        puertas_entrada_libres_buenos, puertas_en_servicio_caja, nro_dirs_asociadas,
        id_xygo_direccion_edificio, direccion_edificio, comuna_edificio,
        id_smallworld_edificio, lon, lat
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


    rows_to_insert = []
    

    for index, row in dataframe.iterrows():
        try:
            rows_to_insert.append((
                row['ID_SMALLWORLD_CAJA'],             # 1 -> id_caja_dc (PK)
                row['ID_SMALLWORLD_DOBLE_CONECTOR'],   # 2 -> id_dc (FK a Doble_Conector)
                row['NOMBRE_CAJA'],                    # 3 -> nombre_caja
                row['FIJACION_CAJA'],                  # 4 -> fijacion_caja
                row['PISO_CAJA'],                      # 5 -> piso_caja
                row['CUENTA_ASIGNADA_CAJA'],           # 6 -> cuenta_asignada_caja
                row['PUERTAS_TOTALES_CAJA'],           # 7 -> puertas_totales_caja
                row['PUERTAS_HABILITADAS_CAJA'],       # 8 -> puertas_habilitadas_caja
                row['PUERTAS_WORKING_DEACTIVATED'],    # 9 -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],            # 10 -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],         # 11 -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],             # 12 -> puertas_en_reserva
                row['PUERTAS_ENTRADA_LIBRES_BUENOS'],  # 13 -> puertas_entrada_libres_buenos
                row['PUERTAS_EN_SERVICIO_CAJA'],       # 14 -> puertas_en_servicio_caja
                row['NRO_DIRS_ASOCIADAS'],             # 15 -> nro_dirs_asociadas
                row['ID_XYGO_DIRECCION_EDIFICIO'],     # 16 -> id_xygo_direccion_edificio
                row['DIRECCION_EDIFICIO'],             # 17 -> direccion_edificio
                row['COMUNA_EDIFICIO'],                # 18 -> comuna_edificio
                row['ID_SMALLWORLD_EDIFICIO'],         # 19 -> id_smallworld_edificio
                row['LON'],                            # 20 -> lon
                row['LAN']                             # 21 -> lat
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Cajas de Doble Conector.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Cajas_de_Doble_Conector'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Caja de Doble Conector: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Cajas de Doble Conector completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar Cajas de Doble Conector: {e}")
       

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar Cajas de Doble Conector: {e}")


        # --- Función para importar datos a la tabla Caja_Doble_Conector ---
def insert_caja_doble_conectores(connection, cursor, dataframe):
    print("Iniciando importación de Cajas de Doble Conector...")
    # Lista las 21 columnas que debe tener la tabla Caja_Doble_Conector
    sql = """
    INSERT IGNORE INTO Caja_Doble_Conector (
        id_caja_dc, id_dc, nombre_caja, fijacion_caja, piso_caja, cuenta_asignada_caja,
        puertas_totales_caja, puertas_habilitadas_caja, puertas_working_deactivated,
        puertas_en_analisis, puertas_en_a_operativo, puertas_en_reserva,
        puertas_entrada_libres_buenos, puertas_en_servicio_caja, nro_dirs_asociadas,
        id_xygo_direccion_edificio, direccion_edificio, comuna_edificio,
        id_smallworld_edificio, lon, lat
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_to_insert = []
    

    for index, row in dataframe.iterrows():
        try:
            rows_to_insert.append((
                row['ID_SMALLWORLD_CAJA'],             # 1 -> id_caja_dc (PK)
                row['ID_SMALLWORLD_DOBLE_CONECTOR'],   # 2 -> id_dc (FK a Doble_Conector)
                row['NOMBRE_CAJA'],                    # 3 -> nombre_caja
                row['FIJACION_CAJA'],                  # 4 -> fijacion_caja
                row['PISO_CAJA'],                      # 5 -> piso_caja
                row['CUENTA_ASIGNADA_CAJA'],           # 6 -> cuenta_asignada_caja
                row['PUERTAS_TOTALES_CAJA'],           # 7 -> puertas_totales_caja
                row['PUERTAS_HABILITADAS_CAJA'],       # 8 -> puertas_habilitadas_caja
                row['PUERTAS_WORKING_DEACTIVATED'],    # 9 -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],            # 10 -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],         # 11 -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],             # 12 -> puertas_en_reserva
                row['PUERTAS_ENTRADA_LIBRES_BUENOS'],  # 13 -> puertas_entrada_libres_buenos
                row['PUERTAS_EN_SERVICIO_CAJA'],       # 14 -> puertas_en_servicio_caja
                row['NRO_DIRS_ASOCIADAS'],             # 15 -> nro_dirs_asociadas
                row['ID_XYGO_DIRECCION_EDIFICIO'],     # 16 -> id_xygo_direccion_edificio
                row['DIRECCION_EDIFICIO'],             # 17 -> direccion_edificio
                row['COMUNA_EDIFICIO'],                # 18 -> comuna_edificio
                row['ID_SMALLWORLD_EDIFICIO'],         # 19 -> id_smallworld_edificio
                row['LON'],                            # 20 -> lon
                row['LAN']                             # 21 -> lat
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Cajas de Doble Conector.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Cajas_de_Doble_Conector'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Caja de Doble Conector: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Cajas de Doble Conector completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar Cajas de Doble Conector: {e}")
       

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar Cajas de Doble Conector: {e}")


        # --- Función para importar datos a la tabla LDD ---
def insert_ldds(connection, cursor, dataframe):
    print("Iniciando importación de LDDs...")
    # Lista las 21 columnas que debe tener la tabla LDD
    sql = """
    INSERT IGNORE INTO LDD (
        id_ldd, id_gabinete, nombre_caja, fijacion_caja, piso_caja, id_xygo_direccion_caja,
        direccion_caja, comuna_caja, cuenta_asignada_caja, puertas_totales_caja,
        puertas_habilitadas_caja, puertas_working_deactivated, puertas_en_analisis,
        puertas_en_a_operativo, puertas_en_reserva, puertas_libres_y_buenas,
        puertas_en_servicio_caja, nro_dirs_asociadas_caja, id_smallworld_edificio,
        lon, lat
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 21 columnas, 21 placeholders %s
    # Usamos ID_SMALLWORLD_CAJA del Excel para id_ldd (PK)
    # Usamos ID_SMALLWORLD_GABINETE del Excel para id_gabinete (FK)
    # Usamos INSERT IGNORE para saltar duplicados en la clave primaria (id_ldd)

    rows_to_insert = []
    # Asegúrate de que los nombres de las columnas en el DataFrame (row['...'])
    # coincidan exactamente con los nombres en tu hoja de Excel (LDD_GO_GPON2)

    for index, row in dataframe.iterrows():
        try:
            rows_to_insert.append((
                row['ID_SMALLWORLD_CAJA'],             # 1 -> id_ldd (PK)
                row['ID_SMALLWORLD_GABINETE'],         # 2 -> id_gabinete (FK a Gabinete)
                row['NOMBRE_CAJA'],                    # 3 -> nombre_caja
                row['FIJACION_CAJA'],                  # 4 -> fijacion_caja
                row['PISO_CAJA'],                      # 5 -> piso_caja
                row['ID_XYGO_DIRECCION_CAJA'],         # 6 -> id_xygo_direccion_caja
                row['DIRECCION_CAJA'],                 # 7 -> direccion_caja
                row['COMUNA_CAJA'],                    # 8 -> comuna_caja
                row['CUENTA_ASIGNADA_CAJA'],           # 9 -> cuenta_asignada_caja
                row['PUERTAS_TOTALES_CAJA'],           # 10 -> puertas_totales_caja
                row['PUERTAS_HABILITADAS_CAJA'],       # 11 -> puertas_habilitadas_caja
                row['PUERTAS_WORKING_DEACTIVATED'],    # 12 -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],            # 13 -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],         # 14 -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],             # 15 -> puertas_en_reserva
                row['PUERTAS_LIBRES_Y_BUENAS'],        # 16 -> puertas_libres_y_buenas
                row['PUERTAS_EN_SERVICIO_CAJA'],       # 17 -> puertas_en_servicio_caja
                row['NRO_DIRS_ASOCIADAS_CAJA'],        # 18 -> nro_dirs_asociadas_caja
                row['ID_SMALLWORLD_EDIFICIO'],         # 19 -> id_smallworld_edificio
                row['LON'],                            # 20 -> lon
                row['LAN']                             # 21 -> lat
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de LDDs.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'LDD_GO_GPON2'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para LDD: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} LDDs completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar LDDs: {e}")
        # Opcional: Imprimir la sentencia SQL y las primeras filas para depurar si hay un error SQL
        # print(f"Sentencia SQL:\n{sql}")
        # print(f"Primeras filas de datos a insertar: {rows_to_insert[:5]}")

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar LDDs: {e}")

        # --- Helper function to load Splitter Primario names and IDs for lookup ---
def load_splitter_primarios_lookup(connection):
    print("Cargando datos de Splitters Primarios para lookup...")
    sp_lookup = {}
    try:
        cursor = connection.cursor(dictionary=False) # Usamos cursor normal, no de diccionario
        # Selecciona ID y Nombre de los Splitters Primarios
        # Asegúrate que los nombres de columna aquí coincidan con tu tabla Splitter_Primario
        cursor.execute("SELECT id_sp, nombre_splitter FROM Splitter_Primario;")
        results = cursor.fetchall()
        # Construye el diccionario {nombre_splitter_excel: id_sp_bd}
        for id_sp_bd, nombre_splitter_bd in results:
            if nombre_splitter_bd is not None:
                # Normaliza el nombre igual que lo harás al buscar desde el Excel
                # Quitar espacios al inicio/final y convertir a minúsculas es una buena práctica
                sp_name_normalized = str(nombre_splitter_bd).strip().lower()
                # Si hay nombres duplicados en Splitter_Primario, el último ID encontrado para ese nombre gana
                sp_lookup[sp_name_normalized] = id_sp_bd
        cursor.close()
        print(f"Cargados {len(sp_lookup)} nombres de Splitters Primarios únicos para lookup.")
        return sp_lookup
    except Error as e:
        print(f"Error al cargar datos de Splitters Primarios para lookup: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al cargar datos de Splitters Primarios para lookup: {e}")
        return None


# --- Función para importar datos a la tabla Splitter_Secundario ---
# Esta función ahora necesita el diccionario de lookup de SPs
def insert_splitter_secundarios(connection, cursor, dataframe, sp_lookup):
    print("Iniciando importación de Splitters Secundarios...")
    if sp_lookup is None:
        print("Error: No se pudo cargar el lookup de Splitters Primarios. Saltando importación de Splitters Secundarios.")
        return

    # Lista las 17 columnas que debe tener la tabla Splitter_Secundario
    sql = """
    INSERT IGNORE INTO Splitter_Secundario (
        id_ss, id_sp, nombre_splitter, puertas_totales, puertas_working_deactivated,
        puertas_en_analisis, puertas_en_a_operativo, puertas_en_reserva, puertas_libres,
        puertas_en_servicio, id_contenedor, tipo_contenedor, direccion_contenedor,
        piso_contenedor, comuna_contenedor, longitud_principal, pta_splitter_primario
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 17 columnas, 17 placeholders %s

    rows_to_insert = []
    # Asegúrate de que los nombres de las columnas en el DataFrame (row['...'])
    # coincidan exactamente con los nombres en tu hoja de Excel (Splitters_Secundarios)

    for index, row in dataframe.iterrows():
        # Lookup del id_sp usando el nombre del Splitter Primario desde el Excel
        id_sp_val = None # Por defecto, el FK será NULL
        sp_name_excel = None
        try:
            sp_name_excel = row['NOMBRE_SPLITTER_PRIMARIO'] # Nombre del SP Primario en la fila actual del Excel
            if pd.notna(sp_name_excel):
                # Normaliza el nombre del Excel igual que lo hiciste al cargar el diccionario
                sp_name_normalized = str(sp_name_excel).strip().lower()
                # Buscar el id_sp en el diccionario usando el nombre normalizado
                id_sp_val = sp_lookup.get(sp_name_normalized) # get() retorna None si la clave no existe

                if id_sp_val is None:
                     # Si el nombre del Excel no se encontró en el diccionario, significa que ese SP Primario no está en la DB
                     print(f"Advertencia: Splitter Primario '{sp_name_excel}' (normalizado '{sp_name_normalized}') no encontrado en la tabla Splitter_Primario para la fila {index} de SS. id_sp será NULL para esta fila.")
        except KeyError:
             # Si la columna 'NOMBRE_SPLITTER_PRIMARIO' no existe en esta fila del DataFrame, maneja el error
             print(f"Advertencia: Columna 'NOMBRE_SPLITTER_PRIMARIO' no encontrada en la fila {index} de Splitters Secundarios. id_sp será NULL para esta fila.")
        except Exception as e:
             # Otros errores durante el lookup
             print(f"Error inesperado al buscar SP '{sp_name_excel}' para fila {index} de SS: {e}")
             id_sp_val = None # Asegurar que sea None en caso de error

        try:
            rows_to_insert.append((
                row['ID_SPLITTER'],              # 1 -> id_ss (PK)
                id_sp_val,                       # 2 -> id_sp (FK, resultado del lookup)
                row['NOMBRE_SPLITTER'],          # 3 -> nombre_splitter
                row['PUERTAS_TOTALES'],          # 4 -> puertas_totales
                row['PUERTAS_WORKING_DEACTIVATED'], # 5 -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],      # 6 -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],   # 7 -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],       # 8 -> puertas_en_reserva
                row['PUERTAS_LIBRES'],           # 9 -> puertas_libres
                row['PUERTAS_EN_SERVICIO'],      # 10 -> puertas_en_servicio
                row['ID_CONTENEDOR'],            # 11 -> id_contenedor
                row['TIPO_CONTENEDOR'],          # 12 -> tipo_contenedor
                row['DIRECCION_CONTENEDOR'],     # 13 -> direccion_contenedor
                row['PISO_CONTENEDOR'],          # 14 -> piso_contenedor
                row['COMUNA_CONTENEDOR'],        # 15 -> comuna_contenedor
                row['LONGITUD_PRINCIPAL'],       # 16 -> longitud_principal
                row['PTA_SPLITTER_PRIMARIO']     # 17 -> pta_splitter_primario
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Splitters Secundarios.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Splitters_Secundarios'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Splitter Secundario: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Splitters Secundarios completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar Splitters Secundarios: {e}")
        # Opcional: Imprimir la sentencia SQL y las primeras filas para depurar si hay un error SQL
        # print(f"Sentencia SQL:\n{sql}")
        # print(f"Primeras filas de datos a insertar: {rows_to_insert[:5]}")

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar Splitters Secundarios: {e}")


        # --- Helper function to load Doble Conector addresses and IDs for lookup ---
# Esto es necesario para la vinculación de CTOs a DCs por dirección
def load_doble_conectores_lookup(connection):
    print("Cargando datos de Doble Conectores para lookup por dirección...")
    dc_lookup = {}
    try:
        cursor = connection.cursor(dictionary=False) # Usamos cursor normal
        # Selecciona ID y Direccion de los Doble Conectores
        # Asegúrate que los nombres de columna aquí coincidan con tu tabla Doble_Conector
        cursor.execute("SELECT id_dc, direccion FROM Doble_Conector;")
        results = cursor.fetchall()
        # Construye el diccionario {direccion_normalizada_bd: id_dc_bd}
        for id_dc_bd, direccion_bd in results:
            if direccion_bd is not None:
                # Normaliza la dirección de la BASE DE DATOS para usar como clave del diccionario
                # Quitar espacios al inicio/final y convertir a minúsculas es una buena práctica
                direccion_normalized = str(direccion_bd).strip().lower()
                # Si hay direcciones duplicadas en Doble_Conector, el último ID encontrado para esa dirección gana.
                # Esto es una limitación de la vinculación por dirección si hay múltiples DCs en la misma dirección.
                if direccion_normalized in dc_lookup:
                     # Advertencia si encontramos duplicados en la DB (esto es un problema de tus datos de origen)
                     print(f"Advertencia: Dirección duplicada en tabla Doble_Conector: '{direccion_bd}' (normalizada '{direccion_normalized}'). El lookup solo usará el id_dc '{id_dc_bd}'.")
                dc_lookup[direccion_normalized] = id_dc_bd
        cursor.close()
        print(f"Cargadas {len(dc_lookup)} direcciones únicas de Doble Conectores para lookup.")
        return dc_lookup
    except Error as e:
        print(f"Error al cargar datos de Doble Conectores para lookup: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado al cargar datos de Doble Conectores para lookup: {e}")
        return None

# --- Función para importar datos a la tabla CTO (desde Cajas_de_Gabinete) ---
# Esta función necesita el diccionario de lookup de DCs por dirección
def insert_ctos(connection, cursor, dataframe, dc_lookup):
    print("Iniciando importación de CTOs (desde Cajas_de_Gabinete)...")
    if dc_lookup is None:
        print("Error: No se pudo cargar el lookup de Doble Conectores. Saltando importación de CTOs.")
        return

    # Lista las 21 columnas que debe tener la tabla CTO (5 iniciales + 16 añadidas)
    sql = """
    INSERT IGNORE INTO CTO (
        id_cto, id_gabinete, id_dc, direccion, cantidad_clientes,
        nombre_caja, fijacion_caja, piso_caja, id_xygo_direccion_caja,
        comuna_caja, cuenta_asignada_caja, puertas_totales_caja, puertas_habilitadas_caja,
        puertas_working_deactivated, puertas_en_analisis, puertas_en_a_operativo,
        puertas_en_reserva, puertas_entrada_libres_buenos, puertas_en_servicio_caja,
        lon, lat
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 21 columnas, 21 placeholders %s
    # Usamos ID_SMALLWORLD_CAJA del Excel para id_cto (PK)
    # Usamos ID_SMALLWORLD_GABINETE del Excel para id_gabinete (FK)
    # Usamos DIRECCION_CAJA del Excel para lookup id_dc (FK)
    # Usamos NRO_DIRS_ASOCIADAS del Excel para cantidad_clientes
    # Usamos INSERT IGNORE para saltar duplicados en la clave primaria (id_cto)


    rows_to_insert = []
    # Asegúrate de que los nombres de las columnas en el DataFrame (row['...'])
    # coincidan exactamente con los nombres en tu hoja de Excel (Cajas_de_Gabinete)

    for index, row in dataframe.iterrows():
        # Lookup del id_dc usando la dirección del Doble Conector desde el Excel (DIRECCION_CAJA)
        id_dc_val = None # Por defecto, el FK será NULL
        direccion_excel = None
        try:
            direccion_excel = row['DIRECCION_CAJA'] # Dirección en la fila actual del Excel
            if pd.notna(direccion_excel):
                # Normaliza la dirección del EXCEL igual que lo hiciste al cargar el diccionario de lookup
                direccion_normalized = str(direccion_excel).strip().lower()
                # Buscar el id_dc en el diccionario usando la dirección normalizada
                id_dc_val = dc_lookup.get(direccion_normalized) # get() retorna None si la clave no existe

                if id_dc_val is None:
                     # Si la dirección del Excel no se encontró en el diccionario de lookup, significa que ese DC no está en la DB
                     # O la dirección no coincide exactamente.
                     # Esto generará una advertencia. El id_dc para esta CTO será NULL.
                     print(f"Advertencia: Dirección de DC '{direccion_excel}' (normalizado '{direccion_normalized}') no encontrada en la tabla Doble_Conector para la fila {index} de CTOs (hoja Cajas_de_Gabinete). id_dc será NULL.")
        except KeyError:
             # Si la columna 'DIRECCION_CAJA' no existe en esta fila del DataFrame
             print(f"Advertencia: Columna 'DIRECCION_CAJA' no encontrada en la fila {index} de CTOs (hoja Cajas_de_Gabinete). id_dc será NULL.")
        except Exception as e:
             # Otros errores durante el lookup
             print(f"Error inesperado al buscar DC por dirección '{direccion_excel}' para fila {index} de CTOs: {e}")
             id_dc_val = None # Asegurar que sea None en caso de error


        try:
            rows_to_insert.append((
                row['ID_SMALLWORLD_CAJA'],             # 1 -> id_cto (PK)
                row['ID_SMALLWORLD_GABINETE'],         # 2 -> id_gabinete (FK a Gabinete)
                id_dc_val,                             # 3 -> id_dc (FK, resultado del lookup por dirección)
                row['DIRECCION_CAJA'],                 # 4 -> direccion (La dirección de la CTO/Caja)
                row['NRO_DIRS_ASOCIADAS_CAJA'],             # 5 -> cantidad_clientes
                row['NOMBRE_CAJA'],                    # 6 -> nombre_caja
                row['FIJACION_CAJA'],                  # 7 -> fijacion_caja
                row['PISO_CAJA'],                      # 8 -> piso_caja
                row['ID_XYGO_DIRECCION_CAJA'],         # 9 -> id_xygo_direccion_caja
                row['COMUNA_CAJA'],                    # 10 -> comuna_caja
                row['CUENTA_ASIGNADA_CAJA'],           # 11 -> cuenta_asignada_caja
                row['PUERTAS_TOTALES_CAJA'],           # 12 -> puertas_totales_caja
                row['PUERTAS_HABILITADAS_CAJA'],       # 13 -> puertas_habilitadas_caja
                row['PUERTAS_WORKING_DEACTIVATED'],    # 14 -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],            # 15 -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],         # 16 -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],             # 17 -> puertas_en_reserva
                row['PUERTAS_ENTRADA_LIBRES_BUENOS'],  # 18 -> puertas_entrada_libres_buenos
                row['PUERTAS_EN_SERVICIO_CAJA'],       # 19 -> puertas_en_servicio_caja
                row['LON'],                            # 20 -> lon
                row['LAN']                             # 21 -> lat
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de CTOs (hoja Cajas_de_Gabinete).")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Cajas_de_Gabinete'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para CTO: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} CTOs completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar CTOs: {e}")
        # Opcional: Imprimir la sentencia SQL y las primeras filas para depurar si hay un error SQL
        # print(f"Sentencia SQL:\n{sql}")
        # print(f"Primeras filas de datos a insertar: {rows_to_insert[:5]}")

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar CTOs: {e}")


        # --- Función para importar datos a la tabla Terminal ---
# Esta función asume que la tabla Splitter_Secundario ya está populada
def insert_terminales(connection, cursor, dataframe):
    print("Iniciando importación de Terminales...")
    # Lista las 23 columnas que debe tener la tabla Terminal
    sql = """
    INSERT IGNORE INTO Terminal (
        id_terminal, id_ss_conectado, nombre_terminal, tipo_terminal,
        dir_edificio_terminal, piso_terminal, comuna, tipo_fijacion,
        puertas_instaladas, puertas_habilitadas, puertas_working_deactivated,
        puertas_en_analisis, puertas_en_a_operativo, puertas_en_reserva,
        puertas_libres, puertas_en_servicio, nro_dirs_asociadas,
        nombre_splitter_primario, longitud_principal, nombre_splitter_conectado,
        nombre_hub, lon, lat
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 23 columnas, 23 placeholders %s
    # Usamos ID_TERMINAL del Excel para id_terminal (PK)
    # Usamos ID_SPLITTER_CONECTADO del Excel para id_ss_conectado (FK a Splitter_Secundario)
    # Usamos INSERT IGNORE para saltar duplicados en la clave primaria (id_terminal)

    rows_to_insert = []
    # Asegúrate de que los nombres de las columnas en el DataFrame (row['...'])
    # coincidan exactamente con los nombres en tu hoja de Excel (Terminales)

    for index, row in dataframe.iterrows():
        # Obtener el ID del Splitter Secundario conectado (FK)
        id_ss_conectado_val = None # Por defecto, el FK será NULL
        try:
            # Obtenemos el valor del Excel para el ID del SS conectado
            ss_id_excel = row['ID_SPLITTER_CONECTADO']
            # Si el valor no es nulo/NaN, lo usamos. Si es nulo o el ID no existe en SS, el FK en la DB será NULL.
            if pd.notna(ss_id_excel):
                 id_ss_conectado_val = ss_id_excel
        except KeyError:
             # Si la columna 'ID_SPLITTER_CONECTADO' no existe en esta fila del DataFrame
             print(f"Advertencia: Columna 'ID_SPLITTER_CONECTADO' no encontrada en la fila {index} de Terminales. id_ss_conectado será NULL.")
        except Exception as e:
             # Otros errores al obtener el ID del SS
             print(f"Error inesperado al obtener ID de SS conectado para fila {index} de Terminales: {e}")
             id_ss_conectado_val = None # Asegurar que sea None en caso de error


        try:
            rows_to_insert.append((
                row['ID_TERMINAL'],                # 1 -> id_terminal (PK)
                id_ss_conectado_val,               # 2 -> id_ss_conectado (FK a Splitter_Secundario)
                row['NOMBRE_TERMINAL'],            # 3 -> nombre_terminal
                row['TIPO_TERMINAL'],              # 4 -> tipo_terminal
                row['DIR_EDIFICIO_TERMINAL'],      # 5 -> dir_edificio_terminal
                row['PISO_TERMINAL'],              # 6 -> piso_terminal
                row['COMUNA'],                     # 7 -> comuna
                row['TIPO_FIJACION'],              # 8 -> tipo_fijacion
                row['PUERTAS_INSTALADAS'],         # 9 -> puertas_instaladas
                row['PUERTAS_HABILITADAS'],        # 10 -> puertas_habilitadas
                row['PUERTAS_WORKING_DEACTIVATED'], # 11 -> puertas_working_deactivated
                row['PUERTAS_EN_ANALISIS'],        # 12 -> puertas_en_analisis
                row['PUERTAS_EN_A_OPERATIVO'],     # 13 -> puertas_en_a_operativo
                row['PUERTAS_EN_RESERVA'],         # 14 -> puertas_en_reserva
                row['PUERTAS_LIBRES'],             # 15 -> puertas_libres
                row['PUERTAS_EN_SERVICIO'],        # 16 -> puertas_en_servicio
                row['NRO_DIRS_ASOCIADAS'],         # 17 -> nro_dirs_asociadas
                row['NOMBRE_SPLITTER_PRIMARIO'],   # 18 -> nombre_splitter_primario (redundante)
                row['LONGITUD_PRINCIPAL'],         # 19 -> longitud_principal
                row['NOMBRE_SPLITTER_CONECTADO'],  # 20 -> nombre_splitter_conectado (redundante)
                row['NOMBRE_HUB'],                 # 21 -> nombre_hub (redundante)
                row['LON'],                        # 22 -> lon
                row['LAN']                         # 23 -> lat
            ))
        except KeyError as e:
            print(f"Error de Columna: La columna '{e}' no fue encontrada en el DataFrame de Terminales.")
            print(f"Verifica el nombre de la columna en tu hoja de Excel 'Terminales'.")
            print(f"Fila problemática (índice en DataFrame): {index}")
            print("Columnas disponibles en DataFrame:", dataframe.columns.tolist()) # Imprimir columnas para depurar
            return # Detiene la importación si falta una columna crucial
        except Exception as e:
             print(f"Error inesperado al procesar fila {index} para Terminal: {e}")
             print("Datos de la fila:", row.to_dict()) # Imprimir datos para depurar
             return # Detiene la importación


    try:
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        print(f"Importación de {cursor.rowcount} Terminales completada con éxito.")

    except Error as e:
        connection.rollback()
        print(f"Error SQL al importar Terminales: {e}")
        # Opcional: Imprimir la sentencia SQL y las primeras filas para depurar si hay un error SQL
        # print(f"Sentencia SQL:\n{sql}")
        # print(f"Primeras filas de datos a insertar: {rows_to_insert[:5]}")

    except Exception as e:
        connection.rollback()
        print(f"Error inesperado general al importar Terminales: {e}")


        # --- Función para importar datos a la tabla Splitter_Gabinete_V2 ---




# --- Bloque Principal del Script ---
if __name__ == "__main__":
    conn = create_db_connection(DB_CONFIG['host'], DB_CONFIG['database'], DB_CONFIG['user'], DB_CONFIG['password'])

    if conn:
        cursor = conn.cursor() # Usaremos un cursor para ejecutar comandos SQL

        # --- Orden de Importación ---
        # Seguiremos el orden de dependencia: Gabinetes, Doble Conectores, Splitters Primarios, etc.
        gabinetes_df = read_excel_sheet(EXCEL_FILE_V2, 'Gabinetes')
        if gabinetes_df is not None:
            insert_gabinetes(conn, cursor, gabinetes_df) # Pasa conexión y cursor

        # 1. Importar Gabinetes
        # gabinetes_df = read_excel_sheet(EXCEL_FILE_V2, 'Gabinetes')
        # if gabinetes_df is not None:
        #     insert_gabinetes(conn, cursor, gabinetes_df) # Pasamos conexión y cursor

        # 2. Importar Splitters Gabinete V2 (desde V2, hoja Splitters) <-- NUEVA IMPORTACIÓN
        # TRUNCATE TABLE Splitter_Gabinete_V2; # Descomentar si quieres vaciar/reimportar
        splitter_gabinete_v2_df = read_excel_sheet(EXCEL_FILE_V2, 'Splitters') # <-- Lee la hoja 'Splitters' del Archivo V2
        if splitter_gabinete_v2_df is not None:
            insert_splitter_gabinete_v2(conn, cursor, splitter_gabinete_v2_df) # <-- Llama a la nueva función

        #3. Importar Doble Conectores (desde V2, hoja Doble_Conectores)
        doble_conectores_df = read_excel_sheet(EXCEL_FILE_V2, 'Doble_Conectores')
        if doble_conectores_df is not None:
            insert_doble_conectores(conn, cursor, doble_conectores_df) # Llama a la nueva función



        # 4. Importar Splitters Primarios (desde V1, hoja Splitters_Primarios)
        splitter_primarios_df = read_excel_sheet(EXCEL_FILE_V1, 'Splitters_Primarios') # <-- Usa EXCEL_FILE_V1
        if splitter_primarios_df is not None:
            insert_splitter_primarios(conn, cursor, splitter_primarios_df) # <-- Llama a la nueva función



        # 5. Importar Cajas de Doble Conector (desde V2, hoja Cajas_de_Doble_Conector)
        caja_doble_conectores_df = read_excel_sheet(EXCEL_FILE_V2, 'Cajas_de_Doble_Conector') # <-- Usa V2 y la hoja correcta
        if caja_doble_conectores_df is not None:
            insert_caja_doble_conectores(conn, cursor, caja_doble_conectores_df) # <-- Llama a la nueva función


        # 6. Importar LDDs (desde V2, hoja LDD_GO_GPON2)
        # Si quieres re-importar LDDs (vacía la tabla antes con TRUNCATE):
        # TRUNCATE TABLE LDD; -- Ejecuta esto en MySQL antes de correr el script si quieres vaciarla
        ldds_df = read_excel_sheet(EXCEL_FILE_V2, 'LDD_GO_GPON2') # <-- Usa V2 y la hoja correcta
        if ldds_df is not None:
            insert_ldds(conn, cursor, ldds_df) # <-- Llama a la nueva función


            # --- Preparar para Splitters Secundarios (requiere lookup de SP) ---
        # Asegúrate de que la tabla Splitter_Primario esté populada antes de esto (paso 3)
        sp_lookup_dict = load_splitter_primarios_lookup(conn) # <-- Llama a la función de lookup ANTES de importar SS

        #7. Importar Splitters Secundarios (desde V1, hoja Splitters_Secundarios)
        if sp_lookup_dict is not None: # Solo importar si el lookup de SP fue exitoso
            # TRUNCATE TABLE Splitter_Secundario; # Descomentar si quieres vaciar/reimportar
            splitter_secundarios_df = read_excel_sheet(EXCEL_FILE_V1, 'Splitters_Secundarios') # Usa V1
            if splitter_secundarios_df is not None:
                # Pasar el diccionario de lookup a la función de importación de SS
                insert_splitter_secundarios(conn, cursor, splitter_secundarios_df, sp_lookup_dict)



                # --- Preparar para CTOs (requiere lookup de DC por dirección) ---
        # Asegúrate de que la tabla Doble_Conector esté populada antes de esto (paso 2)
        # Asegúrate de que la columna 'direccion' en Doble_Conector tenga un índice (ya lo creamos)
        dc_lookup_dict = load_doble_conectores_lookup(conn) # <-- Llama a la función de lookup de DCs

        # 8. Importar CTOs (desde V2, hoja Cajas_de_Gabinete)
        # TRUNCATE TABLE CTO; # Descomentar si quieres vaciar/reimportar
        if dc_lookup_dict is not None: # Solo importar si el lookup de DCs fue exitoso
            # <-- Lee la hoja Cajas_de_Gabinete del archivo V2 para los datos de CTO
            ctos_df = read_excel_sheet(EXCEL_FILE_V2, 'Cajas_de_Gabinete')
            if ctos_df is not None:
                 # Pasar el diccionario de lookup de DCs a la función de importación de CTOs
                insert_ctos(conn, cursor, ctos_df, dc_lookup_dict)


        #9. Importar Terminales (desde V1, hoja Terminales)
        # Asegúrate de que la tabla Splitter_Secundario esté populada antes de esto (paso 6)
        # TRUNCATE TABLE Terminal; # Descomentar si quieres vaciar/reimportar
        terminales_df = read_excel_sheet(EXCEL_FILE_V1, 'Terminales') # <-- Usa V1 y la hoja correcta
        if terminales_df is not None:
            insert_terminales(conn, cursor, terminales_df) # <-- Llama a la nueva función

        # ¡Todas las tablas intentadas!
        print("\n--- Proceso de importación de todas las tablas intentado ---")


            
        # Continúa con las demás tablas...

        # No olvides cerrar la conexión
        cursor.close()
        conn.close()
        print("Conexión a la base de datos cerrada.")