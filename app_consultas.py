import streamlit as st
import pymysql
from pymysql import cursors # Necesario si usas pymysql.cursors.DictCursor
import pandas as pd
import os # Necesario para leer variables de entorno

# Si quieres usar .env localmente, descomenta la siguiente línea
from dotenv import load_dotenv # Asegúrate de que esta línea NO esté comentada

st.set_page_config(layout="wide")

# Cargar variables de entorno desde .env al iniciar el script (solo funciona localmente)
# Si solo usas st.secrets en Streamlit Cloud, puedes ELIMINAR la siguiente línea
load_dotenv() # <--- ¡Asegúrate de que esta línea NO esté comentada!

# --- Función para Conectar a la Base de Datos ---
# Usa cache_resource para mantener la conexión abierta y reutilizarla entre interacciones
@st.cache_resource
def get_db_connection():
    """Establece y retorna una conexión a la base de datos Cloud SQL."""
    try:
        # INTENTAR leer de variables de entorno (desde .env local)
        # Si no están en env, leer de st.secrets (Streamlit Cloud)
        # Asegúrate de que tus keys en .env (ej: MYSQL_HOST) coincidan con estos nombres
        host = os.environ.get("MYSQL_HOST")
        database = os.environ.get("MYSQL_DATABASE")
        user = os.environ.get("MYSQL_USER")
        password = os.environ.get("MYSQL_PASSWORD")
        port_env = os.environ.get("MYSQL_PORT")

        # Fallback a st.secrets si no se encontraron en variables de entorno (para Streamlit Cloud)
        if not host:
             host = st.secrets["connections"]["mysql"]["host"]
        if not database:
             database = st.secrets["connections"]["mysql"]["database"]
        if not user:
             user = st.secrets["connections"]["mysql"]["user"]
        if not password:
             password = st.secrets["connections"]["mysql"]["password"]
        if not port_env:
             port = int(st.secrets["connections"]["mysql"]["port"])
        else:
             port = int(port_env) # Convertir puerto de env a int

        conexion = pymysql.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
            # Puedes añadir charset='utf8mb4' si es necesario
        )
        
        return conexion
    except (KeyError, TypeError) as e:
         # Este error ocurre si falta una clave en st.secrets Y no está en .env/os.environ
         st.error(f"Error: Falta una clave de base de datos en .env/variables de entorno o st.secrets: {e}")
         st.info("Asegúrate de que las variables de entorno (MYSQL_HOST, etc.) en tu .env local O los secretos en Streamlit Cloud ([connections.mysql]) estén configurados correctamente.")
         return None
    except ValueError:
         st.error("Error: El puerto de la base de datos no es un número válido.")
         return None
    except pymysql.Error as e:
        st.error(f"Error al conectar a la base de datos Cloud SQL: {e}")
        st.code(f"Detalles del error: {e}")
        st.info("Verifica las credenciales, la IP pública y las redes autorizadas de tu instancia de Cloud SQL.")
        return None
    except Exception as e:
        st.error(f"Ocurrió un error inesperado al conectar a la base de datos: {e}")
        st.code(f"Detalles del error general: {e}")
        return None

# --- Función para Ejecutar Consultas SQL (Genérica) ---
# Recibe el objeto de conexión como argumento
def ejecutar_consulta(conexion, query, params=None):
    """
    Ejecuta una consulta SQL usando una conexión dada.
    Retorna los datos como lista de diccionarios y los nombres de columna, o None en caso de error.
    """
    # Comprobación de conexión al inicio
    if conexion is None:
        st.warning("No hay conexión a la base de datos para ejecutar la consulta.")
        return None, None # Retorna None si no hay conexión

    try:
        # Usar 'with' para asegurar que el cursor se cierre automáticamente
        # Usamos pymysql.cursors.DictCursor (equivale a dictionary=True)
        with conexion.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query, params)
            # Obtener nombres de columnas solo si hay una descripción (SELECT)
            if cursor.description:
                nombres_columnas = [col[0] for col in cursor.description]
                datos_obtenidos = cursor.fetchall()
            else: # Para INSERT, UPDATE, DELETE, etc.
                 nombres_columnas = []
                 datos_obtenidos = []
                 # Si es una operación DML, considera hacer conexion.commit() aquí
                 # conexion.commit()
                 # st.info(f"Consulta '{query[:50]}...' ejecutada con éxito.")

        return datos_obtenidos, nombres_columnas
    except pymysql.Error as e:
        st.error(f"Error al ejecutar la consulta SQL: {e}")
        # Si hiciste commit antes, considera hacer rollback aquí: conexion.rollback()
        # st.code(f"Consulta fallida: {query}\nParámetros: {params}") # Considera ELIMINAR en producción
        return None, None
    except Exception as e:
        st.error(f"Ocurrió un error inesperado al ejecutar la consulta: {e}")
        # Si hiciste commit antes, considera hacer rollback aquí: conexion.rollback()
        # st.code(f"Consulta: {query}\nParámetros: {params}") # Considera ELIMINAR en producción
        return None, None


# 1. Intentar obtener la conexión a la base de datos
db_conexion = get_db_connection() # Llama a la función que intenta conectar

# --- Interfaz de Streamlit ---
st.title("CRA UNIFICADO")

# --- Sección 1: Búsqueda General por ID (Mejorada) ---

st.subheader("Búsqueda General por ID")


# Campo de entrada para el ID general
entrada_id_general = st.text_input("Introduce un ID a buscar:")

# Botón para realizar la búsqueda general
if st.button("Buscar en Todas las Columnas de ID"):
    # Asegúrate de que tienes el objeto de conexión disponible aquí
    # Normalmente, 'db_conexion' se define al principio del script.
    # Solo procedemos si la conexión existe:
    if 'db_conexion' not in locals() or db_conexion is None:
        st.error("No se pudo conectar a la base de datos. Inténtalo de nuevo.")
    elif entrada_id_general: # Si hay conexión y hay ID de entrada
        st.write(f"Buscando ID: **{entrada_id_general}** en columnas de ID principales y de contenedor...")
        se_encontro_algun_resultado = False

        # Definir las tablas y las columnas de ID donde buscar
        # Cada entrada es: (nombre_de_la_tabla_en_bd, nombre_de_la_columna_id, etiqueta_para_mostrar)
        objetivos_busqueda_id = [

            # Gabinete: ID, Nombre, Dirección
            ("gabinete", "id_gabinete", "Gabinete (ID)"),
            ("gabinete", "nombre_gabinete", "Gabinete (Nombre)"),
            ("gabinete", "direccion", "Gabinete (Dirección)"), # Usar columna 'direccion'

            # CTO: ID, Nombre, Dirección
            ("cto", "id_cto", "CTO (ID)"),
            ("cto", "nombre_caja", "CTO (Nombre)"),
            ("cto", "direccion", "CTO (Dirección)"),

            # Doble Conector: ID, Nombre, Dirección
            ("doble_conector", "id_dc", "Doble Conector (ID)"),
            ("doble_conector", "nombre_dc", "Doble Conector (Nombre)"),
            ("doble_conector", "direccion", "Doble Conector (Dirección)"), # Usar columna 'direccion'

            # Splitter Primario: ID, Nombre, ID Contenedor (mantener este especial)
            ("splitter_primario", "id_sp", "Splitter Primario (ID)"),
            ("splitter_primario", "nombre_splitter", "Splitter Primario (Nombre)"),
            ("splitter_primario", "id_contenedor", "Splitter Primario (Contenedor ID)"), # Mantener este especial

            # Splitter Secundario: ID, Nombre, ID Contenedor (mantener este especial)
            ("splitter_secundario", "id_ss", "Splitter Secundario (ID)"),
            ("splitter_secundario", "nombre_splitter", "Splitter Secundario (Nombre)"),
            ("splitter_secundario", "id_contenedor", "Splitter Secundario (Contenedor ID)"), # Mantener este especial

            # Terminal: ID, Nombre, Dirección
            # Terminal: ID, Nombre, Dirección (Corregido a minúsculas)
            ("terminal", "id_terminal", "Terminal (ID)"),
            ("terminal", "nombre_terminal", "Terminal (Nombre)"),
            ("terminal", "dir_edificio_terminal", "Terminal (Dirección Edificio)"),# Usar columna 'dir_edificio_terminal'

            # LDD: ID, Nombre, Dirección
            ("ldd", "id_ldd", "LDD (ID)"),
            ("ldd", "nombre_caja", "LDD (Nombre)"),
            ("ldd", "direccion_caja", "LDD (Dirección)"),
            
            # Splitter Gabinete V2 (de la hoja 'Splitters' V2): ID, Nombre, Dirección Gabinete <-- ¡Nuevas entradas!
            ("splitter_gabinete_v2", "id_splitter", "Splitter Gabinete V2 (ID)"),
            ("splitter_gabinete_v2", "nombre_splitter", "Splitter Gabinete V2 (Nombre)") # Nombre de esta tabla,

        ]

        # Ejecutar la búsqueda en cada tabla/columna definida
        for nombre_tabla_bd, nombre_columna_id, etiqueta_mostrar in objetivos_busqueda_id:
            query = f"SELECT * FROM `{nombre_tabla_bd}` WHERE `{nombre_columna_id}` = %s;"
            params = (entrada_id_general,)

            # Usamos execute_query que retorna datos y nombres de columnas
            # ¡CORRECCIÓN AQUÍ! Pasar db_conexion como el primer argumento
            datos_resultados, columnas_resultados = ejecutar_consulta(db_conexion, query, params) # <-- CORREGIDO

            if datos_resultados: # Si se encontraron resultados en esta búsqueda específica (en esta tabla/columna)
                st.subheader(f"Resultados encontrados para '{etiqueta_mostrar}':")
                # Crea un DataFrame de pandas y muéstralo en Streamlit
                df_resultados = pd.DataFrame(datos_resultados, columns=columnas_resultados)
                st.dataframe(df_resultados)
                se_encontro_algun_resultado = True # Marcamos que sí encontramos algo

        # Mensaje si no se encontró nada en ninguna búsqueda
        if not se_encontro_algun_resultado:
            st.info(f"No se encontró ningún elemento con el ID '{entrada_id_general}' en ninguna de las columnas de ID buscadas.")

    else: # Si el usuario no introdujo un ID
        st.warning("Por favor, introduce un ID para la búsqueda general.")

# --- Sección 2: Consultas Jerárquicas y Relacionadas ---

# --- Consulta Específica: Gabinete y sus Hijos (Mostrar Listas) ---
st.write("---") # Separador visual para esta sub-consulta
st.subheader("Buscar Gabinete, Resumen de elementos asociados")
st.write("Introduce el ID de un Gabinete para ver su información")

# Usamos una clave única para cada campo de texto
entrada_id_gabinete_jerarquico = st.text_input("ID del Gabinete:", key="gabinete_hierarchical_list_id")

# Usamos una clave única para cada botón
if st.button("Mostrar elementos del gabinete", key="mostrar_gabinete_elementos_button"):
    if entrada_id_gabinete_jerarquico:
        st.write(f"Buscando Gabinete con ID: **{entrada_id_gabinete_jerarquico}** y sus elementos asociados...")

        # Usamos una variable local para el ID buscado para mayor claridad
        gabinete_id_a_buscar = entrada_id_gabinete_jerarquico

        # Query 1: Obtener los detalles del Gabinete
        query_info_gabinete = "SELECT * FROM gabinete WHERE id_gabinete = %s;"
        params = (gabinete_id_a_buscar,)
        datos_info_gabinete, columnas_info_gabinete = ejecutar_consulta(db_conexion, query_info_gabinete, params)

        if datos_info_gabinete: # Si se encontró el Gabinete
            df_info_gabinete = pd.DataFrame(datos_info_gabinete, columns=columnas_info_gabinete)
            nombre_gabinete = datos_info_gabinete[0].get('nombre_gabinete', 'Desconocido')

            st.subheader(f"Información del Gabinete '{nombre_gabinete}':")
            st.dataframe(df_info_gabinete)

            # Query 2: Contar Splitters Gabinete V2 asociados
            # Query 5: Contar Splitters Gabinete V2 asociados <-- ¡ESTA CONSULTA DE CONTEO EXISTE AQUÍ!
            query_conteo_sgv2 = "SELECT COUNT(*) FROM splitter_gabinete_v2 WHERE id_gabinete = %s;"
            datos_conteo_sgv2, _ = ejecutar_consulta(db_conexion, query_conteo_sgv2, params)
            conteo_sgv2 = datos_conteo_sgv2[0].get('COUNT(*)', 0) if datos_conteo_sgv2 else 0

            # Query 3: Contar CTOs asociadas
            query_conteo_cto = "SELECT COUNT(*) FROM cto WHERE id_gabinete = %s;"
            datos_conteo_cto, _ = ejecutar_consulta(db_conexion, query_conteo_cto, params)
            conteo_cto = datos_conteo_cto[0].get('COUNT(*)', 0) if datos_conteo_cto else 0

            # Query 4: Contar Doble Conectores asociados
            query_conteo_dc = "SELECT COUNT(*) FROM doble_conector WHERE id_gabinete = %s;"
            datos_conteo_dc, _ = ejecutar_consulta(db_conexion, query_conteo_dc, params)
            conteo_dc = datos_conteo_dc[0].get('COUNT(*)', 0) if datos_conteo_dc else 0

            
            # Mostrar mensaje resumen de conteos - Incluyendo Splitter Gabinete V2
            st.write(
                f"Resumen de conteos de elementos asociados a **'{nombre_gabinete}'** ({gabinete_id_a_buscar}):"
            )
            st.info(
                f"- **{conteo_sgv2}** splitters gabinete V2\n" 
                f"- **{conteo_cto}** ctos\n"
                f"- **{conteo_dc}** doble Conectores\n"   
            )

            # --- Mostrar Listas de Elementos Hijos ---

            # Query 7: Listar Splitters Gabinete V2 asociados <-- NUEVA CONSULTA DE LISTA
            st.write("---") # Separador visual
            st.subheader(f"Lista de Splitters Gabinete V2 de '{nombre_gabinete}':")
            if conteo_sgv2 > 0:
                query_lista_sgv2 = "SELECT * FROM splitter_gabinete_v2 WHERE id_gabinete = %s;"
                datos_lista_sgv2, columnas_lista_sgv2 = ejecutar_consulta(db_conexion, query_lista_sgv2, params)
                if datos_lista_sgv2:
                    st.dataframe(pd.DataFrame(datos_lista_sgv2, columns=columnas_lista_sgv2))
                else: st.warning("Error al obtener lista de Splitters Gabinete v2 a pesar del conteo.")
            else:
                st.info("No hay Splitters Gabinete V2 asociados a este Gabinete.")

        
            # Query 8: Listar CTOs asociadas
            st.write("---") # Separador visual
            st.subheader(f"Lista de CTOs de '{nombre_gabinete}':")
            if conteo_cto > 0:
                query_lista_cto = "SELECT * FROM cto WHERE id_gabinete = %s;"
                datos_lista_cto, columnas_lista_cto = ejecutar_consulta(db_conexion, query_lista_cto, params)
                if datos_lista_cto:
                    st.dataframe(pd.DataFrame(datos_lista_cto, columns=columnas_lista_cto))
                else: st.warning("Error al obtener lista de CTOs a pesar del conteo.")
            else:
                st.info("No hay CTOs asociadas a este Gabinete.")

            # Query 9: Listar Doble Conectores asociados
            st.write("---") # Separador visual
            st.subheader(f"Lista de Doble Conectores de '{nombre_gabinete}':")
            if conteo_dc > 0:
                query_lista_dc = "SELECT * FROM doble_conector WHERE id_gabinete = %s;"
                datos_lista_dc, columnas_lista_dc = ejecutar_consulta(db_conexion, query_lista_dc, params)
                if datos_lista_dc:
                    st.dataframe(pd.DataFrame(datos_lista_dc, columns=columnas_lista_dc))
                else: st.warning("Error al obtener lista de DCs a pesar del conteo.")
            else:
                st.info("No hay Doble Conectores asociados a este Gabinete.")


        else: # Gabinete not found in Query 1
            st.info(f"No se encontró ningún Gabinete con el ID: {gabinete_id_a_buscar}")

    else: # Input field is empty
        st.warning("Por favor, introduce un ID de Gabinete para mostrar")



# --- Consulta Específica: Doble Conector y sus Relaciones (MODIFICADA) ---
st.write("---") # Separador visual principal para esta sección
st.subheader("Buscar Doble Conector y sus Relaciones")
st.write("Introduce el ID de un Doble Conector para ver su información")

entrada_id_dc_jerarquico = st.text_input("ID del Doble Conector:", key="dc_hierarchical_id")

if st.button("Mostrar Relaciones de Doble Conector", key="show_dc_relations_button"):
    if entrada_id_dc_jerarquico:
        st.write(f"Buscando Doble Conector con ID: **{entrada_id_dc_jerarquico}** y sus relaciones...")

        # Query 1: Get DC details
        query_info_dc = "SELECT * FROM doble_conector WHERE id_dc = %s;"
        params = (entrada_id_dc_jerarquico,)
        datos_info_dc, columnas_info_dc = ejecutar_consulta(db_conexion, query_info_dc, params)

        if datos_info_dc: # Si se encontró el DC
            df_info_dc = pd.DataFrame(datos_info_dc, columns=columnas_info_dc)
            nombre_dc = datos_info_dc[0].get('nombre_dc', 'Desconocido')
            st.subheader(f"Información del Doble Conector '{nombre_dc}':")
            st.dataframe(df_info_dc)

            # Query 2: Get Parent Gabinete Info (JOIN) - usando el FK id_gabinete en Doble_Conector
            st.write("---")
            st.subheader(f"Gabinete  de '{nombre_dc}':")
            query_gabinete_padre = """
                SELECT T2.*
                FROM doble_conector T1
                JOIN gabinete T2 ON T1.id_gabinete = T2.id_gabinete
                WHERE T1.id_dc = %s;
            """
            datos_gabinete_padre, columnas_gabinete_padre = ejecutar_consulta(db_conexion, query_gabinete_padre, params)

            if datos_gabinete_padre:
                st.dataframe(pd.DataFrame(datos_gabinete_padre, columns=columnas_gabinete_padre))
            else:
                st.info(f"Este Doble Conector no tiene un Gabinete asociado ")

            # Query 3: List Child CTOs
            st.write("---")
            st.subheader(f"CTOs pertenencientes a '{nombre_dc}':")
            # Listamos CTOs donde el id_dc es el ID del Doble Conector actual
            query_lista_cto = "SELECT * FROM cto WHERE id_dc = %s;"
            datos_lista_cto, columnas_lista_cto = ejecutar_consulta(db_conexion, query_lista_cto, params)

            if datos_lista_cto:
                st.dataframe(pd.DataFrame(datos_lista_cto, columns=columnas_lista_cto))
            else:
                st.info(f"No hay CTOs asociadas directamente a este Doble Conector.")

            # Query 4: List Associated Cajas de Doble Conector <-- NUEVA CONSULTA
            st.write("---")
            st.subheader(f"Cajas de Doble Conector Asociadas a '{nombre_dc}':")
            # Listamos Cajas_de_Doble_Conector donde el id_dc es el ID del Doble Conector actual
            query_lista_cdc = "SELECT * FROM caja_doble_conector WHERE id_dc = %s;"
            datos_lista_cdc, columnas_lista_cdc = ejecutar_consulta(db_conexion, query_lista_cdc, params)

            if datos_lista_cdc:
                st.dataframe(pd.DataFrame(datos_lista_cdc, columns=columnas_lista_cdc))
            else:
                st.info(f"No hay Cajas de Doble Conector asociadas a este Doble Conector.")


        else: # DC not found in Query 1
            st.info(f"No se encontró ningún Doble Conector con el ID: {entrada_id_dc_jerarquico}")

    else: # Input field is empty
        st.warning("Por favor, introduce un ID de Doble Conector para mostrar sus relaciones.")


# --- Consulta Específica: Splitter Secundario y sus Relaciones ---
st.write("---") # Separador visual principal para esta sección
st.subheader("Buscar Splitter Secundario y sus Relaciones")
st.write("Introduce el ID de un Splitter Secundario para ver su información,")

entrada_id_ss_jerarquico = st.text_input("ID del Splitter Secundario:", key="ss_hierarchical_id")

if st.button("Mostrar Relaciones de Splitter Secundario", key="show_ss_relations_button"):
    if entrada_id_ss_jerarquico:
        st.write(f"Buscando Splitter Secundario con ID: **{entrada_id_ss_jerarquico}** y sus relaciones...")

        # Query 1: Get SS details
        query_info_ss = "SELECT * FROM splitter_secundario WHERE id_ss = %s;"
        params = (entrada_id_ss_jerarquico,)
        datos_info_ss, columnas_info_ss = ejecutar_consulta(db_conexion, query_info_ss, params)

        if datos_info_ss: # Si se encontró el SS
            df_info_ss = pd.DataFrame(datos_info_ss, columns=columnas_info_ss)
            # Asumimos que Splitter_Secundario también tiene una columna 'nombre_splitter'
            nombre_ss = datos_info_ss[0].get('nombre_splitter', 'Desconocido')
            st.subheader(f"Información del Splitter Secundario '{nombre_ss}':")
            st.dataframe(df_info_ss)

            # Query 2: Get Parent Splitter Primario Info (JOIN) - usando el FK id_sp en Splitter_Secundario
            st.write("---")
            st.subheader(f"splitter Primario de '{nombre_ss}':")
            # Unimos Splitter_Secundario con Splitter_Primario en base al FK id_sp en SS
            query_sp_padre = """
                SELECT T2.*
                FROM splitter_secundario T1
                JOIN splitter_primario T2 ON T1.id_sp = T2.id_sp
                WHERE T1.id_ss = %s;
            """
            datos_sp_padre, columnas_sp_padre = ejecutar_consulta(db_conexion, query_sp_padre, params)

            if datos_sp_padre:
                st.dataframe(pd.DataFrame(datos_sp_padre, columns=columnas_sp_padre))
            else:
                st.info(f"Este Splitter Secundario no tiene un Splitter Primario  asociado ")

            # Query 3: List Child Terminales
            st.write("---")
            st.subheader(f"Terminales Conectados a '{nombre_ss}':")
            # Listamos Terminales donde el id_ss_conectado es el ID del Splitter Secundario actual
            # Asegúrate que la columna FK en Terminales se llama id_ss_conectado
            query_lista_terminales = "SELECT * FROM terminal WHERE id_ss_conectado = %s;"
            datos_lista_terminales, columnas_lista_terminales = ejecutar_consulta(db_conexion, query_lista_terminales, params)

            if datos_lista_terminales:
                st.dataframe(pd.DataFrame(datos_lista_terminales, columns=columnas_lista_terminales))
            else:
                st.info(f"No hay Terminales conectados directamente a este Splitter Secundario.")


        else: # SS not found in Query 1
            st.info(f"No se encontró ningún Splitter Secundario con el ID: {entrada_id_ss_jerarquico}")

    else: # Input field is empty
        st.warning("Por favor, introduce un ID de Splitter Secundario para mostrar sus relaciones.")


# --- Consulta Específica: Terminal y su Relación Padre ---
st.write("---") # Separador visual principal para esta sección
st.subheader("Buscar Terminal y sus elementos relacionados")
st.write("Introduce el ID de un Terminal para ver su información y el Splitter Secundario al que está conectado.")

entrada_id_terminal_jerarquico = st.text_input("ID del Terminal:", key="terminal_hierarchical_id")

if st.button("Mostrar Relación de Terminal", key="show_terminal_relation_button"):
    if entrada_id_terminal_jerarquico:
        st.write(f"Buscando Terminal con ID: **{entrada_id_terminal_jerarquico}** y su relación padre...")

        # Query 1: Get Terminal details
        query_info_terminal = "SELECT * FROM terminal WHERE id_terminal = %s;"
        params = (entrada_id_terminal_jerarquico,)
        datos_info_terminal, columnas_info_terminal = ejecutar_consulta(db_conexion, query_info_terminal, params)

        if datos_info_terminal: # Si se encontró el Terminal
            df_info_terminal = pd.DataFrame(datos_info_terminal, columns=columnas_info_terminal)
            # Asumimos que Terminal tiene una columna 'nombre_terminal'
            nombre_terminal = datos_info_terminal[0].get('nombre_terminal', 'Desconocido')
            st.subheader(f"Información del Terminal '{nombre_terminal}':")
            st.dataframe(df_info_terminal)

            # Query 2: Get Parent Splitter Secundario Info (JOIN) - usando el FK id_ss_conectado en Terminal
            st.write("---")
            st.subheader(f"Splitter Secundario al que está conectado '{nombre_terminal}':")
            query_ss_padre = """
                SELECT T2.*
                FROM terminal T1
                JOIN splitter_secundario T2 ON T1.id_ss_conectado = T2.id_ss
                WHERE T1.id_terminal = %s;
            """
            datos_ss_padre, columnas_ss_padre = ejecutar_consulta(db_conexion, query_ss_padre, params)

            if datos_ss_padre:
                st.dataframe(pd.DataFrame(datos_ss_padre, columns=columnas_ss_padre))
            else:
                st.info(f"Este Terminal no tiene un Splitter Secundario padre asociado ")


        else: # Terminal not found in Query 1
            st.info(f"No se encontró ningún Terminal con el ID: {entrada_id_terminal_jerarquico}")

    else: # Input field is empty
        st.warning("Por favor, introduce un ID de Terminal para mostrar su relación padre.")



