import datetime
import boto3
import pandas as pd
from io import StringIO
from utils import obtener_semestre, obtener_facultad_programa, limpiar_columnas
from validaciones import validar_datos
from logs import registrar_error

s3 = boto3.client('s3')
bucket_name = 'uq-datalake'

def procesar_acta(nombre_archivo_s3):
    try:
        # Descarga el archivo CSV desde S3
        obj = s3.get_object(Bucket=bucket_name, Key=nombre_archivo_s3)
        df = pd.read_csv(obj['Body'], encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(obj['Body'], encoding='latin-1')
    except Exception as e:
        registrar_error(f"Error al leer el archivo {nombre_archivo_s3}: {e}")
        return

    # Limpiar nombres de columnas
    limpiar_columnas(df)

    # Validar datos
    if validar_datos(df, nombre_archivo_s3):
        # Obtener ruta dinámica
        semestre = obtener_semestre()
        facultad, programa = obtener_facultad_programa(nombre_archivo_s3)

        ruta_s3 = f"raw/semestre={semestre}/area=academico/facultad={facultad}/programa={programa}/acta_notas_{programa}.csv"

        # Convertir DataFrame a CSV en memoria
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Cargar archivo procesado a S3
        cargar_archivo_s3(csv_buffer.getvalue(), ruta_s3, semestre, facultad, programa)

        # Eliminar el archivo original después de la carga exitosa
        eliminar_archivo_s3(nombre_archivo_s3)
    else:
        print(f"Errores encontrados en el archivo {nombre_archivo_s3}, revisa el log de errores.")

def cargar_archivo_s3(contenido_csv, ruta_s3, semestre, facultad, programa):
    try:
        metadata = {
            "sem": semestre,
            "area": "academico",
            "fac": facultad,
            "prog": programa,
            "tipo_doc": "notas",
            "subarea": facultad,
            "fecha_creacion": datetime.datetime.now().isoformat(),
            "descripcion": f"notas de estudiantes del programa {programa} para el semestre {semestre}",
            "confidencialidad": "restringido",
            "tipo_archivo": "notas_estudiantes"
        }
        # Subir el archivo CSV a S3 desde la memoria
        s3.put_object(Body=contenido_csv, Bucket=bucket_name, Key=ruta_s3, Metadata=metadata)
        print(f"Archivo cargado exitosamente a {ruta_s3}")
    except Exception as e:
        registrar_error(f"Error al cargar el archivo a {ruta_s3}: {e}")

def eliminar_archivo_s3(nombre_archivo_s3):
    try:
        # Eliminar el archivo de la ruta inicial
        s3.delete_object(Bucket=bucket_name, Key=nombre_archivo_s3)
        print(f"Archivo {nombre_archivo_s3} eliminado correctamente de la ruta original.")
    except Exception as e:
        registrar_error(f"Error al eliminar el archivo {nombre_archivo_s3}: {e}")