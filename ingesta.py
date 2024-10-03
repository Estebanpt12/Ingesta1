import boto3
import pandas as pd
import re
from logs import registrar_error

s3 = boto3.client('s3')

# Ruta del bucket
bucket_name = 'uq-datalake'

def validar_datos(df, nombre_archivo):
    columnas_requeridas = [
        'Nombre del estudiante', 'Código del estudiante', 'Materia', 
        'Nota', 'Periodo académico', 'Programa académico'
    ]
    
    errores_encontrados = False

    # Validar que las columnas requeridas estén presentes
    for columna in columnas_requeridas:
        if columna not in df.columns:
            error_msg = f"Error en {nombre_archivo}: Columna faltante: {columna}"
            registrar_error(error_msg)
            errores_encontrados = True

    # Validar cada registro del DataFrame
    for index, row in df.iterrows():
        # Validar que no haya valores nulos
        if row.isnull().any():
            error_msg = f"Error en {nombre_archivo}, fila {index+1}: Hay valores nulos en la fila"
            registrar_error(error_msg)
            errores_encontrados = True
            continue  # Si hay nulos, no procede a las siguientes validaciones de esa fila

        # Validar que el código del estudiante sea numérico
        if 'Código del estudiante' in df.columns:
            if not str(row['Código del estudiante']).isdigit():
                error_msg = f"Error en {nombre_archivo}, fila {index+1}: Código del estudiante no es numérico"
                registrar_error(error_msg)
                errores_encontrados = True

        # Validar que la nota sea un float
        if 'Nota' in df.columns:
            try:
                nota = float(row['Nota'])
                if nota < 0 or nota > 5:  # Rango típico de calificaciones
                    error_msg = f"Error en {nombre_archivo}, fila {index+1}: Nota fuera de rango (0-5)"
                    registrar_error(error_msg)
                    errores_encontrados = True
            except ValueError:
                error_msg = f"Error en {nombre_archivo}, fila {index+1}: La nota no es un número válido"
                registrar_error(error_msg)
                errores_encontrados = True

        # Validar que el periodo académico siga el formato YYYY-N (N puede ser 1 o 2)
        if 'Periodo académico' in df.columns:
            if not re.match(r'^\d{4}-[12]$', str(row['Periodo académico'])):
                error_msg = f"Error en {nombre_archivo}, fila {index+1}: Formato de periodo académico inválido (Debe ser YYYY-1 o YYYY-2)"
                registrar_error(error_msg)
                errores_encontrados = True

        # Validar que los campos de texto no tengan caracteres raros
        if 'Nombre del estudiante' in df.columns:
            if not re.match(r"^[A-Za-zÁÉÍÓÚáéíóúñÑ' ]+$", str(row['Nombre del estudiante'])):
                error_msg = f"Error en {nombre_archivo}, fila {index+1}: El nombre contiene caracteres inválidos"
                registrar_error(error_msg)
                errores_encontrados = True

        if 'Materia' in df.columns:
            if not re.match(r"^[A-Za-z0-9ÁÉÍÓÚáéíóúñÑ' ]+$", str(row['Materia'])):
                error_msg = f"Error en {nombre_archivo}, fila {index+1}: La materia contiene caracteres inválidos"
                registrar_error(error_msg)
                errores_encontrados = True

        if 'Programa académico' in df.columns:
            if not re.match(r"^[A-Za-zÁÉÍÓÚáéíóúñÑ' ]+$", str(row['Programa académico'])):
                error_msg = f"Error en {nombre_archivo}, fila {index+1}: El programa académico contiene caracteres inválidos"
                registrar_error(error_msg)
                errores_encontrados = True

    return not errores_encontrados

def cargar_archivo_s3(nombre_archivo, ruta_s3):
    try:
        s3.upload_file(nombre_archivo, bucket_name, ruta_s3)
        print(f"Archivo {nombre_archivo} cargado exitosamente a {ruta_s3}")
    except Exception as e:
        registrar_error(f"Error al cargar el archivo {nombre_archivo}: {e}")

def limpiar_columnas(df):
    # Limpiar nombres de columnas quitando espacios extra o caracteres invisibles
    df.columns = df.columns.str.strip()

def procesar_acta(nombre_archivo, ruta_s3):
    # Cargar archivo CSV con codificación
    try:
        df = pd.read_csv(nombre_archivo, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(nombre_archivo, encoding='latin-1')
    except Exception as e:
        registrar_error(f"Error al leer el archivo CSV {nombre_archivo}: {e}")
        return
    
    # Limpiar nombres de columnas
    limpiar_columnas(df)
    
    # Validar datos
    if validar_datos(df, nombre_archivo):
        # Cargar a S3 si la validación es exitosa
        cargar_archivo_s3(nombre_archivo, ruta_s3)
    else:
        print(f"Errores encontrados en el archivo {nombre_archivo}, revisa el log de errores.")

# Ejemplo de uso con dos archivos
procesar_acta(
    'archivos/notas_ing_sistemas.csv',
    'raw/semestre=2024-1/area=academico/facultad=ingenieria/programa=ingenieria_sistemas/acta_notas_ingenieria_sistemas.csv'
)

procesar_acta(
    'archivos/notas_ing_electronica.csv',
    'raw/semestre=2024-1/area=academico/facultad=ingenieria/programa=ingenieria_electronica/acta_notas_ingenieria_electronica.csv'
)
