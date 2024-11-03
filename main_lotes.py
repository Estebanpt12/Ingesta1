import boto3
import os
import logging
from datetime import datetime
from PyPDF2 import PdfReader
import docx  

# Configuración de logging para registrar errores
s3 = boto3.client('s3')
bucket_name = 'uq-datalake'
carpeta_base = 'archivos/'
carpeta_destino = 'raw/'
carpeta_extras = 'extras/'  # Carpeta para archivos que no se procesaron correctamente
temp_dir = "/tmp"  # Usar el directorio temporal de Lambda

# Configuración del logger
log_file_path = os.path.join(temp_dir, "log_errores.txt")
logging.basicConfig(filename=log_file_path, level=logging.ERROR)

# Campos requeridos en los archivos de investigación
required_fields = ["Nombre del investigador", "Código de investigación", "Título del informe", 
                   "Fecha de publicación", "Resumen", "Palabras clave"]

def get_file_size(file_key):
    """Obtener el tamaño del archivo en bytes"""
    response = s3.head_object(Bucket=bucket_name, Key=file_key)
    return response['ContentLength']

def batch_ingest_investigations():
    """Función para ingestar archivos de investigaciones por lotes en el Data Lake con validación y metadatos"""
    try:
        # Obtener lista de archivos en la carpeta de origen
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=carpeta_base)
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(('.pdf', '.docx'))]
        # Variables de control para lotes
        current_batch = []
        current_size = 0
        for file_key in files:
            file_size = get_file_size(file_key)
            # Validar archivo de actas de investigación antes de añadirlo al lote
            if validate_research_data(file_key):
                current_batch.append(file_key)
                current_size += file_size
            else:
                move_to_extras(file_key)  # Mover archivo no procesado a la carpeta /extras
            # Cargar el lote si se cumple el tamaño o número de archivos
            if len(current_batch) >= 5 or current_size >= 100 * 1024 * 1024:
                upload_batch(current_batch)
                current_batch.clear()
                current_size = 0
        # Subir archivos restantes si los hay
        if current_batch:
            upload_batch(current_batch)
        # Eliminar archivos originales
        delete_processed_files(files)
        # Subir el log de errores a S3
        upload_error_log_to_s3()
    except Exception as e:
        logging.error(f"{datetime.now()} - Error en la ingesta por lotes de investigaciones: {e}")
        print("Ocurrió un error en la ingesta por lotes. Revisa el archivo log para más detalles.")

def upload_error_log_to_s3():
    """Subir el archivo de log de errores al bucket S3"""
    try:
        s3.upload_file(log_file_path, bucket_name, f"{carpeta_base}log_errores.txt")
        print(f"Archivo de log de errores subido a S3 en {carpeta_base}log_errores.txt")
    except Exception as e:
        logging.error(f"{datetime.now()} - Error al subir el log de errores a S3: {e}")

def upload_batch(batch_files):
    """Función para cargar un lote de archivos de investigación en el Data Lake con metadatos"""
    for file_key in batch_files:
        try:
            file_name = os.path.basename(file_key)

            name_without_extension, extension = file_name.rsplit('.', 1)  # Divide en el último punto

            # Divide el nombre del archivo en partes
            parts = name_without_extension.split('_')
            if len(parts) >= 3:
                semestre = parts[0]  # Ejemplo: '2024-2'
                grupo_investigacion = parts[2]     # Ejemplo: 'investigaciones_sinfoci'
                nombre_proyecto = '_'.join(parts[3:]) + '.' + extension  # Combina el resto como nombre del proyecto con extensión
            else:
                raise ValueError("El nombre del archivo no contiene suficientes partes para extraer semestre, grupo y nombre del proyecto.")

            destination_key = f"{carpeta_destino}semestre={semestre}/area=investigaciones/grupo={grupo_investigacion}/{nombre_proyecto}"
            
            # Definir los metadatos
            metadata = {
                "tipo_doc": "acta_investigacion",
                "fecha_creacion": datetime.now().isoformat(),
                "descripcion": f"Acta de investigacion para el grupo {grupo_investigacion}.",
                "confidencialidad": "restringido",
                "tipo_archivo": "acta_investigacion"
            }
            
            # Copiar archivo a la nueva ubicación con metadatos
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=destination_key,
                Metadata=metadata,
                MetadataDirective="REPLACE"
            )
            print(f"Archivo {file_name} cargado en {destination_key} con metadatos.")
            
        except Exception as e:
            logging.error(f"{datetime.now()} - Error al cargar el archivo {file_key}: {e}")

def move_to_extras(file_key):
    """Función para mover archivos no procesados a la carpeta /extras"""
    try:
        file_name = os.path.basename(file_key)
        destination_key = carpeta_extras + file_name
        
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': file_key},
            Key=destination_key,
            MetadataDirective="REPLACE"  # Para asegurarte de que los metadatos no se copien
        )
        
        # Eliminar el archivo original
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"Archivo {file_name} movido a {destination_key} debido a errores en el procesamiento.")
        
    except Exception as e:
        logging.error(f"{datetime.now()} - Error al mover el archivo {file_key} a extras: {e}")

def delete_processed_files(files):
    """Función para eliminar archivos originales procesados"""
    for file_key in files:
        try:
            s3.delete_object(Bucket=bucket_name, Key=file_key)
            print(f"Archivo {file_key} eliminado de la carpeta original.")
        except Exception as e:
            logging.error(f"{datetime.now()} - Error al eliminar el archivo {file_key}: {e}")

def validate_research_data(file_key):
    """Función para validar datos en archivos de actas de investigación (PDF o DOCX)"""
    try:
        # Descargar el archivo temporalmente
        file_name = os.path.join(temp_dir, os.path.basename(file_key))
        s3.download_file(bucket_name, file_key, file_name) 

        # Leer y validar el contenido del archivo
        if file_name.endswith('.pdf'):
            text = extract_text_from_pdf(file_name)
        elif file_name.endswith('.docx'):
            text = extract_text_from_docx(file_name)

        # Verificar que todos los campos requeridos estén en el texto
        missing_fields = [field for field in required_fields if field not in text]
        if missing_fields:
            logging.error(f"{datetime.now()} - Archivo {file_key} faltan datos: {', '.join(missing_fields)}")
            return False

        return True

    except Exception as e:
        logging.error(f"{datetime.now()} - Error en la validación del archivo {file_key}: {e}")
        return False
    finally:
        # Eliminar el archivo local después de procesarlo
        if os.path.exists(file_name):
            os.remove(file_name)  # Elimina el archivo local descargado

def extract_text_from_pdf(file_name):
    """Función para extraer texto de un archivo PDF usando PdfReader"""
    text = ""
    with open(file_name, "rb") as pdf_file:
        pdf_reader = PdfReader(pdf_file)
        for page in pdf_reader.pages:
            text += page.extract_text()  # Extrae el texto de cada página
    return text


def extract_text_from_docx(file_name):
    """Función para extraer texto de un archivo DOCX"""
    doc = docx.Document(file_name)
    text = " ".join([paragraph.text for paragraph in doc.paragraphs])
    return text


if __name__ == "__main__":
    print("Iniciando el proceso de ingesta por lotes...")
    batch_ingest_investigations()
    print("Proceso de ingesta finalizado.")
