import datetime
import boto3

s3 = boto3.client('s3')
bucket_name = 'uq-datalake'
log_file_s3 = 'archivos/log_errores.txt'  # Ruta en S3 para el archivo de log

def registrar_error(error_msg):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    error_message = f"[{timestamp}] {error_msg}\n"
    
    try:
        # Intentar obtener el contenido actual del archivo de log en S3
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=log_file_s3)
            # Intentar leer el archivo en 'utf-8'
            try:
                current_log = obj['Body'].read().decode('utf-8')
            except UnicodeDecodeError:
                # Si falla, intenta con 'latin-1'
                current_log = obj['Body'].read().decode('latin-1')
        except s3.exceptions.NoSuchKey:
            # Si el archivo no existe, se crea un log vacío
            current_log = ""

        # Agregar el nuevo mensaje de error al log existente
        updated_log = current_log + error_message

        # Sobrescribir el archivo de log en S3 con el contenido actualizado
        s3.put_object(Body=updated_log.encode('utf-8'), Bucket=bucket_name, Key=log_file_s3)
    
    except Exception as e:
        print(f"Error al registrar el error: {e}")