import boto3
from procesamiento import procesar_acta

# Cliente de S3
s3 = boto3.client('s3')

# Ruta del bucket
bucket_name = 'uq-datalake'

def listar_archivos_en_ruta(bucket_name, ruta_prefijo):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=ruta_prefijo)

        # Verificar si hay archivos en la ruta
        if 'Contents' in response:
            print('Archivos encontrados')
            archivos = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.csv')]
            return archivos
        else:
            print(f"No se encontraron archivos en la ruta {ruta_prefijo}")
            return []
    except Exception as e:
        print(f"Error al listar los archivos en S3: {e}")
        return []

def main():
    # Ruta dentro de S3 donde están los archivos a procesar
    ruta_prefijo = 'archivos/'

    # Listar todos los archivos CSV en la ruta
    archivos_en_ruta = listar_archivos_en_ruta(bucket_name, ruta_prefijo)

    # Procesar cada archivo
    for archivo_s3 in archivos_en_ruta:
        print(f"Procesando archivo: {archivo_s3}")
        procesar_acta(archivo_s3)

if __name__ == "__main__":
    main()
