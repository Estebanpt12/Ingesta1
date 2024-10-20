import re
from logs import registrar_error

def validar_datos(df, nombre_archivo):
    columnas_requeridas = [
        'Nombre del estudiante', 'Código del estudiante', 'Materia', 
        'Nota', 'Periodo académico', 'Programa académico'
    ]
    
    errores_encontrados = False

    for columna in columnas_requeridas:
        if columna not in df.columns:
            registrar_error(f"Error en {nombre_archivo}: Columna faltante: {columna}")
            errores_encontrados = True

    for index, row in df.iterrows():
        if row.isnull().any():
            registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Valores nulos")
            errores_encontrados = True
            continue

        if 'Código del estudiante' in df.columns and not str(row['Código del estudiante']).isdigit():
            registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Código no es numérico")
            errores_encontrados = True

        if 'Nota' in df.columns:
            try:
                nota = float(row['Nota'])
                if nota < 0 or nota > 5:
                    registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Nota fuera de rango")
                    errores_encontrados = True
            except ValueError:
                registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Nota no es un número válido")
                errores_encontrados = True

        if 'Periodo académico' in df.columns and not re.match(r'^\d{4}-[12]$', str(row['Periodo académico'])):
            registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Periodo inválido")
            errores_encontrados = True

        if 'Nombre del estudiante' in df.columns and not re.match(r"^[A-Za-zÁÉÍÓÚáéíóúñÑ' ]+$", str(row['Nombre del estudiante'])):
            registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Nombre con caracteres inválidos")
            errores_encontrados = True

        if 'Materia' in df.columns and not re.match(r"^[A-Za-z0-9ÁÉÍÓÚáéíóúñÑ' ]+$", str(row['Materia'])):
            registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Materia con caracteres inválidos")
            errores_encontrados = True

        if 'Programa académico' in df.columns and not re.match(r"^[A-Za-zÁÉÍÓÚáéíóúñÑ' ]+$", str(row['Programa académico'])):
            registrar_error(f"Error en {nombre_archivo}, fila {index+1}: Programa con caracteres inválidos")
            errores_encontrados = True

    return not errores_encontrados