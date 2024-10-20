import datetime

def obtener_semestre():
    año_actual = datetime.datetime.now().year
    mes_actual = datetime.datetime.now().month
    semestre = 1 if mes_actual <= 6 else 2
    return f"{año_actual}-{semestre}"

def obtener_facultad_programa(nombre_archivo):
    facultades = ['ciencias_agroindustriales', 'ciencias_humanas', 'ingenieria', 'medicina', 'economia']
    for facultad in facultades:
        if facultad in nombre_archivo:
            # Capturar el programa completo, incluyendo el nombre de la facultad
            programa = nombre_archivo.split(facultad + '_', 1)[1].rsplit('.csv', 1)[0]
            if programa == 'ingenieria':
                programa = facultad + '_' + programa
            return facultad, programa
    return 'desconocido', 'desconocido'

def limpiar_columnas(df):
    df.columns = df.columns.str.strip()