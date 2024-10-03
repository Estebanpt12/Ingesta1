import datetime

def registrar_error(error_msg):
    with open('logs/log_errores.txt', 'a') as log:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log.write(f"[{timestamp}] {error_msg}\n")