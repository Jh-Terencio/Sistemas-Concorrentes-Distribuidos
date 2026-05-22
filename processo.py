import socket
import time
from datetime import datetime

from utils import (MSG_GRANT, MSG_RELEASE, MSG_REQUEST, parsear, receber_completo, serializar)

HOST = "127.0.0.1"
PORT = 5000

ARQUIVO_RESULTADO = "resultado.txt"

def run_processo(pid: int, r: int, k: float) -> None:
    """Roda o ciclo completo de um processo cliente."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT)) # Conecta com o coordenador

    try:
        for i in range(r):
            time.sleep(k) # Dormindo antes de pedir acesso à RC

            sock.sendall(serializar(MSG_REQUEST, pid)) # Envia REQUEST para o coordenador (Acesso a RC)

            esperar_grant(sock, pid) # Espera até receber GRANT do coordenador para este PID

            # REGIÃO CRÍTICA 
            agora = datetime.now().strftime("%H:%M:%S.%f")[:-3]  # ms
            with open(ARQUIVO_RESULTADO, "a", encoding="utf-8") as f:
                f.write(f"PID {pid} | {agora}\n")

            sock.sendall(serializar(MSG_RELEASE, pid))
    finally:
        try:
            sock.close()
        except OSError:
            pass

def esperar_grant(sock: socket.socket, pid_esperado: int) -> None:
    """Lê do socket até receber um GRANT para `pid_esperado`."""
    while True: # Fica em loop lendo do socket até receber o GRANT para este PID
        dados = receber_completo(sock)
        
        id_msg, pid_msg = parsear(dados)
        
        if id_msg == MSG_GRANT and pid_msg == pid_esperado:
            return
