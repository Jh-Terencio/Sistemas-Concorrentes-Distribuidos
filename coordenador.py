"""
Coordenador da exclusão mútua centralizada distribuída.

Como rodar:
    python coordenador.py
"""

import logging
import selectors
import socket
import threading
import time
from collections import deque
from pathlib import Path
from utils import (MSG_GRANT, MSG_RELEASE, MSG_REQUEST, parsear, receber_completo, serializar)

HOST = "127.0.0.1"
PORT = 5000

fila_pedidos: deque[int] = deque() # Fila Q do algoritmo: a cabeça (índice 0) é o titular da RC; os demais aguardam, em ordem de chegada.
clientes: dict[int, socket.socket] = {} # PID -> socket do cliente
estado_lock = threading.Lock()

atendidos: dict[int, int] = {}     

executando = threading.Event() # Controla o loop das threads; set() para rodar, clear() para parar.
executando.set()

seletor = selectors.DefaultSelector() # Usado pela thread do algoritmo para esperar mensagens de QUALQUER cliente sem busy-wait. Optamos pelo selectors ao invés do busy-wait para evitar consumo excessivo de CPU e garantir que o coordenador seja responsivo mesmo com muitos clientes ou quando um cliente trava.

def configurar_log() -> None:
    """Configura o logger para gravar em logs/coordenador.log com timestamp em milissegundos"""
    Path("logs").mkdir(exist_ok=True)
    handler = logging.FileHandler("logs/coordenador.log", mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter(fmt="%(asctime)s.%(msecs)03d | %(message)s", datefmt="%H:%M:%S"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

# Para mostrar o log no terminal
def log(evento: str) -> None:
    logging.info(evento)
    print(f"[LOG] {evento}")

def enviar_grant(pid: int) -> None:
    """Envia GRANT para o processo `pid` e registra no log. O titular da RC é sempre fila_pedidos[0]."""
    sock = clientes.get(pid)
    if sock is None:
        log(f"Tentativa de enviar GRANT para PID {pid}, mas socket não encontrado.")
        return
    sock.sendall(serializar(MSG_GRANT, pid))
    log(f"ENVIADO GRANT   -> PID {pid}")

# Thread 1: Aceitar conexões
def thread_aceitar(servidor: socket.socket) -> None:
    """Aceita conexões TCP"""
    while executando.is_set():
        
        sock_cliente, addr = servidor.accept()

        # Lê o primeiro REQUEST para descobrir o PID do cliente. (Handshake simples: o cliente tem que mandar um REQUEST logo ao conectar, e o PID vem daí. Poderia ser mais elaborado, mas é suficiente para este exercício.)
        dados = receber_completo(sock_cliente)
        
        id_msg, pid = parsear(dados)
        if id_msg != MSG_REQUEST:
            log(f"Handshake inválido de {addr}: id={id_msg}")
            sock_cliente.close()
            continue

        with estado_lock: # estado_lock para proteger a estrutura de clientes e fila_pedidos, assim dois clientes não se atropelam ao conectar ao mesmo tempo.
            clientes[pid] = sock_cliente
            seletor.register(sock_cliente, selectors.EVENT_READ, data=pid) # Registra o socket do cliente no seletor para a thread do algoritmo ler mensagens dele.
            log(f"RECEBIDO REQUEST <- PID {pid} (conexão de {addr[0]}:{addr[1]})")
            if not fila_pedidos:        # Q vazia: este PID vira o titular e já recebe o GRANT.
                enviar_grant(pid)
            fila_pedidos.append(pid)    # Entra na fila (na cabeça, se estava vazia).

# Thread 2: Algoritmo centralizado
def thread_algoritmo() -> None:
    while executando.is_set():
        if not seletor.get_map():   # Sem sockets registrados, select() daria erro no Windows; espera chegar algum cliente.
            time.sleep(0.5)
            continue
        eventos = seletor.select(timeout=0.5)
        for chave, _ in eventos:
            pid_origem = chave.data
            sock = chave.fileobj
            dados = receber_completo(sock)
            if dados is None:
                tratar_desconexao(pid_origem, sock)
                continue

            id_msg, pid = parsear(dados)

            if id_msg == MSG_REQUEST:
                tratar_request(pid)
            elif id_msg == MSG_RELEASE:
                tratar_release(pid)
            else:
                log(f"Mensagem desconhecida id={id_msg} de PID {pid}")

def tratar_request(pid: int) -> None:
    """REQUEST: se a fila Q estava vazia, o pid vira titular e recebe GRANT; em seguida entra na fila."""
    with estado_lock:
        log(f"RECEBIDO REQUEST <- PID {pid}")
        if not fila_pedidos:
            enviar_grant(pid)
        fila_pedidos.append(pid)

def tratar_release(pid: int) -> None:
    """RELEASE: remove o titular (cabeça da fila Q) e passa o GRANT para o próximo, se houver."""
    with estado_lock:
        log(f"RECEBIDO RELEASE <- PID {pid}")
        atendidos[pid] = atendidos.get(pid, 0) + 1
        if fila_pedidos:
            fila_pedidos.popleft()          # Remove o titular que acabou de liberar (Q.remove()).
        if fila_pedidos:
            enviar_grant(fila_pedidos[0])   # O novo titular é a nova cabeça da fila (Q.head()).

def tratar_desconexao(pid: int, sock: socket.socket) -> None:
    """Remove cliente caído das estruturas. Se ele era o titular (cabeça da fila), passa o GRANT adiante para não travar o sistema."""
    with estado_lock:
        log(f"DESCONEXÃO    <- PID {pid}")
        try:
            seletor.unregister(sock)
        except (KeyError, ValueError):
            pass
        try:
            sock.close()
        except OSError:
            pass
        clientes.pop(pid, None)

        era_titular = bool(fila_pedidos) and fila_pedidos[0] == pid
        if pid in fila_pedidos:
            fila_pedidos.remove(pid)

        if era_titular and fila_pedidos:
            enviar_grant(fila_pedidos[0])


# Thread 3: Interface
def thread_interface() -> None:
    print("Interface do coordenador ativa.")
    print("Comandos: fila | atendidos | sair")
    while executando.is_set():
        try:
            cmd = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            cmd = "sair"

        if cmd == "fila":
            with estado_lock:
                titular = fila_pedidos[0] if fila_pedidos else None
                print(f"  titular atual: {titular}")
                print(f"  fila de espera: {list(fila_pedidos)[1:]}")
        elif cmd == "atendidos":
            print(f"  atendimentos por PID: {dict(atendidos)}")
        elif cmd == "sair":
            print("Encerrando coordenador...")
            executando.clear()
            break
        elif cmd == "":
            continue
        else:
            print(f"  comando desconhecido: '{cmd}'")

def main() -> None:
    configurar_log()

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((HOST, PORT))
    servidor.listen()
    print(f"Coordenador escutando em {HOST}:{PORT}")
    log(f"Coordenador iniciado em {HOST}:{PORT}")

    t_aceitar = threading.Thread(target=thread_aceitar, args=(servidor,),
                                 name="aceitar", daemon=True)
    t_algoritmo = threading.Thread(target=thread_algoritmo,
                                   name="algoritmo", daemon=True)
    t_aceitar.start()
    t_algoritmo.start()

    try:
        thread_interface()
    finally:    
        with estado_lock:
            for sock in list(clientes.values()):
                try:
                    sock.close()
                except OSError:
                    pass
        try:
            servidor.close()
        except OSError:
            pass

if __name__ == "__main__":
    main()
