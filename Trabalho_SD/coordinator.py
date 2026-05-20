# coordinator.py — Coordenador multi-threaded de exclusão mútua distribuída centralizada
#
# Threads:
#   1. accept_thread   — aceita novas conexões de processos
#   2. algorithm_thread — executa o algoritmo de exclusão mútua (select + fila)
#   3. interface_thread — lê comandos do terminal
#
# Uso:
#   python coordinator.py --port 5000

import argparse
import collections
import select
import socket
import threading
import time
from datetime import datetime

from messages import (
    F, REQUEST, GRANT, RELEASE, MSG_NAMES,
    encode, recv_message
)

# ---------------------------------------------------------------------------
# Estado compartilhado
# ---------------------------------------------------------------------------
lock = threading.Lock()

clients: dict[int, socket.socket] = {}   # proc_id → socket
request_queue: collections.deque = collections.deque()
grant_counts: dict[int, int] = {}        # proc_id → nº de GRANTs concedidos
current_holder: int | None = None        # proc_id com a RC agora
msg_counter = 0                          # ID sequencial das mensagens enviadas

# Sockets monitorados pelo select (inclui o socket de listen para novos clientes)
# Gerenciado apenas pela accept_thread e lido pela algorithm_thread com o lock
sockets_list: list[socket.socket] = []

running = True  # flag de encerramento

# Arquivo de log
log_file = None
log_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _log(direction: str, msg_type: int, proc_id: int) -> None:
    line = f"{_now()}|{MSG_NAMES[msg_type]}|{direction}|P{proc_id}\n"
    with log_lock:
        log_file.write(line)
        log_file.flush()
    print(f"[LOG] {line.rstrip()}")


def _next_msg_id() -> int:
    global msg_counter
    msg_counter += 1
    return msg_counter


def _send_grant(proc_id: int) -> None:
    global current_holder
    mid = _next_msg_id()
    data = encode(GRANT, 0, mid)   # proc_id=0 significa "coordenador"
    clients[proc_id].sendall(data)
    grant_counts[proc_id] += 1
    current_holder = proc_id
    _log("OUT", GRANT, proc_id)


# ---------------------------------------------------------------------------
# Thread 1 — aceita conexões
# ---------------------------------------------------------------------------

def accept_thread(server_sock: socket.socket) -> None:
    global running
    print(f"[Coordenador] Aguardando conexões na porta {args.port}...")
    server_sock.settimeout(1.0)   # permite verificar o flag `running`
    while running:
        try:
            conn, addr = server_sock.accept()
        except socket.timeout:
            continue
        except OSError:
            break

        # Primeiro pacote: identificação do processo (proc_id)
        try:
            _, proc_id, _ = recv_message(conn)
        except Exception as e:
            print(f"[accept_thread] Erro ao receber identificação: {e}")
            conn.close()
            continue

        with lock:
            clients[proc_id] = conn
            grant_counts[proc_id] = 0
            sockets_list.append(conn)

        print(f"[Coordenador] Processo P{proc_id} conectado de {addr}")


# ---------------------------------------------------------------------------
# Thread 2 — algoritmo de exclusão mútua
# ---------------------------------------------------------------------------

def algorithm_thread() -> None:
    global current_holder, running

    while running:
        with lock:
            monitored = list(sockets_list)

        if not monitored:
            time.sleep(0.05)
            continue

        try:
            readable, _, exceptional = select.select(monitored, [], monitored, 0.1)
        except Exception:
            # Pode acontecer se um socket foi fechado; reconstruímos na próxima iteração
            continue

        for sock in exceptional:
            _remove_socket(sock)

        for sock in readable:
            try:
                msg_type, proc_id, msg_id = recv_message(sock)
            except ConnectionError:
                _remove_socket(sock)
                continue
            except Exception as e:
                print(f"[algorithm_thread] Erro ao receber mensagem: {e}")
                _remove_socket(sock)
                continue

            _log("IN", msg_type, proc_id)

            with lock:
                if msg_type == REQUEST:
                    if current_holder is None:
                        _send_grant(proc_id)
                    else:
                        request_queue.append(proc_id)
                        print(f"[Coordenador] P{proc_id} enfileirado. Fila: {list(request_queue)}")

                elif msg_type == RELEASE:
                    current_holder = None
                    if request_queue:
                        next_proc = request_queue.popleft()
                        _send_grant(next_proc)

                else:
                    print(f"[algorithm_thread] Tipo de mensagem desconhecido: {msg_type}")


def _remove_socket(sock: socket.socket) -> None:
    """Remove um socket desconectado das estruturas de dados."""
    with lock:
        if sock in sockets_list:
            sockets_list.remove(sock)
        # Identifica e remove o processo do dicionário
        to_remove = [pid for pid, s in clients.items() if s is sock]
        for pid in to_remove:
            del clients[pid]
            print(f"[Coordenador] Processo P{pid} desconectado")
    try:
        sock.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Thread 3 — interface de terminal
# ---------------------------------------------------------------------------

def interface_thread() -> None:
    global running
    print("\nComandos disponíveis:")
    print("  q — imprimir fila de pedidos atual")
    print("  c — imprimir contagem de GRANTs por processo")
    print("  x — encerrar o coordenador\n")

    while running:
        try:
            cmd = input(">> ").strip().lower()
        except EOFError:
            break

        if cmd == "q":
            with lock:
                fila = list(request_queue)
                holder = current_holder
            print(f"  Fila atual: {fila}")
            print(f"  Processo com RC: {f'P{holder}' if holder is not None else 'nenhum'}")

        elif cmd == "c":
            with lock:
                counts = dict(grant_counts)
            for pid, cnt in sorted(counts.items()):
                print(f"  P{pid}: {cnt} vez(es) atendido")

        elif cmd == "x":
            print("[Coordenador] Encerrando...")
            running = False
            break

        else:
            print("  Comando inválido. Use q, c ou x.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Coordenador de exclusão mútua centralizada")
    parser.add_argument("--port", type=int, default=5000, help="Porta TCP do coordenador (padrão: 5000)")
    parser.add_argument("--log", type=str, default="coordinator_log.txt", help="Arquivo de log (padrão: coordinator_log.txt)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    log_file = open(args.log, "w", encoding="utf-8")
    log_file.write(f"# Coordenador iniciado em {_now()}\n")
    log_file.write("# TIMESTAMP|TIPO|DIRECAO|PROCESSO\n")
    log_file.flush()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", args.port))
    server_sock.listen(50)

    t_accept = threading.Thread(target=accept_thread, args=(server_sock,), name="accept_thread", daemon=True)
    t_algo   = threading.Thread(target=algorithm_thread, name="algorithm_thread", daemon=True)
    t_iface  = threading.Thread(target=interface_thread, name="interface_thread")

    t_accept.start()
    t_algo.start()
    t_iface.start()

    t_iface.join()   # aguarda a thread de interface (bloqueia até 'x')

    running = False
    t_accept.join(timeout=2)
    t_algo.join(timeout=2)

    server_sock.close()
    log_file.close()
    print("[Coordenador] Encerrado.")
