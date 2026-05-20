"""Coordenador multi-threaded para exclusao mutua centralizada.

Threads:
  1. accept_loop      - aceita novas conexoes TCP.
  2. handle_client    - uma por cliente, le mensagens (REQUEST/RELEASE).
  3. mutex_loop       - serializa o acesso a regiao critica (envia GRANT, espera RELEASE).
  4. interface_loop   - terminal interativo (1=fila, 2=atendidos, 3=encerrar).

Tambem mantem um LamportClock proprio, atualizado a cada evento de envio/recebimento.
"""

from __future__ import annotations

import argparse
import socket
import sys
import threading
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional

from protocolo import (
    F,
    GRANT,
    RELEASE,
    REQUEST,
    TIPO_NOME,
    LamportClock,
    decode,
    encode,
    recv_exato,
)


# ---------------------------------------------------------------------------
# Estado global do coordenador
# ---------------------------------------------------------------------------

clock = LamportClock()

clientes: dict[int, socket.socket] = {}        # pid -> socket
clientes_lock = threading.Lock()

fila: deque[int] = deque()                      # fila FIFO de pids aguardando GRANT
fila_cond = threading.Condition()               # protege fila e sinaliza chegadas

release_event = threading.Event()               # sinaliza chegada do RELEASE esperado
release_pid_esperado: Optional[int] = None      # pid do qual aguardamos RELEASE
release_lock = threading.Lock()                 # protege release_pid_esperado

atendidos: dict[int, int] = defaultdict(int)    # pid -> quantidade de GRANTs entregues
atendidos_lock = threading.Lock()

log_lock = threading.Lock()
log_file = None                                  # type: ignore[assignment]

shutdown = threading.Event()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log_evento(direcao: str, tipo: int, pid: int, lamport_msg: int, lamport_coord: int) -> None:
    """Escreve uma linha no log do coordenador.

    Formato: <ts_fisico_ms>|<lamport_coord>|<direcao>|<tipo>|<pid>|<lamport_msg>
    """
    ts = datetime.now().isoformat(timespec="milliseconds")
    linha = f"{ts}|{lamport_coord}|{direcao}|{TIPO_NOME[tipo]}|{pid}|{lamport_msg}\n"
    with log_lock:
        log_file.write(linha)
        log_file.flush()


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------

def handle_client(sock: socket.socket, addr) -> None:
    """Le mensagens de um cliente ate ele desconectar."""
    pid_registrado: Optional[int] = None
    try:
        while not shutdown.is_set():
            buf = recv_exato(sock, F)
            if not buf:
                break  # cliente fechou conexao
            try:
                msg_id, pid, lamport_msg = decode(buf)
            except ValueError as e:
                print(f"[coord] erro decodificando de {addr}: {e}", file=sys.stderr)
                break

            # Atualiza relogio logico do coordenador ao receber.
            lc = clock.update(lamport_msg)
            log_evento("RECV", msg_id, pid, lamport_msg, lc)

            if pid_registrado is None:
                pid_registrado = pid
                with clientes_lock:
                    clientes[pid] = sock

            if msg_id == REQUEST:
                with fila_cond:
                    fila.append(pid)
                    fila_cond.notify_all()
            elif msg_id == RELEASE:
                with release_lock:
                    esperado = release_pid_esperado
                if esperado == pid:
                    release_event.set()
                else:
                    print(
                        f"[coord] RELEASE inesperado de pid={pid} (esperado={esperado})",
                        file=sys.stderr,
                    )
            elif msg_id == GRANT:
                # Coordenador nao deve receber GRANT.
                print(f"[coord] GRANT recebido (inesperado) de pid={pid}", file=sys.stderr)
    except OSError:
        pass
    finally:
        try:
            sock.close()
        except OSError:
            pass
        if pid_registrado is not None:
            with clientes_lock:
                clientes.pop(pid_registrado, None)


def accept_loop(server_sock: socket.socket) -> None:
    """Aceita novas conexoes e dispara uma thread receptora para cada cliente."""
    while not shutdown.is_set():
        try:
            sock, addr = server_sock.accept()
        except OSError:
            break
        t = threading.Thread(target=handle_client, args=(sock, addr), daemon=True)
        t.start()


def mutex_loop() -> None:
    """Loop do algoritmo centralizado: GRANT -> espera RELEASE -> repete."""
    global release_pid_esperado
    while not shutdown.is_set():
        # 1) aguarda alguem na fila
        with fila_cond:
            while not fila and not shutdown.is_set():
                fila_cond.wait(timeout=0.5)
            if shutdown.is_set():
                return
            pid = fila.popleft()

        # 2) localiza o socket
        with clientes_lock:
            sock = clientes.get(pid)
        if sock is None:
            # cliente desconectou antes de receber GRANT; ignora e segue.
            print(f"[coord] pid={pid} sem socket; pulando", file=sys.stderr)
            continue

        # 3) prepara espera por RELEASE deste pid
        with release_lock:
            release_pid_esperado = pid
        release_event.clear()

        # 4) envia GRANT
        lc = clock.tick()
        try:
            sock.sendall(encode(GRANT, pid, lc))
        except OSError as e:
            print(f"[coord] erro enviando GRANT a pid={pid}: {e}", file=sys.stderr)
            with release_lock:
                release_pid_esperado = None
            continue
        log_evento("SEND", GRANT, pid, lc, lc)
        with atendidos_lock:
            atendidos[pid] += 1

        # 5) aguarda RELEASE correspondente
        while not shutdown.is_set():
            if release_event.wait(timeout=0.5):
                break
        with release_lock:
            release_pid_esperado = None


def interface_loop() -> None:
    """Terminal interativo do coordenador."""
    ajuda = "[1] mostrar fila  [2] mostrar atendidos  [3] encerrar"
    print(ajuda)
    while not shutdown.is_set():
        try:
            cmd = input("> ").strip()
        except EOFError:
            return
        if cmd == "1":
            with fila_cond:
                snapshot = list(fila)
            print(f"fila: {snapshot}")
        elif cmd == "2":
            with atendidos_lock:
                snapshot = dict(atendidos)
            print(f"atendidos: {snapshot}")
        elif cmd == "3":
            print("[coord] encerrando...")
            shutdown.set()
            return
        else:
            print(ajuda)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    global log_file

    parser = argparse.ArgumentParser(description="Coordenador de exclusao mutua centralizada.")
    parser.add_argument("--host", default="0.0.0.0", help="endereco de bind")
    parser.add_argument("--porta", type=int, default=5000, help="porta TCP")
    parser.add_argument("--log", default="coordenador.log", help="arquivo de log")
    args = parser.parse_args()

    log_file = open(args.log, "w", encoding="utf-8", buffering=1)

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((args.host, args.porta))
    server_sock.listen(128)
    print(f"[coord] escutando em {args.host}:{args.porta}")

    t_accept = threading.Thread(target=accept_loop, args=(server_sock,), daemon=True)
    t_mutex = threading.Thread(target=mutex_loop, daemon=True)
    t_ui = threading.Thread(target=interface_loop, daemon=True)
    t_accept.start()
    t_mutex.start()
    t_ui.start()

    try:
        # Espera shutdown via interface ou Ctrl+C.
        while not shutdown.is_set():
            shutdown.wait(timeout=0.5)
    except KeyboardInterrupt:
        shutdown.set()
    finally:
        try:
            server_sock.close()
        except OSError:
            pass
        # Acorda o mutex_loop, se estiver aguardando na fila vazia.
        with fila_cond:
            fila_cond.notify_all()
        # Fecha sockets de clientes.
        with clientes_lock:
            for s in list(clientes.values()):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass
            clientes.clear()
        if log_file is not None:
            log_file.close()
        print("[coord] encerrado.")


if __name__ == "__main__":
    main()
