"""Processo cliente do algoritmo centralizado de exclusao mutua.

Cada processo:
  1. Conecta TCP ao coordenador.
  2. Em loop r vezes:
     - dorme random.uniform(3, 4) segundos FORA da regiao critica;
     - envia REQUEST (com Lamport.tick());
     - aguarda GRANT (atualiza Lamport com o valor recebido);
     - entra na RC: escreve uma linha em resultado.txt e dorme k segundos;
     - envia RELEASE (com Lamport.tick()).
  3. Fecha o socket.
"""

import argparse
import os
import random
import socket
import sys
import time
from datetime import datetime

from protocolo import (
    F,
    GRANT,
    RELEASE,
    REQUEST,
    LamportClock,
    decode,
    encode,
    recv_exato,
)


# Lock global de arquivo entre processos seria inviavel sem auxilio do SO.
# Aqui a exclusao mutua garantida pelo coordenador faz com que apenas UM
# processo escreva por vez em resultado.txt, entao um append simples basta.
def escrever_resultado(caminho: str, pid: int, lamport: int) -> None:
    ts = datetime.now().isoformat(timespec="milliseconds")
    linha = f"{pid}|{ts}|{lamport}\n"
    with open(caminho, "a", encoding="utf-8") as f:
        f.write(linha)


def main() -> None:
    parser = argparse.ArgumentParser(description="Processo cliente de exclusao mutua.")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--k", type=float, required=True, help="tempo (s) dentro da RC")
    parser.add_argument("--r", type=int, required=True, help="numero de requisicoes")
    parser.add_argument("--resultado", default="resultado.txt")
    parser.add_argument("--sleep-min", type=float, default=3.0, help="min do sleep fora da RC")
    parser.add_argument("--sleep-max", type=float, default=4.0, help="max do sleep fora da RC")
    parser.add_argument("--seed", type=int, default=None, help="seed do random (debug)")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed + args.pid)
    else:
        # Mistura pid, pid do SO e tempo para garantir variabilidade entre processos.
        random.seed((args.pid * 1000003) ^ os.getpid() ^ time.time_ns())

    clock = LamportClock()

    # Conecta ao coordenador.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.porta))

    try:
        for i in range(args.r):
            # 1) sleep aleatorio FORA da RC
            time.sleep(random.uniform(args.sleep_min, args.sleep_max))

            # 2) envia REQUEST
            lc = clock.tick()
            sock.sendall(encode(REQUEST, args.pid, lc))

            # 3) aguarda GRANT
            buf = recv_exato(sock, F)
            if not buf:
                print(f"[p{args.pid}] conexao fechada antes do GRANT", file=sys.stderr)
                return
            msg_id, pid_g, lamport_g = decode(buf)
            if msg_id != GRANT:
                print(f"[p{args.pid}] esperava GRANT, recebeu {msg_id}", file=sys.stderr)
                return
            clock.update(lamport_g)

            # 4) regiao critica: escreve e dorme k
            escrever_resultado(args.resultado, args.pid, clock.value)
            time.sleep(args.k)

            # 5) envia RELEASE
            lc = clock.tick()
            sock.sendall(encode(RELEASE, args.pid, lc))
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()


if __name__ == "__main__":
    main()
