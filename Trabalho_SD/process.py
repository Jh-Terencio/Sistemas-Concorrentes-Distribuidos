# process.py — Processo cliente para exclusão mútua distribuída centralizada
#
# Comportamento:
#   1. Conecta ao coordenador e envia identificação (proc_id)
#   2. Repete r vezes:
#      a. Envia REQUEST ao coordenador
#      b. Aguarda GRANT
#      c. Abre resultado.txt em append, escreve proc_id + timestamp com ms, fecha
#      d. Envia RELEASE ao coordenador
#      e. Dorme k segundos
#   3. Encerra conexão
#
# Uso:
#   python process.py --id 1 --host 127.0.0.1 --port 5000 --r 5 --k 1

import argparse
import socket
import time
from datetime import datetime

from messages import (
    F, REQUEST, GRANT, RELEASE,
    encode, recv_message
)

RESULTADO_FILE = "resultado.txt"


def parse_args():
    parser = argparse.ArgumentParser(description="Processo cliente de exclusão mútua centralizada")
    parser.add_argument("--id",   type=int, required=True,             help="Identificador único do processo")
    parser.add_argument("--host", type=str, default="127.0.0.1",       help="Endereço IP do coordenador (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5000,              help="Porta do coordenador (padrão: 5000)")
    parser.add_argument("--r",    type=int, default=5,                 help="Número de repetições (padrão: 5)")
    parser.add_argument("--k",    type=float, default=1.0,             help="Segundos de sleep após região crítica (padrão: 1.0)")
    return parser.parse_args()


def main():
    args = parse_args()
    proc_id = args.id
    msg_id = 0

    def next_mid():
        nonlocal msg_id
        msg_id += 1
        return msg_id

    # Conecta ao coordenador
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.port))
    print(f"[P{proc_id}] Conectado ao coordenador {args.host}:{args.port}")

    # Handshake: envia identificação (REQUEST tipo especial com msg_id=0 serve como ident)
    # Usamos REQUEST com msg_id=0 para identificação — o coordenador espera uma mensagem
    # com proc_id neste primeiro pacote.
    ident = encode(REQUEST, proc_id, 0)
    sock.sendall(ident)
    print(f"[P{proc_id}] Identificação enviada")

    for i in range(1, args.r + 1):
        # --- Solicita RC ---
        mid = next_mid()
        sock.sendall(encode(REQUEST, proc_id, mid))
        print(f"[P{proc_id}] REQUEST enviado (repetição {i}/{args.r})")

        # --- Aguarda GRANT ---
        msg_type, _, _ = recv_message(sock)
        if msg_type != GRANT:
            print(f"[P{proc_id}] Esperava GRANT, recebi tipo {msg_type}. Abortando.")
            break
        print(f"[P{proc_id}] GRANT recebido — entrando na região crítica")

        # --- Região Crítica ---
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(RESULTADO_FILE, "a", encoding="utf-8") as f:
            f.write(f"{proc_id},{timestamp}\n")
        print(f"[P{proc_id}] Escreveu em resultado.txt: {proc_id},{timestamp}")

        # --- Libera RC ---
        mid = next_mid()
        sock.sendall(encode(RELEASE, proc_id, mid))
        print(f"[P{proc_id}] RELEASE enviado")

        # --- Sleep fora da RC ---
        time.sleep(args.k)

    sock.close()
    print(f"[P{proc_id}] Finalizado após {args.r} repetição(ões).")


if __name__ == "__main__":
    main()
