# launch.py — Lança n processos de exclusão mútua simultaneamente
#
# Responsabilidades:
#   - Remove resultado.txt existente (execução limpa)
#   - Inicia n processos (process.py) sem retardo
#   - Aguarda todos terminarem
#   - Imprime resumo
#
# Uso:
#   python launch.py --n 3 --r 5 --k 1 --host 127.0.0.1 --port 5000

import argparse
import os
import subprocess
import sys
import time


def parse_args():
    parser = argparse.ArgumentParser(description="Lançador de processos para exclusão mútua centralizada")
    parser.add_argument("--n",    type=int,   default=3,         help="Número de processos (padrão: 3)")
    parser.add_argument("--r",    type=int,   default=5,         help="Repetições por processo (padrão: 5)")
    parser.add_argument("--k",    type=float, default=1.0,       help="Segundos de sleep pós RC por processo (padrão: 1.0)")
    parser.add_argument("--host", type=str,   default="127.0.0.1", help="Endereço IP do coordenador (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int,   default=5000,      help="Porta do coordenador (padrão: 5000)")
    return parser.parse_args()


def main():
    args = parse_args()

    resultado_path = "resultado.txt"
    if os.path.exists(resultado_path):
        os.remove(resultado_path)
        print(f"[launcher] Arquivo '{resultado_path}' removido para execução limpa.")

    python_exec = sys.executable  # usa o mesmo interpretador Python do launcher
    process_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.py")

    processes = []
    print(f"[launcher] Iniciando {args.n} processo(s) com r={args.r}, k={args.k}...")

    for i in range(1, args.n + 1):
        cmd = [
            python_exec, process_script,
            "--id",   str(i),
            "--host", args.host,
            "--port", str(args.port),
            "--r",    str(args.r),
            "--k",    str(args.k),
        ]
        proc = subprocess.Popen(cmd)
        processes.append((i, proc))

    print(f"[launcher] Todos os {args.n} processo(s) iniciados. Aguardando conclusão...")

    for proc_id, proc in processes:
        proc.wait()
        print(f"[launcher] Processo P{proc_id} finalizado (código: {proc.returncode})")

    print(f"\n[launcher] Execução concluída. Verifique '{resultado_path}' (esperado: {args.n * args.r} linhas).")


if __name__ == "__main__":
    main()
