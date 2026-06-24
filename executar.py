"""
Lançador do experimento: cria n processos com multiprocessing, cada um executando run_processo(pid, r, k).
"""

import argparse
from pathlib import Path
from multiprocessing import Process
from processo import run_processo
from validar import validar_log, validar_resultado

ARQUIVO_RESULTADO = "resultado.txt"

def run_experimento(n: int, r: int, k: float) -> None:
    Path(ARQUIVO_RESULTADO).write_text("", encoding="utf-8")

    print(f"=== Experimento: n={n} processos, r={r} repetições, k={k}s ===")
    print("(certifique-se que o coordenador.py já está rodando)\n")

    procs = [
        Process(target=run_processo, args=(pid, r, k), name=f"P{pid}")
        for pid in range(1, n + 1)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    print("\n=== Todos os processos terminaram ===")

    validar_resultado(n, r)
    validar_log()

def main() -> None:
    parser = argparse.ArgumentParser(description="Experimento de exclusão mútua")
    parser.add_argument("--n", type=int, default=5, help="número de processos")
    parser.add_argument("--r", type=int, default=3, help="repetições por processo")
    parser.add_argument("--k", type=float, default=1.0, help="sleep antes do REQUEST (segundos)")
    args = parser.parse_args()
    run_experimento(args.n, args.r, args.k)

if __name__ == "__main__":
    main()
