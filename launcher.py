"""Launcher: dispara n instancias de processo.py em paralelo via subprocess.Popen.

Pre-condicao: o coordenador ja deve estar rodando no host/porta indicados.
"""

import argparse
import os
import subprocess
import sys
import time


def main() -> None:
    parser = argparse.ArgumentParser(description="Launcher dos n processos clientes.")
    parser.add_argument("--n", type=int, required=True, help="numero de processos")
    parser.add_argument("--k", type=float, required=True, help="tempo (s) dentro da RC")
    parser.add_argument("--r", type=int, required=True, help="requisicoes por processo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--porta", type=int, default=5000)
    parser.add_argument("--resultado", default="resultado.txt")
    parser.add_argument(
        "--limpar",
        action="store_true",
        help="apaga resultado.txt antes de iniciar",
    )
    parser.add_argument("--sleep-min", type=float, default=3.0)
    parser.add_argument("--sleep-max", type=float, default=4.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="interpretador Python a usar (default: o atual)",
    )
    args = parser.parse_args()

    if args.limpar:
        try:
            os.remove(args.resultado)
        except FileNotFoundError:
            pass

    base_dir = os.path.dirname(os.path.abspath(__file__))
    processo_py = os.path.join(base_dir, "processo.py")

    procs: list[subprocess.Popen] = []
    t0 = time.time()
    for i in range(args.n):
        cmd = [
            args.python,
            processo_py,
            "--pid", str(i),
            "--host", args.host,
            "--porta", str(args.porta),
            "--k", str(args.k),
            "--r", str(args.r),
            "--resultado", args.resultado,
            "--sleep-min", str(args.sleep_min),
            "--sleep-max", str(args.sleep_max),
        ]
        if args.seed is not None:
            cmd += ["--seed", str(args.seed)]
        procs.append(subprocess.Popen(cmd))

    print(f"[launcher] {args.n} processos iniciados; aguardando termino...")
    rc = 0
    for p in procs:
        ret = p.wait()
        if ret != 0:
            rc = ret
    elapsed = time.time() - t0
    print(f"[launcher] todos finalizados em {elapsed:.2f}s (rc={rc})")
    sys.exit(rc)


if __name__ == "__main__":
    main()
