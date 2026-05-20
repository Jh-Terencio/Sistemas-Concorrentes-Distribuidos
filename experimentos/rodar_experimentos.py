"""Executa a matriz de experimentos descrita no plan.md.

Para cada cenario:
  1. Limpa resultado.txt e coordenador.log no diretorio do cenario.
  2. Sobe o coordenador (em subprocesso).
  3. Roda o launcher e aguarda termino.
  4. Encerra o coordenador (envia '3' via stdin).
  5. Coleta metricas: tempo total, throughput (RC/s), tamanho medio da fila,
     justica (variancia de atendidos[pid]) e drift Lamport vs. fisico.
  6. Move artefatos para experimentos/resultados/<cenario>/.

Uso:
  python experimentos/rodar_experimentos.py
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable
RESULTADOS_DIR = os.path.join(BASE_DIR, "experimentos", "resultados")


def porta_livre() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    porta = s.getsockname()[1]
    s.close()
    return porta


def aguardar_porta(host: str, porta: int, timeout: float = 5.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection((host, porta), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def coletar_metricas(resultado_path: str, log_path: str, n: int, r: int,
                     tempo_total: float) -> dict:
    # resultado.txt
    pids = []
    lamports = []
    timestamps_fis = []
    with open(resultado_path, encoding="utf-8") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            pid, ts, lamp = raw.split("|")
            pids.append(int(pid))
            lamports.append(int(lamp))
            timestamps_fis.append(datetime.fromisoformat(ts))

    cont = Counter(pids)
    atendidos = [cont.get(i, 0) for i in range(n)]
    variancia_just = statistics.pvariance(atendidos) if atendidos else 0.0

    # coordenador.log -> tamanho da fila ao longo do tempo
    # tamanho_fila[i] = numero de REQUESTs recebidos ate o evento i menos GRANTs enviados
    fila_tamanhos: list[int] = []
    pendentes = 0
    with open(log_path, encoding="utf-8") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            partes = raw.split("|")
            if len(partes) != 6:
                continue
            direcao, tipo = partes[2], partes[3]
            if direcao == "RECV" and tipo == "REQUEST":
                pendentes += 1
                fila_tamanhos.append(pendentes)
            elif direcao == "SEND" and tipo == "GRANT":
                pendentes = max(0, pendentes - 1)
                fila_tamanhos.append(pendentes)
    tamanho_medio_fila = statistics.mean(fila_tamanhos) if fila_tamanhos else 0.0
    tamanho_max_fila = max(fila_tamanhos) if fila_tamanhos else 0

    # Drift Lamport vs fisico: correlacao simples ordinal sobre as entradas de resultado.txt
    if len(lamports) >= 2:
        epoch = [t.timestamp() for t in timestamps_fis]
        # normaliza
        e_min, e_max = min(epoch), max(epoch)
        l_min, l_max = min(lamports), max(lamports)
        if e_max - e_min > 0 and l_max - l_min > 0:
            # diferenca media absoluta entre rank-normalizados
            e_norm = [(x - e_min) / (e_max - e_min) for x in epoch]
            l_norm = [(x - l_min) / (l_max - l_min) for x in lamports]
            drift = statistics.mean(abs(a - b) for a, b in zip(e_norm, l_norm))
        else:
            drift = 0.0
    else:
        drift = 0.0

    rcs = n * r
    throughput = rcs / tempo_total if tempo_total > 0 else 0.0

    return {
        "n": n,
        "r": r,
        "tempo_total_s": round(tempo_total, 3),
        "rcs_executadas": rcs,
        "throughput_rcs_por_s": round(throughput, 3),
        "tamanho_medio_fila": round(tamanho_medio_fila, 3),
        "tamanho_max_fila": tamanho_max_fila,
        "atendidos_por_pid": atendidos,
        "variancia_justica": round(variancia_just, 4),
        "lamport_min": min(lamports) if lamports else None,
        "lamport_max": max(lamports) if lamports else None,
        "drift_lamport_vs_fisico": round(drift, 4),
    }


def rodar_cenario(nome: str, n: int, k: float, r: int,
                  sleep_min: float, sleep_max: float, seed: int | None) -> dict:
    destino = os.path.join(RESULTADOS_DIR, nome)
    os.makedirs(destino, exist_ok=True)

    resultado_path = os.path.join(destino, "resultado.txt")
    log_path = os.path.join(destino, "coordenador.log")

    # limpa artefatos antigos
    for p in (resultado_path, log_path):
        if os.path.exists(p):
            os.remove(p)

    porta = porta_livre()

    coord_cmd = [
        PYTHON,
        os.path.join(BASE_DIR, "coordenador.py"),
        "--host", "127.0.0.1",
        "--porta", str(porta),
        "--log", log_path,
    ]
    coord = subprocess.Popen(
        coord_cmd,
        cwd=BASE_DIR,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    if not aguardar_porta("127.0.0.1", porta, timeout=5.0):
        coord.kill()
        raise RuntimeError(f"coordenador nao subiu (porta={porta})")

    launcher_cmd = [
        PYTHON,
        os.path.join(BASE_DIR, "launcher.py"),
        "--n", str(n),
        "--k", str(k),
        "--r", str(r),
        "--host", "127.0.0.1",
        "--porta", str(porta),
        "--resultado", resultado_path,
        "--sleep-min", str(sleep_min),
        "--sleep-max", str(sleep_max),
    ]
    if seed is not None:
        launcher_cmd += ["--seed", str(seed)]

    t0 = time.time()
    try:
        rc = subprocess.call(launcher_cmd, cwd=BASE_DIR)
    finally:
        tempo_total = time.time() - t0

    # encerra coordenador via stdin (comando '3')
    try:
        coord.stdin.write("3\n")
        coord.stdin.flush()
    except (BrokenPipeError, OSError):
        pass
    try:
        coord.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        coord.terminate()
        coord.wait(timeout=2.0)

    if rc != 0:
        raise RuntimeError(f"launcher falhou (rc={rc})")

    metricas = coletar_metricas(resultado_path, log_path, n, r, tempo_total)
    metricas["nome"] = nome
    metricas["k"] = k
    metricas["sleep_min"] = sleep_min
    metricas["sleep_max"] = sleep_max
    with open(os.path.join(destino, "metricas.json"), "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)
    return metricas


def main() -> None:
    parser = argparse.ArgumentParser(description="Matriz de experimentos.")
    parser.add_argument(
        "--rapido",
        action="store_true",
        help="usa sleep-min/max=0.1/0.3 para validar rapidamente (NAO e o cenario do trabalho)",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.rapido:
        sleep_min, sleep_max = 0.1, 0.3
    else:
        sleep_min, sleep_max = 3.0, 4.0

    cenarios: list[tuple[str, int, float, int]] = []
    # variar n
    for n in (2, 4, 8, 16):
        cenarios.append((f"n{n}_k0.1_r5", n, 0.1, 5))
    # variar k
    for k in (0.0, 0.05, 0.2, 0.5):
        cenarios.append((f"n4_k{k}_r5", 4, k, 5))
    # variar r
    for r in (3, 5, 10):
        cenarios.append((f"n4_k0.1_r{r}", 4, 0.1, r))

    resumos = []
    for nome, n, k, r in cenarios:
        print(f"\n=== Cenario {nome} (n={n}, k={k}, r={r}) ===")
        m = rodar_cenario(nome, n, k, r, sleep_min, sleep_max, args.seed)
        print(json.dumps(m, indent=2, ensure_ascii=False))
        resumos.append(m)

    resumo_path = os.path.join(RESULTADOS_DIR, "resumo.json")
    with open(resumo_path, "w", encoding="utf-8") as f:
        json.dump(resumos, f, indent=2, ensure_ascii=False)
    print(f"\nResumo salvo em {resumo_path}")


if __name__ == "__main__":
    main()
