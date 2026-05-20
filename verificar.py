"""Verificador de corretude do experimento.

Checa:
  (a) resultado.txt tem exatamente n*r linhas;
  (b) timestamps fisicos sao monotonicamente nao-decrescentes;
  (c) timestamps de Lamport sao monotonicamente crescentes (consistencia causal);
  (d) cada pid aparece exatamente r vezes;
  (e) no coordenador.log, cada GRANT e seguido por RELEASE do mesmo pid antes
      do proximo GRANT.

Imprime "OK" se tudo passar; caso contrario, lista os problemas e sai com codigo 1.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime


def parse_resultado(caminho: str) -> list[tuple[int, datetime, int]]:
    linhas = []
    with open(caminho, "r", encoding="utf-8") as f:
        for n_lin, raw in enumerate(f, start=1):
            raw = raw.rstrip("\n")
            if not raw:
                continue
            partes = raw.split("|")
            if len(partes) != 3:
                raise ValueError(f"resultado.txt linha {n_lin}: formato invalido: {raw!r}")
            pid = int(partes[0])
            ts = datetime.fromisoformat(partes[1])
            lamport = int(partes[2])
            linhas.append((pid, ts, lamport))
    return linhas


def parse_log(caminho: str) -> list[tuple[datetime, int, str, str, int, int]]:
    eventos = []
    with open(caminho, "r", encoding="utf-8") as f:
        for n_lin, raw in enumerate(f, start=1):
            raw = raw.rstrip("\n")
            if not raw:
                continue
            partes = raw.split("|")
            if len(partes) != 6:
                raise ValueError(f"log linha {n_lin}: formato invalido: {raw!r}")
            ts = datetime.fromisoformat(partes[0])
            lc = int(partes[1])
            direcao = partes[2]
            tipo = partes[3]
            pid = int(partes[4])
            lamp = int(partes[5])
            eventos.append((ts, lc, direcao, tipo, pid, lamp))
    return eventos


def verificar(resultado: str, log: str, n: int, r: int) -> list[str]:
    problemas: list[str] = []

    # ---- resultado.txt ----
    linhas = parse_resultado(resultado)
    esperado = n * r
    if len(linhas) != esperado:
        problemas.append(f"resultado.txt tem {len(linhas)} linhas; esperado {esperado}")

    # monotonicidade fisica
    for i in range(1, len(linhas)):
        if linhas[i][1] < linhas[i - 1][1]:
            problemas.append(
                f"resultado.txt: timestamp fisico decrescente na linha {i + 1} "
                f"({linhas[i - 1][1]} -> {linhas[i][1]})"
            )
            break

    # monotonicidade de Lamport (estrita, pois cada entrada na RC e um evento novo)
    for i in range(1, len(linhas)):
        if linhas[i][2] <= linhas[i - 1][2]:
            problemas.append(
                f"resultado.txt: Lamport nao-crescente na linha {i + 1} "
                f"({linhas[i - 1][2]} -> {linhas[i][2]})"
            )
            break

    # contagem por pid
    cont = Counter(p for p, _, _ in linhas)
    for pid in range(n):
        if cont.get(pid, 0) != r:
            problemas.append(
                f"resultado.txt: pid={pid} aparece {cont.get(pid, 0)} vezes; esperado {r}"
            )

    # pids fora da faixa
    fora = [pid for pid in cont if pid < 0 or pid >= n]
    if fora:
        problemas.append(f"resultado.txt: pids fora da faixa [0, {n}): {sorted(fora)}")

    # ---- coordenador.log ----
    eventos = parse_log(log)
    grants = [e for e in eventos if e[3] == "GRANT" and e[2] == "SEND"]
    if len(grants) != esperado:
        problemas.append(
            f"log: {len(grants)} GRANTs enviados; esperado {esperado}"
        )

    # cada GRANT a pid X deve ser seguido por RELEASE de X antes do proximo GRANT.
    pid_atual: int | None = None
    for ts, lc, direcao, tipo, pid, lamp in eventos:
        if direcao == "SEND" and tipo == "GRANT":
            if pid_atual is not None:
                problemas.append(
                    f"log: novo GRANT a pid={pid} sem RELEASE anterior de pid={pid_atual}"
                )
            pid_atual = pid
        elif direcao == "RECV" and tipo == "RELEASE":
            if pid_atual is None:
                problemas.append(f"log: RELEASE de pid={pid} sem GRANT prévio")
            elif pid_atual != pid:
                problemas.append(
                    f"log: RELEASE de pid={pid} esperava pid={pid_atual}"
                )
                pid_atual = None
            else:
                pid_atual = None
    if pid_atual is not None:
        problemas.append(f"log: terminou sem RELEASE final do pid={pid_atual}")

    # Lamport do coordenador deve ser nao-decrescente no log.
    for i in range(1, len(eventos)):
        if eventos[i][1] < eventos[i - 1][1]:
            problemas.append(
                f"log linha {i + 1}: Lamport do coordenador decrescente "
                f"({eventos[i - 1][1]} -> {eventos[i][1]})"
            )
            break

    return problemas


def main() -> None:
    parser = argparse.ArgumentParser(description="Verifica corretude do experimento.")
    parser.add_argument("--resultado", default="resultado.txt")
    parser.add_argument("--log", default="coordenador.log")
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--r", type=int, required=True)
    args = parser.parse_args()

    problemas = verificar(args.resultado, args.log, args.n, args.r)
    if problemas:
        print("FALHA:")
        for p in problemas:
            print(f"  - {p}")
        sys.exit(1)
    print(f"OK: {args.n * args.r} entradas validas, log consistente.")


if __name__ == "__main__":
    main()
