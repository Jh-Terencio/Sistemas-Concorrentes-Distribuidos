"""
Script de validação automática do experimento.
"""

import re
import argparse
from pathlib import Path
from datetime import datetime
from collections import Counter

ARQUIVO_RESULTADO = "resultado.txt"
ARQUIVO_LOG = "logs/coordenador.log"

def validar_resultado(n: int, r: int) -> bool:
    """Confere nº de linhas, ordem cronológica e contagem por PID."""
    print("\n=== Validando resultado.txt ===")
    caminho = Path(ARQUIVO_RESULTADO)
    if not caminho.exists():
        print(f"  FALHA: {caminho} não existe.")
        return False

    linhas = caminho.read_text(encoding="utf-8").splitlines()
    esperado = n * r
    ok = True

    # Numero de linhas
    if len(linhas) == esperado:
        print(f"OK: {len(linhas)} linhas (esperado n*r = {esperado}).")
    else:
        print(f"FALHA: {len(linhas)} linhas, esperado {esperado}.")
        ok = False

    # Ordem cronológica e contagem por PID
    contagem = Counter()
    ultimo_ts = None
    for i, linha in enumerate(linhas, start=1):
        m = re.match(r"PID (\d+) \| (\d{2}:\d{2}:\d{2}\.\d+)", linha)
        if not m:
            print(f"FALHA: linha {i} mal-formada: {linha!r}")
            ok = False
            continue
        pid = int(m.group(1))
        ts = datetime.strptime(m.group(2), "%H:%M:%S.%f")
        contagem[pid] += 1
        if ultimo_ts is not None and ts < ultimo_ts:
            print(f"FALHA: timestamp fora de ordem na linha {i}: {linha}")
            ok = False
        ultimo_ts = ts

    if ok:
        print("OK: ordem cronológica respeitada.")

    print(f"Execuções por PID: {dict(sorted(contagem.items()))}")
    for pid, c in contagem.items():
        if c != r:
            print(f"FALHA: PID {pid} executou {c} vezes, esperado {r}.")
            ok = False
    if all(c == r for c in contagem.values()) and len(contagem) == n:
        print(f"OK: cada um dos {n} processos executou exatamente {r} vezes.")

    return ok

def validar_log() -> bool:
    """Confere os invariantes de exclusão mútua a partir do log do coordenador."""
    print("\n=== Validando logs/coordenador.log ===")
    caminho = Path(ARQUIVO_LOG)
    if not caminho.exists():
        print(f"AVISO: {caminho} não existe — pulando validação do log.")
        return True

    # Procuramos só as linhas de evento do protocolo.
    padrao = re.compile(
        r"(RECEBIDO REQUEST|ENVIADO GRANT|RECEBIDO RELEASE).*?PID (\d+)"
    )
    eventos = []
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        m = padrao.search(linha)
        if m:
            eventos.append((m.group(1), int(m.group(2))))

    ok = True
    titular = None
    pids_grant_em_ordem = []
    pids_release_em_ordem = []

    for evento, pid in eventos:
        if evento == "ENVIADO GRANT":
            if titular is not None:
                print(f"FALHA: GRANT para PID {pid} mas titular era {titular} "
                      "(GRANT sem RELEASE intermediário).")
                ok = False
            titular = pid
            pids_grant_em_ordem.append(pid)
        elif evento == "RECEBIDO RELEASE":
            if titular != pid:
                print(f"FALHA: RELEASE de PID {pid} mas titular era {titular}.")
                ok = False
            titular = None
            pids_release_em_ordem.append(pid)

    if ok:
        print(f"OK: {len(pids_grant_em_ordem)} GRANT/RELEASE intercalados corretamente.")

    if pids_grant_em_ordem == pids_release_em_ordem:
        print("OK: ordem dos GRANT == ordem dos RELEASE.")
    else:
        print("FALHA: ordem dos GRANT diferente da ordem dos RELEASE.")
        ok = False

    return ok


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, required=True, help="número de processos")
    parser.add_argument("--r", type=int, required=True, help="repetições por processo")
    args = parser.parse_args()

    r1 = validar_resultado(args.n, args.r)
    r2 = validar_log()
    print()
    if r1 and r2:
        print(">>> TODAS AS VALIDAÇÕES PASSARAM <<<")
    else:
        print(">>> HOUVE FALHAS — ver mensagens acima <<<")


if __name__ == "__main__":
    main()
