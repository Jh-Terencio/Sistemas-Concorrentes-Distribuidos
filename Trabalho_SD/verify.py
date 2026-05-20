# verify.py — Verifica a corretude de resultado.txt e coordinator_log.txt
#
# Verificações em resultado.txt:
#   1. Total de linhas == n * r
#   2. Cada processo escreveu exatamente r vezes
#   3. Timestamps estão em ordem não-decrescente
#
# Verificações em coordinator_log.txt:
#   4. Depois de cada GRANT sempre há um RELEASE antes do próximo GRANT
#   5. A ordem dos proc_ids nos REQUESTs é a mesma dos RELEASEs
#
# Uso:
#   python verify.py --n 3 --r 5 --log coordinator_log.txt

import argparse
from datetime import datetime


RESULTADO_FILE = "resultado.txt"
PASS = "PASS"
FAIL = "FAIL"


def parse_args():
    parser = argparse.ArgumentParser(description="Verifica a corretude da execução de exclusão mútua")
    parser.add_argument("--n",   type=int, required=True, help="Número de processos")
    parser.add_argument("--r",   type=int, required=True, help="Repetições por processo")
    parser.add_argument("--log", type=str, default="coordinator_log.txt", help="Arquivo de log do coordenador")
    return parser.parse_args()


def check_resultado(n: int, r: int) -> list[tuple[str, str]]:
    """Verifica resultado.txt. Retorna lista de (status, descrição)."""
    results = []

    try:
        with open(RESULTADO_FILE, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
    except FileNotFoundError:
        return [(FAIL, f"'{RESULTADO_FILE}' não encontrado")]

    # 1. Total de linhas
    expected = n * r
    status = PASS if len(lines) == expected else FAIL
    results.append((status, f"Total de linhas: {len(lines)} (esperado: {expected})"))

    # 2. Contagem por processo
    counts: dict[int, int] = {}
    valid_format = True
    timestamps: list[datetime] = []
    for line in lines:
        parts = line.split(",", 1)
        if len(parts) != 2:
            valid_format = False
            continue
        try:
            pid = int(parts[0])
            ts  = datetime.strptime(parts[1], "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            valid_format = False
            continue
        counts[pid] = counts.get(pid, 0) + 1
        timestamps.append(ts)

    if not valid_format:
        results.append((FAIL, "Algumas linhas têm formato inválido em resultado.txt"))

    wrong_counts = {pid: cnt for pid, cnt in counts.items() if cnt != r}
    missing_procs = set(range(1, n + 1)) - set(counts.keys())
    if wrong_counts or missing_procs:
        details = ", ".join(f"P{pid}={cnt}" for pid, cnt in sorted(wrong_counts.items()))
        miss    = ", ".join(f"P{pid}=0" for pid in sorted(missing_procs))
        all_d   = f"{details}{', ' if details and miss else ''}{miss}"
        results.append((FAIL, f"Contagem incorreta por processo: {all_d}"))
    else:
        results.append((PASS, f"Cada um dos {n} processos escreveu exatamente {r} vez(es)"))

    # 3. Timestamps em ordem não-decrescente
    ordered = all(timestamps[i] <= timestamps[i + 1] for i in range(len(timestamps) - 1))
    status  = PASS if ordered else FAIL
    results.append((status, "Timestamps em ordem não-decrescente"))

    return results


def check_log(log_path: str) -> list[tuple[str, str]]:
    """Verifica coordinator_log.txt. Retorna lista de (status, descrição)."""
    results = []

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            raw_lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        return [(FAIL, f"'{log_path}' não encontrado")]

    # Parseia: TIMESTAMP|TIPO|DIRECAO|PROCESSO
    events = []
    for line in raw_lines:
        parts = line.split("|")
        if len(parts) < 4:
            continue
        msg_type  = parts[1]
        direction = parts[2]
        proc_str  = parts[3]
        pid = int(proc_str.lstrip("P")) if proc_str.lstrip("P").isdigit() else -1
        events.append((msg_type, direction, pid))

    # 4. GRANT sempre seguido de RELEASE antes do próximo GRANT
    grant_pending = False
    grant_release_ok = True
    interleave_errors = []
    for msg_type, direction, pid in events:
        if msg_type == "GRANT" and direction == "OUT":
            if grant_pending:
                grant_release_ok = False
                interleave_errors.append("GRANT sem RELEASE anterior")
            grant_pending = True
        elif msg_type == "RELEASE" and direction == "IN":
            if not grant_pending:
                grant_release_ok = False
                interleave_errors.append("RELEASE sem GRANT anterior")
            grant_pending = False

    if grant_pending:
        grant_release_ok = False
        interleave_errors.append("GRANT final sem RELEASE correspondente")

    status = PASS if grant_release_ok else FAIL
    desc   = "GRANT/RELEASE sempre intercalados"
    if interleave_errors:
        desc += f": {'; '.join(interleave_errors)}"
    results.append((status, desc))

    # 5. Ordem de proc_ids em REQUEST == ordem em RELEASE
    request_order = [pid for msg_type, direction, pid in events if msg_type == "REQUEST" and direction == "IN"]
    release_order = [pid for msg_type, direction, pid in events if msg_type == "RELEASE" and direction == "IN"]

    # A identificação inicial (msg_id=0) também aparece como REQUEST; filtramos pela contagem:
    # se houver mais REQUESTs que RELEASEs (por causa do handshake), descartamos o excesso inicial
    if len(request_order) > len(release_order):
        excess = len(request_order) - len(release_order)
        request_order = request_order[excess:]

    status = PASS if request_order == release_order else FAIL
    results.append((status, f"Ordem REQUEST == ordem RELEASE (primeiros {len(release_order)} eventos)"))

    return results


def main():
    args = parse_args()

    print("=" * 60)
    print("VERIFICAÇÃO DE resultado.txt")
    print("=" * 60)
    for status, desc in check_resultado(args.n, args.r):
        mark = "✓" if status == PASS else "✗"
        print(f"  [{status}] {mark} {desc}")

    print()
    print("=" * 60)
    print(f"VERIFICAÇÃO DE {args.log}")
    print("=" * 60)
    for status, desc in check_log(args.log):
        mark = "✓" if status == PASS else "✗"
        print(f"  [{status}] {mark} {desc}")

    print()


if __name__ == "__main__":
    main()
