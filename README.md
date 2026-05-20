# Exclusão Mútua Centralizada (Sistemas Distribuídos)

Implementação em Python do algoritmo centralizado de exclusão mútua distribuída
com sockets TCP, relógio lógico de Lamport e sleep aleatório fora da região
crítica, conforme `plan.md`.

## Arquivos

| Arquivo | Função |
| --- | --- |
| `protocolo.py` | Constantes (`F=24`, IDs), `encode/decode` de mensagens, `LamportClock`. |
| `coordenador.py` | Servidor TCP multi-threaded (accept, handle_client, mutex_loop, interface). |
| `processo.py` | Cliente: conecta, faz `r` iterações de REQUEST/GRANT/RELEASE. |
| `launcher.py` | Dispara `n` processos clientes via `subprocess.Popen`. |
| `verificar.py` | Valida `resultado.txt` e `coordenador.log`. |
| `experimentos/rodar_experimentos.py` | Roda a matriz de cenários e coleta métricas. |

## Protocolo de mensagens

Cada mensagem tem **24 bytes ASCII** (`F = 24`):

```
<msg_id>|<pid>|<lamport>|<padding com '.' até F bytes>
```

- `msg_id`: `1=REQUEST`, `2=GRANT`, `3=RELEASE`
- `lamport`: relógio lógico do remetente no instante do envio

Como TCP é stream, o leitor (`recv_exato`) lê em loop até completar `F` bytes.

## Relógio lógico de Lamport

Implementado em `protocolo.py` como classe `LamportClock` (thread-safe):

- `tick()`: incrementa antes de enviar / evento local.
- `update(recebido)`: `L = max(L, L_recebido) + 1` ao receber mensagem.

O coordenador e cada processo cliente mantêm a sua própria instância. O log
do coordenador registra:

```
<timestamp_físico_ms>|<lamport_coord>|<direção>|<tipo>|<pid>|<lamport_msg>
```

## Sleep aleatório fora da região crítica

Cada processo dorme `random.uniform(3, 4)` segundos **antes** de cada REQUEST
(parâmetros `--sleep-min` e `--sleep-max` em `processo.py` / `launcher.py`).
Isso dessincroniza as requisições. Para validação rápida, é possível reduzir
esses limites (ver `--rapido` em `rodar_experimentos.py`).

## Como executar

### Smoke test

Terminal 1 — coordenador:

```powershell
python coordenador.py --porta 5000
```

Terminal 2 — launcher (apaga `resultado.txt` antes):

```powershell
python launcher.py --n 2 --k 0.1 --r 3 --host 127.0.0.1 --porta 5000 --limpar
```

Esperado: `resultado.txt` com **6 linhas** e `coordenador.log` com 6
REQUEST + 6 GRANT + 6 RELEASE, intercalados na ordem
`REQ → GRANT(x) → RELEASE(x) → ...`.

Como cada processo dorme 3–4 s fora da RC, a execução total fica em torno de
`r * 3.5s ≈ 10–12s`.

### Comandos da interface do coordenador

Enquanto o coordenador está rodando, digite no terminal dele:

- `1` → mostra a fila atual de pids esperando.
- `2` → mostra `pid -> atendidos` (quantos GRANTs cada processo recebeu).
- `3` → encerra o coordenador, fechando todos os sockets.

### Verificação

```powershell
python verificar.py --resultado resultado.txt --log coordenador.log --n 2 --r 3
```

### Matriz de experimentos

```powershell
python experimentos/rodar_experimentos.py
```

Cada cenário sobe um coordenador isolado em uma porta livre, executa o
launcher e salva `resultado.txt`, `coordenador.log` e `metricas.json` em
`experimentos/resultados/<nome_cenario>/`. O resumo agregado fica em
`experimentos/resultados/resumo.json`.

Para validar rapidamente o script sem esperar os sleeps de 3–4 s, use
`--rapido` (sleep 0.1–0.3 s) — **note que isso não corresponde ao cenário
exigido pelo professor**, apenas serve para testar a infraestrutura.

## Métricas coletadas por cenário

- `tempo_total_s`
- `throughput_rcs_por_s` (RCs executadas por segundo)
- `tamanho_medio_fila` e `tamanho_max_fila`
- `atendidos_por_pid` e `variancia_justica` (variância do número de atendimentos por pid)
- `drift_lamport_vs_fisico` (diferença média entre Lamport e timestamp físico, ambos normalizados)

## Decisões de projeto

- **TCP, um socket por processo.** Coordenador mantém `clientes: dict[pid, socket]`.
- **Threading:** `accept_loop` + uma thread `handle_client` por cliente +
  `mutex_loop` + `interface_loop`.
- **Sincronização:** `collections.deque` para fila, `threading.Condition` para
  acordar `mutex_loop` quando há REQUESTs, `threading.Event` para sinalizar
  RELEASE esperado. Optou-se por `deque + Condition` (em vez de `queue.Queue`)
  para permitir inspeção da fila pelo comando `1` da interface.
- **Arquivo `resultado.txt`:** modo *append* por processo. A exclusão mútua é
  garantida pelo coordenador, então não há concorrência real na escrita.
