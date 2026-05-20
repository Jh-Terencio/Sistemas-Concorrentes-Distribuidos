# Plan: Exclusão Mútua Distribuída Centralizada

## TL;DR
Implementar em Python (TCP + argparse) o algoritmo centralizado de exclusão mútua distribuída. O sistema é composto por 5 arquivos: módulo de mensagens, coordenador multi-threaded, processo cliente, script de lançamento e script de verificação.

---

## Arquivos a criar (todos em `Trabalho_SD/`)
- `messages.py` — protocolo de mensagens (constantes, encode/decode)
- `coordinator.py` — coordenador multi-threaded (3 threads)
- `process.py` — processo cliente (argparse)
- `launch.py` — script que lança n processos
- `verify.py` — script de verificação do resultado.txt e log

---

## Fases

### Fase 1 — Protocolo de Mensagens (`messages.py`)
1. Definir constante F (tamanho fixo, ex: 20 bytes) e SEPARATOR (`|`)
2. Definir tipos: REQUEST=`1`, GRANT=`2`, RELEASE=`3`
3. Implementar `encode(msg_type, proc_id, msg_id) -> bytes` — monta string `type|proc_id|msg_id` e preenche com `0`s até F bytes
4. Implementar `decode(raw: bytes) -> (msg_type, proc_id, msg_id)` — faz split por `|` e converte

**Formato:** `1|3|42|000000000` (F=20, type=1, proc_id=3, msg_id=42)

---

### Fase 2 — Coordenador (`coordinator.py`)
*Depende da Fase 1*

**Estruturas de dados compartilhadas (protegidas por `threading.Lock`):**
- `clients: dict {proc_id: socket}` — sockets dos processos
- `request_queue: collections.deque` — fila de proc_ids aguardando
- `grant_counts: dict {proc_id: int}` — contagem de GRANTs por processo
- `current_holder: int | None` — quem detém a RC agora
- `sockets_list: list` — sockets monitorados pelo select()
- `msg_counter: int` — ID sequencial de mensagens enviadas

**Thread 1 — `accept_thread`:**
5. Cria socket TCP, bind na porta configurada, listen
6. Loop: `accept()` nova conexão, recebe primeiro pacote com proc_id do cliente, adiciona a `clients` e `sockets_list`, incrementa `grant_counts[proc_id]=0`

**Thread 2 — `algorithm_thread`:**
7. Usa `select.select(sockets_list, ...)` para monitorar todos os clientes
8. Ao receber REQUEST: adiciona proc_id à `request_queue`; se `current_holder is None`, faz pop e envia GRANT, define `current_holder`
9. Ao receber RELEASE: limpa `current_holder`; se fila não vazia, pop e envia GRANT ao próximo
10. Gera log a cada mensagem recebida/enviada: `TIMESTAMP | TYPE | IN/OUT | PROC_ID`

**Thread 3 — `interface_thread`:**
11. Loop bloqueado em `input()`, processa comandos:
    - `q`: imprime fila atual
    - `c`: imprime contagem por processo
    - `x`: encerra (seta flag `running=False`)

**Sincronização:** Um único `threading.Lock` protege `request_queue`, `current_holder`, `grant_counts` e `sockets_list`. O `algorithm_thread` adquire o lock ao processar cada mensagem.

**Log:** Arquivo `coordinator_log.txt` com linhas `TIMESTAMP|TYPE|DIRECTION|PROC_ID`

---

### Fase 3 — Processo (`process.py`)
*Depende da Fase 1*

**Argparse:** `--id`, `--host`, `--port`, `--r`, `--k`

12. Conecta socket TCP ao coordenador
13. Envia pacote de identificação com próprio proc_id ao conectar
14. Loop r vezes:
    a. Monta e envia REQUEST (com msg_id incremental)
    b. Aguarda receber GRANT (recv F bytes)
    c. Abre `resultado.txt` em modo append, escreve `proc_id,YYYY-MM-DD HH:MM:SS.mmm\n`, fecha
    d. Monta e envia RELEASE
    e. `time.sleep(k)`
15. Fecha socket e termina

---

### Fase 4 — Script de Lançamento (`launch.py`)
*Depende das Fases 2 e 3*

**Argparse:** `--n`, `--r`, `--k`, `--host`, `--port`

16. Remove `resultado.txt` se existir (fresh start)
17. Loop de 1 a n: chama `subprocess.Popen(['python', 'process.py', '--id', str(i), ...])` sem delay
18. Aguarda todos os processos terminarem com `p.wait()` / `p.join()`

---

### Fase 5 — Script de Verificação (`verify.py`)
*Depende da Fase 4 ter gerado resultado.txt e coordinator_log.txt*

**Argparse:** `--n`, `--r`, `--log` (caminho do log do coordenador)

19. Lê `resultado.txt`:
    - Verifica total de linhas == n*r
    - Conta ocorrências por proc_id e verifica que cada um tem exatamente r
    - Verifica que timestamps estão em ordem não-decrescente
20. Lê `coordinator_log.txt`:
    - Verifica que depois de cada GRANT sempre vem um RELEASE antes do próximo GRANT
    - Verifica que a ordem dos proc_ids em REQUEST é igual à ordem dos proc_ids em RELEASE
21. Imprime relatório: PASS/FAIL por verificação

---

## Arquivos relevantes de referência
- `server_thread.py` — padrão de threading + Lock + socket TCP (reutilizar como base)
- `client.py` — padrão de conexão TCP de cliente (reutilizar como base)
- `totally_ordered_multicast.py` — padrão de múltiplos sockets + argparse + threading.RLock

---

## Verificação
1. Iniciar coordenador: `python coordinator.py --port 5000`
2. Rodar launch: `python launch.py --n 3 --r 5 --k 1 --host 127.0.0.1 --port 5000`
3. Verificar `resultado.txt` tem 15 linhas
4. Rodar: `python verify.py --n 3 --r 5 --log coordinator_log.txt`
5. Testar comandos de interface no terminal do coordenador (q, c, x)

---

## Decisões
- Python + TCP + argparse (escolha do usuário)
- F = 20 bytes com separador `|`
- Identificação inicial: ao conectar, processo envia seu proc_id como primeiro pacote
- Log do coordenador em `coordinator_log.txt`
- `resultado.txt` criado/resetado pelo `launch.py` a cada execução
- Todos os arquivos ficam em `Trabalho_SD/`
