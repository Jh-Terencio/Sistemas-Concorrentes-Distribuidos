# Plano — Trabalho de Exclusão Mútua Centralizada (Sistemas Distribuídos)

## Contexto

Trabalho da disciplina de Sistemas Concorrentes Distribuídos: projetar e implementar o **algoritmo centralizado de exclusão mútua distribuída** em **Python**, usando **sockets TCP** (um socket por processo). Há um coordenador multi-threaded que serializa o acesso à região crítica, e `n` processos clientes que executam em loop `r` requisições, escrevendo em `resultado.txt` e dormindo `k` segundos dentro da seção crítica.

Entregáveis exigidos pelo enunciado:
- Programa funcional (coordenador + processos + launcher).
- Log do coordenador com todas as mensagens enviadas/recebidas.
- Arquivo `resultado.txt` com `n*r` linhas para validação de corretude.
- Avaliação em diferentes cenários (variando n, k, r).
- Relatório de até 6 páginas e apresentação de até 6 minutos.
- Trabalho em trio.

Decisões já alinhadas:
- **Linguagem:** Python
- **Transporte:** TCP, um socket por processo (coordenador mantém dicionário `pid -> socket`)
- **Inicialização:** launcher Python usando `argparse` para parametrizar `n`, `k`, `r`, host e porta
- **Estudos de caso:** variar `n`, `k` e `r`

**Requisitos extras solicitados pelo professor (fora do PDF):**
- **Relógio lógico de Lamport** implementado em todos os processos e no coordenador, atualizado em cada evento de envio/recebimento de mensagem (regra: `L = max(L, L_recebido) + 1`). O timestamp lógico deve ser registrado no log do coordenador e usado para ordenar eventos.
- **Sleep aleatório fora da região crítica:** cada processo dorme `random.uniform(3, 4)` segundos **antes** de enviar cada REQUEST, para dessincronizar os processos e evitar que cheguem todos juntos ao coordenador.

## Estrutura do projeto

```
Sistemas-Concorrentes-Distribuidos/
├── plan.md                     # este plano
├── coordenador.py              # processo coordenador multi-threaded
├── processo.py                 # processo cliente
├── launcher.py                 # dispara n processos sequencialmente via argparse
├── protocolo.py                # constantes, encode/decode de mensagens de F bytes + LamportClock
├── verificar.py                # valida resultado.txt e log do coordenador
├── experimentos/
│   ├── rodar_experimentos.py   # roda múltiplas combinações de n, k, r
│   └── resultados/             # logs e resultado.txt de cada cenário
├── relatorio/
│   └── relatorio.md            # rascunho do relatório (até 6 páginas)
└── README.md                   # instruções de execução
```

## Etapas de desenvolvimento

### Etapa 1 — Protocolo de mensagens (`protocolo.py`)
- Definir constante `F` (tamanho fixo, ex.: `F = 24` bytes para acomodar o timestamp lógico) e separador `|`.
- IDs de mensagem: `REQUEST=1`, `GRANT=2`, `RELEASE=3`.
- **Formato (com Lamport):** `<msg_id>|<pid>|<lamport>|<padding até F bytes>` — o `lamport` é o relógio lógico do remetente no momento do envio.
- Funções:
  - `encode(msg_id, pid, lamport) -> bytes` — gera string de F bytes.
  - `decode(buf) -> (msg_id, pid, lamport)` — separa pelos `|` e valida tamanho.
- Como o TCP é stream, ler exatamente F bytes por mensagem (`recv` em loop até completar F).

### Etapa 1.b — Relógio lógico de Lamport
- Classe `LamportClock` com atributo `value` e `Lock`:
  - `tick() -> int`: incrementa e retorna o valor (uso em evento local / antes de enviar).
  - `update(received: int) -> int`: `value = max(value, received) + 1` (uso ao receber mensagem).
- **Coordenador** mantém um `LamportClock` próprio, atualizado a cada REQUEST/RELEASE recebido e antes de cada GRANT enviado.
- **Processo cliente** mantém um `LamportClock` próprio, atualizado antes de enviar REQUEST/RELEASE e ao receber GRANT.
- O log do coordenador inclui o timestamp lógico de Lamport além do timestamp físico (ms).

### Etapa 2 — Coordenador (`coordenador.py`)
Três threads, conforme sugerido no enunciado:

1. **Thread de aceitação de conexões** (`accept_loop`)
   - `socket.accept()` em loop; para cada novo cliente cria uma thread receptora dedicada e adiciona o socket ao dicionário `clientes[pid]`.

2. **Thread receptora por cliente** (`handle_client`)
   - Lê mensagens de F bytes do socket; faz `clock.update(msg.lamport)` em cada recebimento; ao receber REQUEST, enfileira `pid` na fila; ao receber RELEASE, sinaliza o algoritmo.

3. **Thread do algoritmo de exclusão mútua** (`mutex_loop`)
   - Loop: aguarda fila não vazia → tira primeiro `pid` → `clock.tick()` e envia GRANT pelo socket dele → aguarda RELEASE daquele `pid` → repete.
   - Mantém contador `atendidos[pid]`.

4. **Thread de interface (terminal)** (`interface_loop`)
   - `input()` bloqueante. Comandos:
     - `1` → imprime fila atual.
     - `2` → imprime quantas vezes cada processo foi atendido.
     - `3` → encerra (sinaliza shutdown e fecha sockets).

Sincronização:
- `collections.deque` + `threading.Lock` + `threading.Condition` para coordenar "fila vazia" / "RELEASE recebido".
- Opção recomendada vs. `queue.Queue`: precisamos inspecionar a fila para o comando `1` da interface.

Log:
- Função `log(direcao, tipo_msg, pid, lamport)` grava em `coordenador.log` com timestamp físico em milissegundos (`datetime.now().isoformat(timespec='milliseconds')`) **e** o relógio de Lamport do coordenador no momento do evento.
- Formato sugerido: `<timestamp_fisico>|<lamport_coord>|<direcao>|<tipo>|<pid>|<lamport_msg>`.
- Lock dedicado para escrita no log.

### Etapa 3 — Processo cliente (`processo.py`)
- Args: `--pid`, `--host`, `--porta`, `--k`, `--r`.
- Mantém instância local de `LamportClock`.
- Conecta TCP ao coordenador.
- Loop `r` vezes:
  1. **Sleep aleatório fora da RC:** `time.sleep(random.uniform(3, 4))` — dessincroniza requisições.
  2. `clock.tick()` e envia REQUEST com `lamport` atual.
  3. Aguarda GRANT (bloqueante; lê F bytes); ao receber, `clock.update(grant.lamport)`.
  4. Abre `resultado.txt` em modo append e escreve `<pid>|<timestamp_físico_ms>|<lamport>\n`, fecha.
  5. `time.sleep(k)`.
  6. `clock.tick()` e envia RELEASE com `lamport` atual.
- Fecha socket e termina.

### Etapa 4 — Launcher (`launcher.py`)
- `argparse` com flags `--n`, `--k`, `--r`, `--host`, `--porta`.
- Loop sequencial (sem retardo) que invoca `subprocess.Popen(["python", "processo.py", "--pid", str(i), ...])` para `i` em `range(n)`.
- Espera todos terminarem (`proc.wait()`) e imprime resumo.
- **Pré-condição:** coordenador já deve estar rodando (documentado no README).

### Etapa 5 — Verificação de corretude (`verificar.py`)
- Conferir que `resultado.txt` tem exatamente `n*r` linhas.
- Conferir que os timestamps físicos são monotonicamente crescentes.
- Conferir que **os timestamps de Lamport também são monotonicamente crescentes** (consistência causal).
- Conferir que cada `pid` aparece exatamente `r` vezes.
- Conferir no `coordenador.log` que: (a) cada GRANT é seguido de RELEASE do mesmo `pid` antes do próximo GRANT; (b) a ordem dos `pid` em REQUEST atendidos é igual à ordem de RELEASE.

### Etapa 6 — Estudos de caso (`experimentos/rodar_experimentos.py`)
Matriz de experimentos (combinações sugeridas, ajustáveis). Lembrar que cada iteração agora soma `random.uniform(3, 4)` segundos fora da RC:
- **Variar n:** `n ∈ {2, 4, 8, 16}` com `k=0.1`, `r=5` fixos.
- **Variar k:** `k ∈ {0, 0.05, 0.2, 0.5}` com `n=4`, `r=5` fixos.
- **Variar r:** `r ∈ {3, 5, 10}` com `n=4`, `k=0.1` fixos.

Para cada cenário:
1. Limpar `resultado.txt` e `coordenador.log`.
2. Subir coordenador, rodar launcher, esperar término.
3. Coletar métricas: tempo total, throughput (RC/seg), tamanho médio da fila, justiça (variância de `atendidos[pid]`), drift do relógio de Lamport vs. relógio físico.
4. Mover artefatos para `experimentos/resultados/<cenario>/`.

### Etapa 7 — Relatório e apresentação
- **Relatório (até 6 páginas):** introdução, decisões de projeto (protocolo F bytes, TCP vs UDP, escolha de threads, sincronização com `Condition`, Lamport, sleep aleatório), arquitetura (diagrama), implementação resumida, estudos de caso (tabelas/gráficos), corretude, conclusão.
- **Apresentação (até 6 min):** problema, arquitetura, demo curta (screenshot do log), 1 slide por estudo de caso, conclusão.
- Trio: dividir entre os 3 membros (ex.: coordenador / processo+launcher / experimentos+relatório).

## Arquivos críticos a serem criados
- `protocolo.py`
- `coordenador.py`
- `processo.py`
- `launcher.py`
- `verificar.py`
- `experimentos/rodar_experimentos.py`
- `README.md`

## Bibliotecas Python (stdlib, sem dependências externas)
- `socket` — comunicação TCP.
- `threading` (`Thread`, `Lock`, `Condition`) — multi-threading e proteção do `LamportClock`.
- `collections.deque` — fila de pedidos com inspeção segura.
- `argparse` — CLI do launcher e dos processos.
- `subprocess.Popen` — iniciar os n processos sequencialmente.
- `time` / `datetime` — timestamps com milissegundos.
- `random` — sleep aleatório `random.uniform(3, 4)` fora da região crítica.

## Verificação end-to-end

1. **Smoke test (n=2, k=0.1, r=3):**
   ```
   # Terminal 1
   python coordenador.py --porta 5000
   # Terminal 2
   python launcher.py --n 2 --k 0.1 --r 3 --host 127.0.0.1 --porta 5000
   ```
   Esperado: `resultado.txt` com 6 linhas; `coordenador.log` mostrando 6 REQUEST, 6 GRANT, 6 RELEASE intercalados, com timestamps de Lamport monotonicamente crescentes. Devido ao sleep aleatório de 3–4s fora da RC, a execução total deve ficar próxima de `r * 3.5s ≈ 10–12s`.

2. **Comandos de interface do coordenador:** durante a execução, digitar `1`, `2` e ao final `3` no terminal do coordenador.

3. **Script de verificação:** `python verificar.py --resultado resultado.txt --log coordenador.log --n 2 --r 3` deve reportar "OK".

4. **Carga moderada (n=8, k=0.05, r=10):** confirmar que não há deadlock, que `resultado.txt` tem 80 linhas e timestamps monotônicos.

5. **Rodar matriz completa:** `python experimentos/rodar_experimentos.py` e inspecionar `experimentos/resultados/`.
