# Plano — Exclusão Mútua Centralizada Distribuída (Sistemas Distribuídos)

## Contexto

Trabalho da disciplina de Sistemas Distribuídos: implementar o **algoritmo centralizado de exclusão mútua distribuída** em Python 3, usando apenas bibliotecas padrão (`socket`, `threading`, `selectors`, `queue`, `logging`, `time`, `datetime`, `multiprocessing`).

O sistema é composto por um **coordenador** (multi-threaded) que arbitra acesso a uma região crítica usando uma fila FIFO de pedidos, e por **n processos clientes** que repetidamente solicitam acesso, escrevem (PID, timestamp) em `resultado.txt`, dormem `k` segundos e liberam.

Apresentação **ao vivo (6 min, em trio)** — então o código deve ser **simples de explicar**, com nomes em **português**, comentários focados no *porquê*, e divisão clara de responsabilidades. Existe um código de referência de turmas anteriores na pasta `Trabalho_SD_ExclusaoMutua/` que será usado **apenas como inspiração** (tem bugs sérios — ver seção "Lições do código antigo").

## Decisões de design

| Decisão | Escolha | Justificativa |
|---|---|---|
| Estrutura | Múltiplos arquivos em pasta nova `solucao/` | Permite explicar uma responsabilidade por vez na apresentação |
| Idioma | Português nos identificadores e comentários | Combina com o enunciado e facilita defesa oral |
| Threading do coordenador | 3 threads: aceitar + algoritmo (com `selectors.select`) + interface | Segue exatamente a sugestão do PDF; evita busy-wait (bug do código de referência) |
| Transporte | TCP (`socket.SOCK_STREAM`) | Mais simples para garantir entrega ordenada e confiável; um socket por processo |
| Framing | Mensagens de tamanho fixo `F = 16` bytes + leitura em laço até completar `F` | Resolve fragmentação de TCP; ainda atende o requisito de tamanho fixo |
| Formato da mensagem | `"{id}|{pid}|<padding-zeros>"` ex: `"1|3|000000000000"` | Igual ao exemplo do PDF |
| Parse | `split('|')` e tomar `parts[0]`/`parts[1]`, ignorando `parts[2]` (padding) | Evita o bug de `rstrip('0')` do código antigo (que quebra com PID ≥ 10) |
| Fila | `collections.deque` protegida por `threading.Lock` | `Queue` esconderia detalhes que precisamos inspecionar (comando "fila") |
| Lançamento dos processos | `multiprocessing.Process` no script `executar.py`, sequencial sem retardo | PDF exige inicialização sequencial e mesma máquina |
| Parâmetros | CLI args: `n`, `r`, `k` (com defaults n=5, r=3, k=1) | Permite demonstrar diferentes cenários ao vivo |

## Estrutura de arquivos

```
solucao/
  comum.py          # constantes (F, separador, IDs de msg) + serializar/parsear
  coordenador.py    # 3 threads: aceitar / algoritmo / interface
  processo.py       # função run_processo(pid, r, k): loop REQUEST→GRANT→CR→RELEASE
  executar.py       # lança n processos com multiprocessing + valida no final
  validar.py        # checagens de corretude (linhas, ordem cronológica, log)
  README.md         # como rodar, exemplos, comandos da interface
  logs/             # gerado em runtime: coordenador.log
  resultado.txt     # gerado em runtime
```

Arquivos antigos em `Trabalho_SD_ExclusaoMutua/` ficam intocados para comparação.

## Protocolo de mensagens (`comum.py`)

```
F = 16 bytes, separador = "|"

MSG_REQUEST = "1"   # processo pede a região crítica
MSG_GRANT   = "2"   # coordenador autoriza
MSG_RELEASE = "3"   # processo libera

Formato: "{id_msg}|{pid}|{padding até F bytes com '0'}"
Exemplos com F=16:
  REQUEST de PID 3   -> "1|3|000000000000"
  GRANT   p/ PID 12  -> "2|12|00000000000"
  RELEASE de PID 12  -> "3|12|00000000000"
```

Funções:
- `serializar(id_msg: str, pid: int) -> bytes` — monta string e faz `ljust(F, "0").encode()`
- `parsear(dados: bytes) -> tuple[str, int]` — `decode().split('|')` e retorna `(parts[0], int(parts[1]))`
- `receber_completo(sock) -> bytes | None` — laço lendo até completar F bytes (resolve fragmentação TCP); retorna `None` em desconexão

## Coordenador (`coordenador.py`)

Estado compartilhado, protegido por `threading.Lock`:
- `fila_pedidos: deque[int]` — PIDs na ordem de chegada
- `clientes: dict[int, socket]` — pid → socket
- `atendidos: dict[int, int]` — contagem de quantas vezes cada pid completou a CR
- `processo_atual: int | None` — quem está na CR (None se ninguém)
- `executando: threading.Event` — flag para encerramento limpo

**Thread 1 — `thread_aceitar(servidor)`**
- Loop: `accept()` → handshake (lê primeira msg para descobrir PID) → registra socket no `selectors` da thread de algoritmo via uma fila interna de "novos sockets" + `wakeup pipe` (truque para acordar o `select`).
- *Mais simples na prática*: registra direto no selectors sob o lock e dispara um byte no pipe de wakeup para garantir que o `select` reavalie.

**Thread 2 — `thread_algoritmo()`**
- `sel = selectors.DefaultSelector()`
- Loop principal:
  1. `eventos = sel.select(timeout=0.5)` — espera I/O ou timeout (para checar `executando`)
  2. Para cada socket pronto: `receber_completo()` → `parsear()` → registra no log
     - Se **REQUEST**: anexa pid na `fila_pedidos`. Se a fila estava vazia E `processo_atual is None`, envia GRANT imediato e marca titular.
     - Se **RELEASE**: confere que o pid é o titular (invariante), zera titular, incrementa `atendidos[pid]`, e se a fila não está vazia, popleft o próximo e envia GRANT a ele.
  3. Trata desconexão: remove do `clientes` e do `selectors`.
- **Sem busy-wait**: `select` bloqueia até ter trabalho — corrige o bug do código de referência.

**Thread 3 — `thread_interface()`** (roda na main thread)
- Loop `input()` com comandos:
  - `fila` → imprime `list(fila_pedidos)` e `processo_atual`
  - `atendidos` → imprime `dict(atendidos)`
  - `sair` → seta `executando.clear()`, fecha sockets, encerra

**Logging** (`logging` para `logs/coordenador.log`):
- Formato: `%(asctime)s.%(msecs)03d | %(message)s` com `datefmt="%H:%M:%S"`
- Cada mensagem registra: `RECEBIDO REQUEST de PID X`, `ENVIADO GRANT para PID X`, `RECEBIDO RELEASE de PID X`, conexão/desconexão.

## Processo (`processo.py`)

Função `run_processo(pid, r, k)`:
1. Cria socket TCP, conecta em `127.0.0.1:5000`.
2. Envia mensagem inicial (usa REQUEST de identificação — o handshake é o **primeiro REQUEST do laço**, não inventa msg "0" como o código antigo).
   - *Decisão*: o coordenador identifica o pid pelo **conteúdo da primeira mensagem recebida** desse socket. Sem msg extra fora do protocolo.
3. Loop `r` vezes:
   - Envia REQUEST (na primeira iteração, isso também serve de handshake)
   - `receber_completo()` até chegar um GRANT para esse pid
   - **Região crítica**: abre `resultado.txt` em modo `"a"`, escreve `f"PID {pid} | {datetime.now().strftime('%H:%M:%S.%f')}\n"`, fecha
   - `time.sleep(k)`
   - Envia RELEASE
4. Fecha socket.

## Lançador e validação (`executar.py` + `validar.py`)

`executar.py`:
- CLI: `python executar.py --n 5 --r 3 --k 1`
- Limpa `resultado.txt`.
- Avisa o usuário que o coordenador precisa estar rodando em outro terminal (não inicia coordenador automaticamente — facilita ver os dois logs lado a lado na apresentação).
- Cria `n` `multiprocessing.Process` chamando `run_processo`, dá `.start()` em todos sem retardo, depois `.join()`.
- No final, chama `validar.validar_resultado(n, r)` e `validar.validar_log()`.

`validar.py`:
- `validar_resultado(n, r)`: verifica nº de linhas = `n*r`, ordem cronológica, e `r` execuções por pid.
- `validar_log()`: lê `logs/coordenador.log` e verifica invariantes:
  - Todo GRANT é seguido (eventualmente) por um RELEASE do mesmo PID antes de outro GRANT.
  - A ordem de RELEASE dos pids casa com a ordem de REQUEST aceita (FIFO).

## Lições do código antigo (o que NÃO repetir)

1. **Não usar `rstrip("0")` na string inteira** — quebra PIDs com zeros (10, 20, 100…). Solução: split primeiro, ignora padding.
2. **Não fazer busy-wait com `setblocking(False)` + `recv` em loop** — usa `selectors.select` com timeout.
3. **Não inventar msg_id "0" fora do protocolo** — o primeiro REQUEST já identifica o pid.
4. **Não chamar `recv(F)` confiando que vem F bytes** — fazer laço de leitura até completar F.
5. **Encerramento limpo**: usar `threading.Event` em vez de daemon threads abandonadas.

## Como executar (irá no README.md)

```powershell
# Terminal 1 — coordenador
python solucao/coordenador.py

# Terminal 2 — processos
python solucao/executar.py --n 5 --r 3 --k 1

# Comandos no terminal do coordenador (durante execução):
#   fila        -> mostra a fila atual de pedidos + titular
#   atendidos   -> mostra quantas vezes cada PID completou a CR
#   sair        -> encerra o coordenador
```

## Verificação end-to-end

1. **Cenário base**: `n=5, r=3, k=1` — esperado `resultado.txt` com 15 linhas, ordem cronológica, validação automática deve passar.
2. **Cenário com contenção**: `n=10, r=5, k=0.1` — testa a fila e a robustez do parse com PID=10.
3. **Cenário leve**: `n=2, r=2, k=2` — bom para demonstrar passo-a-passo na apresentação.
4. **Comandos da interface**: durante o cenário com contenção, executar `fila` no terminal do coordenador várias vezes para mostrar a evolução.
5. **Inspecionar log**: abrir `logs/coordenador.log` e mostrar a sequência REQUEST/GRANT/RELEASE intercalada.

## Possíveis perguntas do professor (para preparar a defesa)

| Pergunta | Como responder |
|---|---|
| "Por que centralizado é simples mas não escala?" | Ponto único de falha + gargalo de mensagens (3 msgs por entrada na CR, todas passam pelo coordenador). Comparar com Ricart-Agrawala (2(n-1) msgs) e token ring. |
| "Onde está a região crítica protegida no seu código?" | Entre o recebimento do GRANT e o envio do RELEASE no `processo.py` — o `open/write/close` em `resultado.txt`. |
| "Como você garante FIFO?" | `deque` no coordenador: REQUEST faz `append`, RELEASE faz `popleft`. A entrega ordenada do TCP garante que a ordem de chegada no socket reflita a ordem de envio de cada processo. |
| "Por que precisa do lock se a fila já é acessada por uma thread só?" | A interface (`fila`/`atendidos`) lê de outra thread — sem lock haveria leitura inconsistente. |
| "O que acontece se um processo cair segurando o GRANT?" | Bug conhecido do centralizado: o coordenador trava na CR. Mitigação possível (não implementada): timeout no GRANT ou detecção de desconexão durante a CR liberando automaticamente. |
| "Por que mensagens de tamanho fixo?" | Simplifica o framing: o coordenador sabe exatamente quantos bytes ler. Em TCP, sem isso, precisaríamos de delimitador ou prefixo de tamanho. |
| "Por que `selectors` em vez de uma thread por cliente?" | A) Atende a sugestão do PDF de UMA thread de algoritmo. B) Evita custo de criar n threads. C) Centraliza a lógica num único laço — mais fácil de raciocinar e logar. |
| "O log mostra GRANT sempre intercalado com RELEASE?" | Sim — invariante do algoritmo: só há um titular por vez. O script `validar.py` checa isso automaticamente. |
| "Por que TCP e não UDP?" | TCP garante entrega ordenada e confiável → não preciso me preocupar com perda/reordenação. UDP exigiria retransmissão e sequence numbers. O PDF permite ambos. |
| "O que é o `selectors.select` exatamente?" | Wrapper sobre `select`/`epoll`/`kqueue` do SO: bloqueia até que algum dos sockets registrados tenha dados, sem CPU ativa. |
| "Como funcionaria com processos em máquinas diferentes?" | Trocar `127.0.0.1` pelo IP da máquina do coordenador e abrir a porta no firewall. O algoritmo não muda. |
