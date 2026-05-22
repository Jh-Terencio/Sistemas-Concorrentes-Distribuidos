# Exclusão Mútua Centralizada Distribuída

Implementação em Python 3 do **algoritmo centralizado de exclusão mútua distribuída**, conforme especificação da disciplina de Sistemas Distribuídos.

## Estrutura

| Arquivo | Responsabilidade |
|---|---|
| `comum.py` | Constantes do protocolo (`F`, separador, IDs) e funções de (de)serialização. |
| `coordenador.py` | Coordenador multi-threaded (aceitar / algoritmo / interface). |
| `processo.py` | Função `run_processo(pid, r, k)` — laço REQUEST → GRANT → CR → RELEASE. |
| `executar.py` | Lança `n` processos com `multiprocessing` e valida ao final. |
| `validar.py` | Checa `resultado.txt` (nº de linhas, ordem, contagem) e o log (intercalação GRANT/RELEASE, ordem FIFO). |
| `teste_e2e.py` | (opcional) Sobe coordenador + clientes num único comando, para verificação local. |
| `plan.md` | Decisões de design e perguntas-modelo da apresentação. |
| `DIAGRAMAS.md` | Diagramas ASCII (arquitetura, sequência, máquina de estados). |
| `logs/coordenador.log` | Gerado em runtime — log de todas as mensagens. |
| `resultado.txt` | Gerado em runtime — escritas dos processos na região crítica. |

## Como executar

Em **dois terminais separados**, sempre dentro da pasta `solucao/`:

**Terminal 1** — coordenador:
```powershell
python coordenador.py
```

Comandos interativos disponíveis no terminal do coordenador:
- `fila` — mostra o titular atual da CR e a fila de espera.
- `atendidos` — mostra quantas vezes cada PID completou a CR.
- `sair` — encerra o coordenador.

**Terminal 2** — clientes:
```powershell
python executar.py                       # defaults: n=5, r=3, k=1
python executar.py --n 10 --r 5 --k 0.1  # cenário com contenção
python executar.py --n 2  --r 2 --k 2    # cenário lento (bom para demonstrar passo-a-passo)
```

Ao final, `executar.py` chama automaticamente o `validar.py`.

### Verificação rápida (um único terminal)

Para desenvolvimento, o `teste_e2e.py` orquestra tudo automaticamente:

```powershell
python teste_e2e.py --n 5 --r 3 --k 0.3
python teste_e2e.py --n 10 --r 3 --k 0.1
```

(Para a **apresentação**, use os dois terminais separados — o professor vai querer ver o log do coordenador rolando em tempo real e os comandos `fila`/`atendidos` interativos.)

## Protocolo

Toda mensagem tem **tamanho fixo de F=16 bytes**, formato:

```
<id_msg>|<pid>|<padding com '0' até F bytes>
```

| ID | Tipo | Direção |
|---|---|---|
| `1` | REQUEST | processo → coordenador |
| `2` | GRANT | coordenador → processo |
| `3` | RELEASE | processo → coordenador |

Exemplo (F=16): `"1|3|000000000000"` = REQUEST do PID 3.

## Arquitetura do coordenador

Três threads, conforme sugerido pelo enunciado:

1. **`thread_aceitar`** — fica em `accept()` esperando novas conexões. Ao aceitar, lê o primeiro REQUEST (que serve de handshake para descobrir o PID), registra o socket no `selectors` da thread de algoritmo e envia um byte pelo "socket de wakeup" para destravar o `select()`.

2. **`thread_algoritmo`** — núcleo da lógica. Usa `selectors.select()` para esperar I/O em todos os sockets de clientes ao mesmo tempo, **sem busy-wait**. A cada mensagem:
   - REQUEST → enfileira o PID. Se a CR está livre, manda GRANT imediato.
   - RELEASE → contabiliza atendimento, libera a CR, manda GRANT para o próximo da fila.

3. **`thread_interface`** — roda na main thread; bloqueia em `input()` e processa os comandos do terminal.

Sincronização: um único `threading.Lock` (`estado_lock`) protege a fila, o dicionário de clientes, o contador de atendimentos e o titular atual.

## Validação

`validar.py` confere automaticamente:

- `resultado.txt` tem exatamente `n*r` linhas em ordem cronológica.
- Cada PID escreveu exatamente `r` vezes.
- No log do coordenador, todo `ENVIADO GRANT` é seguido (sem outro GRANT no meio) por um `RECEBIDO RELEASE` do mesmo PID.
- A ordem dos GRANT casa com a ordem dos RELEASE (FIFO respeitada).
