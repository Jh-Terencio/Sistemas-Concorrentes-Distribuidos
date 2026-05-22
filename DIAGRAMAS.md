# Diagramas de funcionamento

Diagramas em ASCII (renderizam em qualquer editor monoespaçado) explicando o sistema de exclusão mútua centralizada implementado nesta pasta.

## 1. Arquitetura geral

```
                  ┌──────────────────────────────────────────────┐
                  │                  COORDENADOR                 │
                  │                                              │
                  │  ┌────────────┐  ┌──────────────┐  ┌──────┐  │
                  │  │  Thread 1  │  │  Thread 2    │  │ Thr 3│  │
                  │  │  aceitar   │  │  algoritmo   │  │ intf │  │
                  │  │ accept()   │  │ selectors    │  │input │  │
                  │  └─────┬──────┘  └──────┬───────┘  └──┬───┘  │
                  │        │                │             │      │
                  │        ▼                ▼             ▼      │
                  │   ┌──────────────────────────────────────┐   │
                  │   │  ESTADO (protegido por estado_lock)  │   │
                  │   │   fila_pedidos : deque[int]          │   │
                  │   │   clientes     : {pid -> socket}     │   │
                  │   │   processo_atual: int | None          │   │
                  │   │   atendidos    : {pid -> int}        │   │
                  │   └──────────────────────────────────────┘   │
                  │                                              │
                  └──────────────▲──────▲──────▲──────▲──────────┘
                                 │ TCP  │ TCP  │ TCP  │ TCP
                                 │      │      │      │
                       ┌─────────┘      │      │      └─────────┐
                       │                │      │                │
                  ┌────┴────┐      ┌────┴────┐ ...         ┌────┴────┐
                  │ PID 1   │      │ PID 2   │             │ PID n   │
                  │ proces. │      │ proces. │             │ proces. │
                  └─────────┘      └─────────┘             └─────────┘
                       │                │                       │
                       ▼                ▼                       ▼
                       └────────►  resultado.txt  ◄─────────────┘
                                   (append-only)
```

## 2. Loop de um processo

```
   ┌────────────────────────────────────────────────────────────────┐
   │                processo.py — run_processo(pid, r, k)           │
   │                                                                │
   │   socket.connect(coord)                                        │
   │           │                                                    │
   │           ▼                                                    │
   │   ┌──► REQUEST ─────────────────────────────►  coordenador     │
   │   │                                                            │
   │   │      [bloqueia em receber_completo()]                      │
   │   │                                                            │
   │   │       GRANT  ◄────────────────────────── coordenador       │
   │   │                                                            │
   │   │   ┌──── REGIÃO CRÍTICA ────────────┐                       │
   │   │   │  open("resultado.txt", "a")    │                       │
   │   │   │  write("PID x | HH:MM:SS.mmm") │                       │
   │   │   │  close()                       │                       │
   │   │   │  time.sleep(k)                 │                       │
   │   │   └────────────────────────────────┘                       │
   │   │                                                            │
   │   │   RELEASE ──────────────────────────────► coordenador      │
   │   │                                                            │
   │   └── repete r vezes                                           │
   │                                                                │
   │   sock.close()                                                 │
   └────────────────────────────────────────────────────────────────┘
```

## 3. Diagrama de sequência (3 processos competindo)

```
  P1                P2                P3              COORDENADOR    fila
  │                 │                 │                    │       [    ]
  │── REQUEST ─────►│                 │                    │       [    ]
  │                 │                 │                    │       [ 1  ]
  │◄── GRANT ──────────────────────────                    │       [    ]  titular=1
  │                 │                 │                    │
  │ [escreve]       │── REQUEST ─────►│                    │       [    ]
  │ [sleep k]       │                 │                    │       [ 2  ]
  │                 │                 │── REQUEST ────────►│       [2,3 ]
  │── RELEASE ──────────────────────────────────────────►  │       [2,3 ]  titular=None
  │                 │                 │                    │       [ 3  ]  titular=2
  │                 │◄── GRANT ─────────────────────────── │
  │                 │ [escreve]       │                    │
  │                 │ [sleep k]       │                    │
  │                 │── RELEASE ─────────────────────────► │       [    ]  titular=3
  │                 │                 │◄── GRANT ────────  │
  │                 │                 │ [escreve]          │
  │                 │                 │ [sleep k]          │
  │                 │                 │── RELEASE ───────► │       [    ]  titular=None
  │                 │                 │                    │

         ╳ tempo cresce para baixo ╳
```

## 4. Máquina de estados do coordenador (lógica do `tratar_*`)

```
            ┌─────────────────────────────────┐
            │     CR LIVRE  (titular = None)  │
            │     fila pode estar vazia       │
            └────────────┬────────────────────┘
                         │
                  recebe REQUEST de X
                         │
                  ┌──────┴──────┐
            fila vazia?         fila não-vazia?
                  │                 │
                  ▼                 ▼
       envia GRANT(X)        append(X) na fila
       titular ← X           (continua livre)
                  │                 │
                  └────────┬────────┘
                           ▼
            ┌─────────────────────────────────┐
            │     CR OCUPADA (titular = X)    │
            └────────────┬────────────────────┘
                         │
                  recebe RELEASE de X
                         │
                  atendidos[X] += 1
                  titular ← None
                         │
                  ┌──────┴──────┐
            fila vazia?         fila tem Y?
                  │                 │
                  ▼                 ▼
          volta para topo    envia GRANT(Y)
                             titular ← Y
                             popleft()
```

## 5. Por que tem o socket de "wakeup"?

```
  Thread aceitar                    Thread algoritmo
  ──────────────                    ────────────────
                                    sel.select(timeout=0.5)  ◄── BLOQUEADA
                                    │  esperando dados em
                                    │  sockets já registrados
  accept() nova conexão             │
  registra novo socket              │   (não vê o novo socket!)
  no seletor                        │
  ─── envia byte ──────────►   _acorda_w
                                    │   socket de wakeup fica "pronto"
                                    │
                                    ▼
                                    select retorna
                                    drena o byte
                                    loop reentra no select
                                    AGORA inclui o novo socket
```
