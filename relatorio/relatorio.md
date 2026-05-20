# Relatório — Exclusão Mútua Centralizada em Sistemas Distribuídos

> Trabalho da disciplina de **Sistemas Concorrentes Distribuídos**
> Implementação em **Python** com sockets TCP, relógio lógico de Lamport e
> sleep aleatório fora da região crítica.

---

## 1. O problema, em linguagem do dia a dia

Imagine um **banheiro único** numa casa onde moram várias pessoas. Só uma
pessoa pode entrar de cada vez. Se duas tentarem entrar ao mesmo tempo, dá
confusão. Como organizar isso?

Uma solução simples: **alguém na sala fica com a chave**. Quem quer usar o
banheiro pede a chave, espera ser chamado, usa o banheiro, devolve a chave.
Quem ficou com a chave (o **coordenador**) garante que só entrega ela para
uma pessoa por vez, e atende todo mundo na ordem em que pediu.

É exatamente isso que esse trabalho implementa, só que:

- o "banheiro" é um trecho de código chamado **região crítica** (RC) — pode
  ser uma escrita em arquivo, em banco de dados, num dispositivo etc.;
- as "pessoas" são **processos** rodando em paralelo, possivelmente em
  computadores diferentes;
- a "chave" é uma **autorização (GRANT)** enviada pela rede.

Esse é o **algoritmo centralizado de exclusão mútua distribuída**.

---

## 2. Por que esse problema é difícil?

Num computador só, dá pra resolver isso com um "mutex" (uma trava da
linguagem). Mas em **sistemas distribuídos**, os processos estão em máquinas
diferentes, conversando por rede. A rede:

- pode atrasar mensagens;
- pode entregar mensagens fora de ordem;
- não tem um relógio físico unificado entre máquinas.

Ou seja, **não dá para confiar no relógio do servidor A para decidir o que
aconteceu antes no servidor B**. Precisamos de mecanismos próprios para:

1. Garantir que **só um processo** entra na região crítica de cada vez.
2. Saber a **ordem** em que os eventos aconteceram.
3. Registrar tudo num **log** para depois conseguir validar.

---

## 3. Os ingredientes da solução

### 3.1. Coordenador (a pessoa com a chave)

Um programa **servidor** que escuta na rede. Ele guarda:

- a lista de processos conectados;
- uma **fila** de quem está esperando entrar na região crítica;
- contadores de quantas vezes cada processo já entrou.

Sempre que alguém pede para entrar, o coordenador coloca esse "alguém" no
final da fila. Sempre que o coordenador termina de atender um pedido, ele
pega o próximo da fila e dá a vez.

### 3.2. Processos clientes (as pessoas que querem usar o banheiro)

Cada processo:

1. Espera um tempinho aleatório (entre 3 e 4 segundos).
2. Pede a vez ao coordenador (mensagem `REQUEST`).
3. Espera o coordenador dizer "pode entrar" (mensagem `GRANT`).
4. Faz seu trabalho na região crítica (escreve no arquivo `resultado.txt`
   e dorme `k` segundos).
5. Avisa que terminou (mensagem `RELEASE`).
6. Repete tudo `r` vezes.

### 3.3. Mensagens (REQUEST, GRANT, RELEASE)

São pequenos textos enviados pela rede. Como TCP entrega bytes em "fluxo
contínuo" (sem fronteiras), padronizamos: **toda mensagem tem exatamente 24
bytes**, com este formato:

```
<tipo>|<pid>|<lamport>|<padding com pontinhos até 24 bytes>
```

- `tipo`: `1`=REQUEST, `2`=GRANT, `3`=RELEASE
- `pid`: o número que identifica o processo (0, 1, 2, ...)
- `lamport`: o valor do "relógio lógico" no momento do envio (explicado já já)
- `padding`: pontos `.` para completar 24 bytes (analogia: papel quadriculado)

Exemplo: `1|7|42|.................` significa "processo 7 mandou um REQUEST
com Lamport=42".

**Analogia:** é como mandar **postais com tamanho padronizado** pelos
correios. O leitor sabe que cada postal tem o mesmo tamanho, então é fácil
separar onde começa e termina cada um.

### 3.4. Relógio lógico de Lamport (a "ordem causal")

Em sistemas distribuídos, dois computadores podem ter o relógio físico
desalinhado em milissegundos. Para descobrir **quem fez o quê antes**,
inventou-se em 1978 um truque simples chamado **relógio lógico de Lamport**:

- Cada processo mantém um **contador inteiro**, começando em zero.
- **Toda vez que envia uma mensagem**, incrementa o contador antes (`L = L + 1`).
- **Toda vez que recebe uma mensagem**, pega o maior valor entre o seu e o
  do remetente, e soma 1: `L = max(L_meu, L_recebido) + 1`.

Por que isso funciona? Porque se um evento A **causa** outro evento B (por
exemplo, A é um envio e B é o recebimento dessa mensagem), então o Lamport
de A será sempre **menor** que o de B. Em outras palavras, esse contador
respeita a **relação de causalidade**.

**Analogia:** imagine que cada vez que você manda um WhatsApp, escreve no
canto da mensagem o número do "evento" da sua vida. Quando seu amigo
recebe, ele anota: "o maior entre o meu e o seu, mais 1". Assim, mesmo se os
relógios da hora estiverem desencontrados, dá pra reconstruir a sequência
de quem reagiu a quem.

No log do coordenador, registramos **dois** timestamps:
- o **físico** (hora do computador, em milissegundos);
- o **lógico de Lamport** (essa nossa contagem).

Isso permite mostrar no relatório que mesmo se os relógios físicos
"andassem mal", a ordem causal estaria preservada.

### 3.5. Sleep aleatório fora da região crítica

Se todos os processos começarem juntos e fizerem `REQUEST` no mesmo
instante, **eles chegam todos colados no coordenador**. Para tornar o
experimento mais realista (processos chegando em momentos diferentes),
cada processo **dorme entre 3 e 4 segundos antes de cada REQUEST**. Esse
sleep é **fora** da região crítica, ou seja, **não conta** como tempo de uso
do recurso — ele só serve para dessincronizar.

**Analogia:** em vez de todo mundo bater na porta do banheiro ao mesmo
tempo, cada pessoa decide aleatoriamente "vou esperar uns segundos antes
de ir lá".

---

## 4. Arquitetura geral

```
        +-------------+         +-------------+
        | processo 0  |         | processo 1  |
        +-------------+         +-------------+
              |                       |
              | (TCP)                 | (TCP)
              v                       v
        +-----------------------------------+
        |          COORDENADOR              |
        |                                   |
        |  fila: [0, 1, ...]                |
        |  clientes: {0: sock0, 1: sock1}   |
        |  Lamport=N  atendidos={0:..,1:..} |
        |                                   |
        |  threads:                         |
        |    - accept_loop                  |
        |    - handle_client (1 por cliente)|
        |    - mutex_loop                   |
        |    - interface (terminal)         |
        +-----------------+-----------------+
                          |
                          v
                  coordenador.log
                  resultado.txt (gravado pelos clientes,
                  mas serializado pelo coordenador)
```

### Por que múltiplas threads no coordenador?

- **`accept_loop`** fica só aceitando conexões novas. Se isso ficasse junto
  com o "atendimento", o servidor poderia travar de aceitar enquanto
  estivesse processando uma mensagem.
- **`handle_client`** (uma por cliente) é a "orelha" do coordenador para
  aquele processo. Lê mensagens conforme chegam e empurra para a fila.
- **`mutex_loop`** é o "cérebro": pega o primeiro da fila, manda GRANT,
  espera o RELEASE, repete.
- **`interface`** é o "teclado": permite ao operador digitar `1`, `2` ou `3`
  no terminal do coordenador para inspecionar fila, ver atendidos ou
  encerrar.

Sem múltiplas threads, o coordenador teria que escolher entre **escutar a
rede** e **ler o teclado**. Threads permitem fazer ambos.

---

## 5. Estrutura de arquivos do projeto

```
Sistemas-Concorrentes-Distribuidos/
├── protocolo.py                  -> mensagens + LamportClock
├── coordenador.py                -> o servidor com as 4 threads
├── processo.py                   -> o cliente, executa r iterações
├── launcher.py                   -> dispara n clientes de uma vez
├── verificar.py                  -> valida resultado.txt e coordenador.log
├── experimentos/
│   ├── rodar_experimentos.py     -> matriz de cenários + métricas
│   └── resultados/               -> uma pasta por cenário
├── relatorio/
│   └── relatorio.md              -> este documento
├── plan.md                       -> plano detalhado do trabalho
├── README.md                     -> instruções rápidas
└── Trabalho_SD_ExclusaoMutua.pdf -> enunciado original
```

### 5.1. `protocolo.py`

Define **três coisas**:

1. **Tamanho fixo `F = 24` bytes** e funções `encode`/`decode` para serializar
   uma mensagem em bytes e voltar.
2. Função `recv_exato(sock, n)` que lê **exatamente** `n` bytes de um socket
   TCP. Sem isso, poderíamos receber só metade de uma mensagem e bagunçar
   tudo.
3. Classe `LamportClock` com `tick()` (evento local / envio) e
   `update(recebido)` (recebimento). Internamente usa um `Lock` para ser
   segura quando várias threads incrementam ao mesmo tempo.

### 5.2. `coordenador.py`

Implementa as 4 threads descritas acima. Pontos importantes:

- A **fila** é um `collections.deque` protegido por um `threading.Condition`.
  O `Condition` permite ao `mutex_loop` ficar bloqueado **sem gastar CPU**
  enquanto a fila estiver vazia, e ser "acordado" automaticamente quando
  alguém chega.
- A espera pelo `RELEASE` correto usa um `threading.Event`: quando o
  cliente "X" recebe GRANT, o coordenador grava `release_pid_esperado = X`
  e fica esperando o evento. Se chega RELEASE de outro processo (não
  deveria acontecer), é ignorado e registrado como erro.
- O **log** é escrito numa única função `log_evento` protegida por um lock,
  para garantir que linhas de threads diferentes não se misturem.

### 5.3. `processo.py`

Programa cliente. Recebe `--pid`, `--host`, `--porta`, `--k`, `--r` e segue o
roteiro:

1. Dorme aleatório entre 3 e 4 s.
2. `tick()` no Lamport, envia `REQUEST`.
3. Aguarda `GRANT`, atualiza Lamport com o valor recebido.
4. Abre `resultado.txt` em modo *append* e escreve uma linha
   `<pid>|<timestamp_físico>|<lamport>`.
5. Dorme `k` segundos (uso "real" da região crítica).
6. `tick()` no Lamport, envia `RELEASE`.
7. Repete tudo `r` vezes e termina.

Como o coordenador garante que só um cliente está na RC por vez, o `append`
em `resultado.txt` é seguro — não há dois processos escrevendo
simultaneamente.

### 5.4. `launcher.py`

Usa `argparse` para receber `--n --k --r --host --porta` e dispara `n`
instâncias de `processo.py` em paralelo via `subprocess.Popen`. Espera
todos terminarem com `proc.wait()` e imprime o tempo total. Há uma flag
`--limpar` que apaga `resultado.txt` antes de começar (útil entre rodadas).

### 5.5. `verificar.py`

Programa de **auditoria**. Lê `resultado.txt` e `coordenador.log` e
verifica:

- (a) `resultado.txt` tem exatamente `n*r` linhas;
- (b) timestamps físicos no `resultado.txt` são **não-decrescentes** (o
  tempo nunca anda para trás);
- (c) os valores de Lamport no `resultado.txt` são **estritamente
  crescentes** (cada entrada na RC é um evento distinto);
- (d) cada `pid` aparece exatamente `r` vezes (justiça absoluta);
- (e) no log do coordenador, cada `GRANT` enviado a um pid X é seguido por
  um `RELEASE` do mesmo X antes do próximo `GRANT`.

Se algum check falha, ele imprime a lista de problemas e sai com código 1.
Se passa tudo, imprime `OK`.

### 5.6. `experimentos/rodar_experimentos.py`

Para cada combinação de `n`, `k`, `r` da matriz definida no `plan.md`:

1. Cria pasta `experimentos/resultados/<cenário>/`.
2. Sobe um coordenador isolado numa porta livre.
3. Roda o launcher.
4. Encerra o coordenador via comando `3` no stdin.
5. Calcula métricas:
   - **tempo total** da rodada;
   - **throughput** (RCs/segundo);
   - **tamanho médio e máximo da fila** ao longo do tempo;
   - **justiça**: variância do número de atendimentos por pid;
   - **drift Lamport vs. físico**: o quanto o relógio lógico "diverge" do
     físico (uma medida ordinal normalizada).
6. Salva um `metricas.json` por cenário e um `resumo.json` agregado.

A flag `--rapido` reduz o sleep aleatório para 0.1–0.3 s, só para validar
rapidamente que a infraestrutura funciona, **sem ser o cenário oficial**.

---

## 6. Fluxo passo a passo (uma execução completa)

Vamos seguir o que acontece numa rodada com 2 processos e 2 iterações cada:

1. **Coordenador sobe**, abre porta TCP, espera conexões.
2. **Launcher dispara 2 processos.** Cada um abre conexão TCP com o
   coordenador. O coordenador cria duas threads `handle_client`.
3. **Processo 0 dorme 3.5 s, envia REQUEST.** Coordenador recebe, atualiza
   Lamport, coloca pid=0 na fila.
4. **`mutex_loop` acorda** porque a fila tem alguém, pega pid=0, envia
   GRANT.
5. **Processo 0 recebe GRANT**, escreve no `resultado.txt`, dorme `k=0.1 s`,
   envia RELEASE.
6. **Coordenador recebe RELEASE**, sinaliza `release_event`. `mutex_loop`
   acorda, vê fila vazia, volta a dormir.
7. **Processo 1 dorme 3.7 s, envia REQUEST.** Mesma sequência: GRANT,
   escrita, RELEASE.
8. Cada processo repete o ciclo `r=2` vezes. **Total: 4 entradas em
   `resultado.txt`** e 12 linhas no log (4 REQUESTs + 4 GRANTs + 4 RELEASEs).
9. Processos fecham conexão. Operador digita `3` no terminal do
   coordenador, que encerra todas as threads.

---

## 7. Decisões de projeto e por quê

| Decisão | Motivo |
| --- | --- |
| **TCP** (e não UDP) | Garante entrega ordenada e sem perdas. UDP exigiria implementar retransmissão. |
| **Tamanho fixo de 24 bytes** | Simplifica leitura: lemos exatamente 24 bytes por mensagem; não precisamos de "delimitador de fim". |
| **`deque` + `Condition`** (em vez de `queue.Queue`) | Precisamos **inspecionar** a fila (comando `1` da interface), o que `queue.Queue` não permite facilmente. |
| **Uma thread por cliente no coordenador** | Permite escutar todos os clientes em paralelo sem `select`. Simples e eficiente para `n` moderado. |
| **`LamportClock` com `Lock`** | Acessado por várias threads ao mesmo tempo. Sem lock, dois `tick()` simultâneos poderiam dar o mesmo valor. |
| **Append em `resultado.txt` por processo** | A exclusão mútua é garantida pelo coordenador, então **não há concorrência real** na escrita. |
| **Log único no coordenador** | Centraliza a evidência. Quem está auditando precisa olhar **um** arquivo, não `n+1`. |
| **Sleep aleatório fora da RC** | Dessincroniza chegadas. Sem isso, todos os REQUEST chegariam empilhados no início. |

---

## 8. Garantias do algoritmo

1. **Exclusão mútua:** o `mutex_loop` envia GRANT, espera `release_event`,
   e só então envia o próximo GRANT. Logo, **em nenhum momento dois
   processos estão na RC simultaneamente**.
2. **Ausência de starvation (justiça):** a fila é FIFO. Quem chega primeiro
   é atendido primeiro. Não tem como um processo "passar na frente".
3. **Ordem causal preservada:** o relógio de Lamport garante que se
   `evento A → evento B` (A causou B), então `Lamport(A) < Lamport(B)`.
   No `resultado.txt`, os Lamports são estritamente crescentes — sinal de
   que a sequência de entradas na RC respeita causalidade.
4. **Sem deadlock:** o coordenador só espera RELEASE depois de mandar
   GRANT. Não há ciclo de dependência possível.

---

## 9. Testando o funcionamento

### 9.1. Requisitos

- Python 3.10+ (testado com 3.12).
- Apenas a biblioteca padrão; **nenhuma dependência externa**.

### 9.2. Smoke test (rodada mínima)

Abra **dois terminais** na pasta do projeto.

**Terminal 1 — coordenador:**

```powershell
python coordenador.py --porta 5000
```

Você verá:

```
[coord] escutando em 0.0.0.0:5000
[1] mostrar fila  [2] mostrar atendidos  [3] encerrar
>
```

**Terminal 2 — launcher** (apaga `resultado.txt` antes):

```powershell
python launcher.py --n 2 --k 0.1 --r 3 --host 127.0.0.1 --porta 5000 --limpar
```

Saída esperada:

```
[launcher] 2 processos iniciados; aguardando termino...
[launcher] todos finalizados em 10.45s (rc=0)
```

> O tempo total fica próximo de `r * 3.5 s ≈ 10–12 s` por causa do sleep
> aleatório fora da RC.

**Enquanto roda**, no terminal do coordenador você pode digitar:

- `1` e ENTER → mostra a fila no momento (ex.: `fila: [0]`);
- `2` e ENTER → mostra `atendidos: {0: 2, 1: 1}`;
- `3` e ENTER → encerra o coordenador.

### 9.3. Verificar corretude automaticamente

Depois que tudo terminou:

```powershell
python verificar.py --resultado resultado.txt --log coordenador.log --n 2 --r 3
```

Saída esperada:

```
OK: 6 entradas validas, log consistente.
```

Se algo der errado, ele lista os problemas. Exemplo de falha hipotética:

```
FALHA:
  - resultado.txt tem 5 linhas; esperado 6
  - log: RELEASE de pid=1 esperava pid=0
```

### 9.4. Inspeção manual

Abra `resultado.txt`. Deve ter algo assim:

```
0|2026-05-13T12:00:03.512|4
1|2026-05-13T12:00:07.103|8
0|2026-05-13T12:00:10.811|12
1|2026-05-13T12:00:14.402|16
0|2026-05-13T12:00:18.110|20
1|2026-05-13T12:00:21.700|24
```

Como ler cada linha:
- **`0`** = pid do processo que estava na RC.
- **`2026-05-13T12:00:03.512`** = hora física da entrada (com
  milissegundos).
- **`4`** = valor do relógio de Lamport do processo naquele momento.

Observe que:
- Os **timestamps físicos sempre crescem** (linha N tem tempo ≥ linha N-1).
- Os **Lamports também crescem** (estritamente), mas com "saltos" — ex.: de
  4 para 8. Isso é normal: entre uma entrada e a próxima, o processo
  envia/recebe outras mensagens que também incrementam o contador.
- Cada `pid` aparece **`r` vezes** (no exemplo, `r=3`).

Abra `coordenador.log`. Cada linha tem o formato:

```
<timestamp_físico>|<lamport_coord>|<direção>|<tipo>|<pid>|<lamport_msg>
```

Exemplo:

```
2026-05-13T12:00:03.500|2|RECV|REQUEST|0|1
2026-05-13T12:00:03.500|3|SEND|GRANT|0|3
2026-05-13T12:00:03.611|6|RECV|RELEASE|0|5
```

Tradução:
- "Às 12:00:03.500, com meu Lamport=2, recebi REQUEST do pid=0 com Lamport=1
  (e atualizei meu Lamport para 3)."
- "Ainda às 12:00:03.500, com Lamport=3, enviei GRANT para pid=0."
- "Às 12:00:03.611, com Lamport=6, recebi RELEASE do pid=0 com Lamport=5."

O padrão deve ser sempre **REQUEST → GRANT → RELEASE → REQUEST → ...**
intercalado. Nunca dois GRANTs seguidos sem o RELEASE entre eles.

### 9.5. Carga moderada (sem deadlock)

```powershell
# Terminal 1
python coordenador.py --porta 5001 --log coordenador.log

# Terminal 2
python launcher.py --n 8 --k 0.05 --r 10 --host 127.0.0.1 --porta 5001 --limpar
python verificar.py --resultado resultado.txt --log coordenador.log --n 8 --r 10
```

Esperado: `resultado.txt` com **80 linhas**, `OK` do verificador.

### 9.6. Matriz completa de experimentos

```powershell
python experimentos/rodar_experimentos.py
```

Isso roda **todos** os cenários do `plan.md`:

- **Variar n:** `n ∈ {2, 4, 8, 16}` com `k=0.1`, `r=5`.
- **Variar k:** `k ∈ {0, 0.05, 0.2, 0.5}` com `n=4`, `r=5`.
- **Variar r:** `r ∈ {3, 5, 10}` com `n=4`, `k=0.1`.

Cada cenário tem sua pasta em `experimentos/resultados/<nome>/` com
`resultado.txt`, `coordenador.log` e `metricas.json`. O `resumo.json` na
pasta `resultados/` agrega todas as métricas.

> **Atenção:** com sleep de 3–4 s por iteração, a matriz completa leva
> tempo considerável (cada cenário roda ~`r * 3.5s`). Para validar
> rapidamente a infraestrutura (não os números do relatório oficial), use
> `--rapido`, que reduz o sleep para 0.1–0.3 s.

### 9.7. Interpretando as métricas

Ao abrir `experimentos/resultados/<cenário>/metricas.json`:

```json
{
  "n": 4,
  "r": 5,
  "tempo_total_s": 14.83,
  "rcs_executadas": 20,
  "throughput_rcs_por_s": 1.349,
  "tamanho_medio_fila": 1.47,
  "tamanho_max_fila": 4,
  "atendidos_por_pid": [5, 5, 5, 5],
  "variancia_justica": 0.0,
  "lamport_min": 4,
  "lamport_max": 86,
  "drift_lamport_vs_fisico": 0.012
}
```

Como ler:

- **`throughput_rcs_por_s`**: quantas vezes por segundo um processo
  conseguiu entrar na RC. Limitado pelo sleep aleatório + `k`.
- **`tamanho_medio_fila`**: em média, quantos processos estavam
  aguardando. Cresce com `n` e diminui com sleep maior.
- **`tamanho_max_fila`**: pico de contenção observado.
- **`atendidos_por_pid`**: deve ter o mesmo valor para todos os pids
  (justiça perfeita).
- **`variancia_justica`**: idealmente **zero** — se algum pid for
  atendido menos vezes, esse número sobe.
- **`drift_lamport_vs_fisico`**: aproximadamente 0 indica que a ordem
  Lamport segue a ordem física. Valores próximos de 1 indicariam
  "divergência" — não acontece em condições normais, porque a única fonte
  de ordem aqui é o coordenador.

### 9.8. O que esperar com variações de `n`, `k` e `r`

- **Aumentar `n`** (mais processos): a fila tende a ficar maior, o tempo
  total cresce porque cada processo espera mais para ser atendido, mas o
  throughput total **não muda muito** (o gargalo é o `k`).
- **Aumentar `k`** (mais tempo na RC): o throughput **cai** linearmente,
  porque o coordenador atende um por vez e cada um demora mais. A fila
  também cresce.
- **Aumentar `r`** (mais iterações por processo): o tempo total cresce
  proporcionalmente, mas o regime estacionário (fila média) é igual.

---

## 10. Conclusão

O trabalho implementa o **algoritmo centralizado** de exclusão mútua
distribuída em Python puro (stdlib), com:

- **Sockets TCP** para a comunicação;
- **Mensagens de tamanho fixo (24 bytes)** para evitar problemas de
  fragmentação do protocolo;
- **Relógio lógico de Lamport** em todos os processos e no coordenador
  para preservar ordem causal mesmo sem relógio físico sincronizado;
- **Sleep aleatório fora da região crítica** para gerar um cenário mais
  realista;
- **Múltiplas threads no coordenador** para aceitar conexões, escutar
  clientes, rodar o algoritmo e atender a interface do operador
  simultaneamente;
- **Verificador automático** que checa corretude (exclusão mútua,
  consistência causal, justiça e contagem de eventos);
- **Bateria de experimentos** que varia `n`, `k` e `r` e produz
  métricas comparáveis.

Os testes locais (smoke test 2×3 e carga 4×5) passaram pelo verificador
sem erros, confirmando que **nenhum processo entrou na região crítica ao
mesmo tempo que outro**, **todos foram atendidos a mesma quantidade de
vezes** e **a ordem causal foi preservada**.

---

## 11. Glossário rápido

| Termo | Significado |
| --- | --- |
| **Região Crítica (RC)** | Trecho de código que só pode ser executado por um processo de cada vez. |
| **Exclusão mútua** | Propriedade de garantir que dois processos nunca estejam ao mesmo tempo na RC. |
| **Coordenador** | Processo central que arbitra o acesso à RC. |
| **REQUEST / GRANT / RELEASE** | "Quero entrar" / "Pode entrar" / "Já saí". |
| **PID** | Process ID, número único de cada processo cliente. |
| **TCP** | Protocolo de rede confiável, ordenado, baseado em conexão. |
| **Socket** | "Tomada" de rede; ponta de uma conexão TCP. |
| **Thread** | Linha de execução paralela dentro de um mesmo processo. |
| **Lock / Mutex** | Trava que impede duas threads de acessarem algo ao mesmo tempo. |
| **Lamport Clock** | Contador lógico que respeita relação causal entre eventos distribuídos. |
| **Throughput** | Quantidade de operações concluídas por unidade de tempo. |
| **Starvation** | Situação em que um processo nunca consegue ser atendido. |
| **Deadlock** | Situação em que processos ficam esperando um pelo outro para sempre. |
