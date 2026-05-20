"""Protocolo de mensagens de tamanho fixo e relogio logico de Lamport.

Formato da mensagem (string ASCII de F bytes):
    <msg_id>|<pid>|<lamport>|<padding ate F bytes>

- msg_id: REQUEST=1, GRANT=2, RELEASE=3
- pid:    identificador inteiro do processo remetente
- lamport: valor do relogio logico de Lamport do remetente no envio
- padding: caracteres '.' ate completar F bytes

Como TCP e stream-oriented, o leitor deve garantir a leitura de exatamente F bytes.
"""


import socket
import threading

F = 24 # Tamanho da mensagem
SEP = "|"
PAD = "."

# Tipo da mensagem
REQUEST = 1
GRANT = 2
RELEASE = 3

TIPO_NOME = {REQUEST: "REQUEST", GRANT: "GRANT", RELEASE: "RELEASE"}


def encode(msg_id: int, pid: int, lamport: int) -> bytes:
    """Serializa uma mensagem em exatamente F bytes ASCII."""
    if msg_id not in TIPO_NOME:
        raise ValueError(f"msg_id invalido: {msg_id}")
    base = f"{msg_id}{SEP}{pid}{SEP}{lamport}{SEP}"
    if len(base) > F:
        raise ValueError(
            f"Mensagem excede F={F} bytes (msg_id={msg_id}, pid={pid}, lamport={lamport})"
        )
    payload = base + PAD * (F - len(base))
    return payload.encode("ascii")


def decode(buf: bytes) -> tuple[int, int, int]:
    """Desserializa F bytes em (msg_id, pid, lamport)."""
    if len(buf) != F:
        raise ValueError(f"Tamanho invalido: esperado {F}, recebido {len(buf)}")
    txt = buf.decode("ascii")
    partes = txt.split(SEP)
    if len(partes) < 4:
        raise ValueError(f"Formato invalido: {txt!r}")
    msg_id = int(partes[0])
    pid = int(partes[1])
    lamport = int(partes[2])
    if msg_id not in TIPO_NOME:
        raise ValueError(f"msg_id desconhecido: {msg_id}")
    return msg_id, pid, lamport


def recv_exato(sock: socket.socket, n: int = F) -> bytes:
    """Le exatamente n bytes do socket. Retorna b'' se a conexao fechar."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""
        buf.extend(chunk)
    return bytes(buf)


class LamportClock:
    """Relogio logico de Lamport thread-safe.

    Regras:
      - tick(): evento local / antes de enviar -> value += 1, retorna value.
      - update(recebido): ao receber -> value = max(value, recebido) + 1, retorna value.
    """

    def __init__(self, inicial: int = 0) -> None:
        self._value = inicial
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update(self, recebido: int) -> int:
        with self._lock:
            self._value = max(self._value, recebido) + 1
            return self._value
