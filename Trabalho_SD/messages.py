# messages.py — Protocolo de mensagens para exclusão mútua distribuída centralizada
#
# Formato de mensagem (F bytes fixos, separador '|'):
#   <type>|<proc_id>|<msg_id>|<padding zeros>
#
# Exemplo (F=20): "1|3|42|00000000000\n" (sem \n na transmissão)

F = 20          # tamanho fixo da mensagem em bytes
SEPARATOR = "|"

# Tipos de mensagem
REQUEST = 1     # processo → coordenador: solicita região crítica
GRANT   = 2     # coordenador → processo: concede região crítica
RELEASE = 3     # processo → coordenador: libera região crítica

MSG_NAMES = {REQUEST: "REQUEST", GRANT: "GRANT", RELEASE: "RELEASE"}


def encode(msg_type: int, proc_id: int, msg_id: int) -> bytes:
    """Codifica uma mensagem no formato fixo de F bytes."""
    base = f"{msg_type}{SEPARATOR}{proc_id}{SEPARATOR}{msg_id}{SEPARATOR}"
    if len(base) > F:
        raise ValueError(f"Mensagem base '{base}' excede F={F} bytes")
    padded = base.ljust(F, "0")
    return padded.encode("ascii")


def decode(raw: bytes) -> tuple[int, int, int]:
    """Decodifica uma mensagem de F bytes.
    Retorna (msg_type, proc_id, msg_id).
    """
    text = raw.decode("ascii").rstrip("0")
    parts = text.split(SEPARATOR)
    # parts pode ser ['1', '3', '42', ''] após o strip
    msg_type = int(parts[0])
    proc_id  = int(parts[1])
    msg_id   = int(parts[2])
    return msg_type, proc_id, msg_id


def recv_message(sock) -> tuple[int, int, int]:
    """Recebe exatamente F bytes de um socket e decodifica."""
    data = b""
    while len(data) < F:
        chunk = sock.recv(F - len(data))
        if not chunk:
            raise ConnectionError("Conexão encerrada pelo par")
        data += chunk
    return decode(data)
