F = 16     # Tamanho fixo (em bytes) de toda mensagem
SEP = "|"  # Separador entre campos

MSG_REQUEST = "1"
MSG_GRANT   = "2"
MSG_RELEASE = "3"

def serializar(id_msg: str, pid: int) -> bytes:
    """Monta uma mensagem do protocolo com tamanho exato de F bytes"""
    base = f"{id_msg}{SEP}{pid}{SEP}"
    # ljust para preencher o resto com zeros, e encode para transformar em bytes
    return base.ljust(F, "0").encode("ascii")

def parsear(dados: bytes) -> tuple[str, int]:
    """Extrai (id_msg, pid) de uma mensagem recebida"""
    texto = dados.decode("ascii")
    partes = texto.split(SEP)
    return partes[0], int(partes[1])

def receber_completo(sock) -> bytes | None:
    """Essa função garante que recebemos exatamente F bytes"""
    buffer = b""
    while len(buffer) < F:
        try:
            pedaco = sock.recv(F - len(buffer))
        except (ConnectionResetError, OSError):
            return None
        if not pedaco:
            return None
        buffer += pedaco
    return buffer
