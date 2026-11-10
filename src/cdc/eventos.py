"""Leitura do envelope do Debezium (JSON, sem schema) para um evento simples.

O Debezium publica uma mensagem por mudança de linha: `op` (c=insert, u=update, d=delete,
r=leitura do snapshot inicial), a linha `before`/`after` e o bloco `source` com o LSN do
PostgreSQL. A identidade de um evento é (tabela, chave, lsn): a mesma mudança repetida pela
fila tem a mesma identidade, e é isso que torna o destino idempotente.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

OPERACOES = {"c", "u", "d", "r"}


@dataclass(frozen=True)
class Evento:
    tabela: str
    chave: int
    op: str
    lsn: int
    ts_ms: int
    linha: dict[str, Any] | None   # estado da linha depois da mudança; None no delete

    @property
    def identidade(self) -> tuple[str, int, int]:
        return (self.tabela, self.chave, self.lsn)


def de_debezium(chave: bytes | str | None, valor: bytes | str | None) -> Evento | None:
    """Converte uma mensagem do Debezium em Evento. Devolve None para tombstone (valor vazio)."""
    if valor is None or valor in (b"", ""):
        return None
    env = json.loads(valor)
    op = env.get("op")
    if op not in OPERACOES:
        raise ValueError(f"operação desconhecida no envelope: {op!r}")
    fonte = env["source"]
    linha = env.get("after") if op != "d" else None
    ref = env.get("before") if op == "d" else env.get("after")
    if chave is not None and chave not in (b"", ""):
        id_ = json.loads(chave)["id"]
    elif ref is not None:
        id_ = ref["id"]
    else:
        raise ValueError("evento sem chave e sem linha de referência")
    return Evento(tabela=fonte["table"], chave=int(id_), op=op, lsn=int(fonte["lsn"]),
                  ts_ms=int(env.get("ts_ms") or fonte.get("ts_ms") or 0), linha=linha)
