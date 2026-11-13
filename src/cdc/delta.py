"""Estado atual em Delta Lake (delta-rs): uma tabela Delta por tabela de origem, via MERGE.

O log bruto em Parquet (`cdc.destino`) guarda tudo o que chegou, com repetidos. Aqui fica a
camada que se consulta: uma linha por chave, sempre a da mudança mais nova. Três regras:

1. **Dedup do lote antes do MERGE:** dentro do lote fica só o evento de maior LSN de cada
   chave. Duas linhas da origem para a mesma chave fariam o MERGE falhar (ou escolher uma ao
   acaso).
2. **Só avança, nunca volta:** `when_matched_update_all(predicate="s.lsn > t.lsn")`. Um evento
   repetido (mesmo LSN) ou atrasado (LSN menor) não muda a linha. Reaplicar o mesmo lote não
   muda nada.
3. **Delete lógico:** o delete (`op='d'`) fica na tabela com `apagado = true` e o LSN dele.
   Assim um evento velho da mesma chave que chegue depois do delete perde para o LSN do delete
   e não ressuscita a linha. Quem lê filtra `apagado = false`.
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

from cdc.eventos import Evento

ESQUEMA = pa.schema([
    pa.field("chave", pa.int64(), nullable=False),
    pa.field("lsn", pa.int64(), nullable=False),   # o LSN do PostgreSQL cabe em int64
    pa.field("op", pa.string(), nullable=False),
    pa.field("ts_ms", pa.int64(), nullable=False),
    pa.field("apagado", pa.bool_(), nullable=False),
    pa.field("linha", pa.string()),                 # JSON da linha; nulo no delete
])


def caminho(raiz: Path, tabela: str) -> Path:
    return raiz / tabela


def _mais_novo_por_chave(eventos: Iterable[Evento]) -> list[Evento]:
    ultimo: dict[int, Evento] = {}
    for e in eventos:
        if e.chave not in ultimo or e.lsn > ultimo[e.chave].lsn:
            ultimo[e.chave] = e
    return [ultimo[k] for k in sorted(ultimo)]


def _tabela_arrow(eventos: list[Evento]) -> pa.Table:
    return pa.Table.from_pylist([
        {"chave": e.chave, "lsn": e.lsn, "op": e.op, "ts_ms": e.ts_ms,
         "apagado": e.op == "d",
         "linha": None if e.linha is None else json.dumps(e.linha, sort_keys=True)}
        for e in eventos], schema=ESQUEMA)


def aplicar_lote(raiz: Path, eventos: Iterable[Evento]) -> dict[str, int]:
    """Aplica o lote às tabelas Delta. Devolve, por tabela, quantas chaves o lote trouxe."""
    por_tabela: dict[str, list[Evento]] = {}
    for e in eventos:
        por_tabela.setdefault(e.tabela, []).append(e)
    aplicadas: dict[str, int] = {}
    for tabela, evs in por_tabela.items():
        fonte = _tabela_arrow(_mais_novo_por_chave(evs))
        dt = DeltaTable.create(str(caminho(raiz, tabela)), schema=ESQUEMA, mode="ignore")
        (dt.merge(fonte, predicate="t.chave = s.chave", source_alias="s", target_alias="t")
           .when_matched_update_all(predicate="s.lsn > t.lsn")
           .when_not_matched_insert_all()
           .execute())
        aplicadas[tabela] = fonte.num_rows
    return aplicadas


def estado_atual(raiz: Path, tabela: str) -> list[dict[str, Any]]:
    """Linhas vivas da tabela Delta, em ordem de chave."""
    alvo = caminho(raiz, tabela)
    if not (alvo / "_delta_log").exists():
        return []
    linhas = DeltaTable(str(alvo)).to_pyarrow_table(
        columns=["chave", "apagado", "linha"]).to_pylist()
    return [json.loads(r["linha"]) for r in sorted(linhas, key=lambda r: r["chave"])
            if not r["apagado"]]


def versoes(raiz: Path, tabela: str) -> int:
    """Quantos commits a tabela Delta tem (cada MERGE é um commit no `_delta_log`)."""
    alvo = caminho(raiz, tabela)
    return DeltaTable(str(alvo)).version() + 1 if (alvo / "_delta_log").exists() else 0
