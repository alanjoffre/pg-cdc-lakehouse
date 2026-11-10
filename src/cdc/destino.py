"""Destino idempotente: eventos brutos em Parquet e o estado atual calculado no DuckDB.

Duas regras garantem "nem perde, nem duplica":
1. O consumidor só confirma o offset no Kafka DEPOIS de gravar o arquivo (entrega
   pelo menos uma vez: na queda, a fila reentrega o que não foi confirmado).
2. A leitura descarta repetidos pela identidade (tabela, chave, lsn) e fica com o
   evento de maior LSN de cada chave. Reentregar o mesmo evento não muda o resultado.

O nome do arquivo vem do tópico, partição e faixa de offsets: reprocessar a mesma faixa
reescreve o mesmo arquivo em vez de criar outro.
"""
from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path

import duckdb

from cdc.eventos import Evento

_COLUNAS = "tabela VARCHAR, chave BIGINT, op VARCHAR, lsn UBIGINT, ts_ms BIGINT, linha VARCHAR"


def nome_arquivo(topico: str, particao: int, offset_ini: int, offset_fim: int) -> str:
    return f"{topico.replace('.', '_')}-p{particao}-{offset_ini:012d}-{offset_fim:012d}.parquet"


def gravar_lote(pasta: Path, eventos: Iterable[Evento], topico: str, particao: int,
                offset_ini: int, offset_fim: int) -> Path | None:
    """Grava o lote num Parquet (escrita atômica: arquivo temporário + rename)."""
    linhas = [(e.tabela, e.chave, e.op, e.lsn, e.ts_ms,
               None if e.linha is None else json.dumps(e.linha, sort_keys=True))
              for e in eventos]
    if not linhas:
        return None
    pasta.mkdir(parents=True, exist_ok=True)
    alvo = pasta / nome_arquivo(topico, particao, offset_ini, offset_fim)
    tmp = alvo.with_suffix(".parquet.tmp")
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE lote ({_COLUNAS})")
        con.executemany("INSERT INTO lote VALUES (?, ?, ?, ?, ?, ?)", linhas)
        con.execute(f"COPY lote TO '{tmp}' (FORMAT parquet)")
    finally:
        con.close()
    os.replace(tmp, alvo)
    return alvo


def _origem(pasta: Path) -> str:
    return f"read_parquet('{pasta}/*.parquet')"


def eventos_unicos(con: duckdb.DuckDBPyConnection, pasta: Path) -> duckdb.DuckDBPyRelation:
    """Todos os eventos gravados, sem repetidos (a mesma identidade conta uma vez)."""
    return con.sql(f"""
        SELECT tabela, chave, lsn, any_value(op) AS op, any_value(ts_ms) AS ts_ms,
               any_value(linha) AS linha
        FROM {_origem(pasta)}
        GROUP BY tabela, chave, lsn
    """)


def estado_atual(con: duckdb.DuckDBPyConnection, pasta: Path, tabela: str) -> list[dict]:
    """Linhas vivas da tabela: o evento de maior LSN de cada chave, sem as apagadas."""
    if not any(pasta.glob("*.parquet")):
        return []
    eventos_unicos(con, pasta).create_view("ev", replace=True)
    linhas = con.execute("""
        SELECT linha FROM (
            SELECT *, row_number() OVER (PARTITION BY chave ORDER BY lsn DESC) AS rn
            FROM ev WHERE tabela = ?
        ) WHERE rn = 1 AND op <> 'd'
        ORDER BY chave
    """, [tabela]).fetchall()
    return [json.loads(r[0]) for r in linhas]
