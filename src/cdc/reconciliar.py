"""Reconciliação origem × destino: contagem e soma de verificação por tabela.

A prova do case é esta comparação depois de cada falha injetada. Contagem igual não basta
(uma linha trocada por outra empata); por isso também se compara um hash do conteúdo,
calculado sobre as linhas ordenadas pela chave e com as colunas em ordem fixa.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


def _normalizar(linha: dict[str, Any], colunas: list[str]) -> str:
    return json.dumps([str(linha[c]) if linha[c] is not None else None for c in colunas])


def assinatura(linhas: list[dict[str, Any]], colunas: list[str]) -> str:
    h = hashlib.sha256()
    for linha in sorted(linhas, key=lambda r: int(r["id"])):
        h.update(_normalizar(linha, colunas).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


@dataclass(frozen=True)
class Resultado:
    tabela: str
    linhas_origem: int
    linhas_destino: int
    faltando: list[int]        # chaves na origem e não no destino
    sobrando: list[int]        # chaves no destino e não na origem
    divergentes: list[int]     # mesma chave, conteúdo diferente
    assinatura_igual: bool

    @property
    def ok(self) -> bool:
        return self.assinatura_igual and not (self.faltando or self.sobrando or self.divergentes)


def comparar(tabela: str, origem: list[dict[str, Any]], destino: list[dict[str, Any]],
             colunas: list[str]) -> Resultado:
    o = {int(r["id"]): _normalizar(r, colunas) for r in origem}
    d = {int(r["id"]): _normalizar(r, colunas) for r in destino}
    return Resultado(
        tabela=tabela, linhas_origem=len(o), linhas_destino=len(d),
        faltando=sorted(o.keys() - d.keys()), sobrando=sorted(d.keys() - o.keys()),
        divergentes=sorted(k for k in o.keys() & d.keys() if o[k] != d[k]),
        assinatura_igual=assinatura(origem, colunas) == assinatura(destino, colunas),
    )
