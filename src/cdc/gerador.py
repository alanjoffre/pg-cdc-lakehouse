"""Carga sintética e determinística na origem: a mesma semente gera as mesmas mudanças.

A carga inicial entra ANTES do conector existir (chega ao destino pelo snapshot, op="r");
as mudanças entram depois (op c/u/d, pela leitura do WAL). Nenhum dado real: nomes e
cidades saem de listas fixas.
"""
from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass, field
from decimal import Decimal

import psycopg

DSN = "host=localhost port=55432 dbname=loja user=cdc password=cdc-local-somente"
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Hugo", "Íris", "João"]
SOBRENOMES = ["Almeida", "Barros", "Costa", "Dias", "Esteves", "Farias", "Gomes", "Lima"]
CIDADES = ["São Paulo", "Campinas", "Santos", "Curitiba", "Recife", "Belém", "Goiânia"]
STATUS = ["novo", "pago", "enviado", "entregue", "cancelado"]


@dataclass
class Estado:
    """O que a origem tem, mantido do lado do gerador para escolher alvos sem consultar."""
    rng: random.Random
    clientes: list[int] = field(default_factory=list)
    pedidos: list[int] = field(default_factory=list)
    prox_cliente: int = 1
    prox_pedido: int = 1


def _valor(rng: random.Random) -> Decimal:
    return Decimal(rng.randint(500, 250_000)) / 100


def carga_inicial(con: psycopg.Connection, est: Estado, n_clientes: int, n_pedidos: int) -> None:
    with con.cursor() as cur:
        for _ in range(n_clientes):
            nome = f"{est.rng.choice(NOMES)} {est.rng.choice(SOBRENOMES)}"
            cur.execute("INSERT INTO clientes (id, nome, cidade) VALUES (%s, %s, %s)",
                        (est.prox_cliente, nome, est.rng.choice(CIDADES)))
            est.clientes.append(est.prox_cliente)
            est.prox_cliente += 1
        for _ in range(n_pedidos):
            cur.execute("INSERT INTO pedidos (id, cliente_id, valor, status) "
                        "VALUES (%s, %s, %s, 'novo')",
                        (est.prox_pedido, est.rng.choice(est.clientes), _valor(est.rng)))
            est.pedidos.append(est.prox_pedido)
            est.prox_pedido += 1
    con.commit()


def mudancas(con: psycopg.Connection, est: Estado, n: int, por_transacao: int = 20,
             pausa_s: float = 0.02) -> dict[str, int]:
    """n mudanças: 30% pedido novo, 50% atualização (pedido ou cliente), 20% pedido apagado."""
    cont = {"insert": 0, "update": 0, "delete": 0}
    with con.cursor() as cur:
        for i in range(n):
            sorteio = est.rng.random()
            if sorteio < 0.3 or len(est.pedidos) < 10:
                cur.execute("INSERT INTO pedidos (id, cliente_id, valor, status) "
                            "VALUES (%s, %s, %s, 'novo')",
                            (est.prox_pedido, est.rng.choice(est.clientes), _valor(est.rng)))
                est.pedidos.append(est.prox_pedido)
                est.prox_pedido += 1
                cont["insert"] += 1
            elif sorteio < 0.7:
                cur.execute("UPDATE pedidos SET status = %s, valor = %s, atualizado = now() "
                            "WHERE id = %s",
                            (est.rng.choice(STATUS), _valor(est.rng), est.rng.choice(est.pedidos)))
                cont["update"] += 1
            elif sorteio < 0.8:
                cur.execute("UPDATE clientes SET cidade = %s, atualizado = now() WHERE id = %s",
                            (est.rng.choice(CIDADES), est.rng.choice(est.clientes)))
                cont["update"] += 1
            else:
                alvo = est.pedidos.pop(est.rng.randrange(len(est.pedidos)))
                cur.execute("DELETE FROM pedidos WHERE id = %s", (alvo,))
                cont["delete"] += 1
            if (i + 1) % por_transacao == 0:
                con.commit()
                time.sleep(pausa_s)
    con.commit()
    return cont


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--semente", type=int, default=42)
    ap.add_argument("--clientes", type=int, default=200)
    ap.add_argument("--pedidos", type=int, default=1000)
    ap.add_argument("--mudancas", type=int, default=0)
    a = ap.parse_args()
    est = Estado(random.Random(a.semente))
    with psycopg.connect(DSN) as con:
        carga_inicial(con, est, a.clientes, a.pedidos)
        if a.mudancas:
            print(mudancas(con, est, a.mudancas))


if __name__ == "__main__":
    main()
