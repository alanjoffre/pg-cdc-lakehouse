"""O coração do case: reentregar, repetir ou embaralhar eventos não muda o destino."""
import json

import duckdb

from cdc.destino import colisoes_de_identidade, estado_atual, gravar_lote, nome_arquivo
from cdc.eventos import Evento, de_debezium
from cdc.reconciliar import comparar


def ev(chave, op, lsn, **linha):
    return Evento("pedidos", chave, op, lsn, 0, None if op == "d" else {"id": chave, **linha})


def _estado(pasta):
    con = duckdb.connect()
    try:
        return estado_atual(con, pasta, "pedidos")
    finally:
        con.close()


def test_insert_update_delete(tmp_path):
    gravar_lote(tmp_path, [ev(1, "c", 10, valor="5"), ev(2, "c", 11, valor="7"),
                           ev(1, "u", 12, valor="9"), ev(2, "d", 13)], "t", 0, 0, 3)
    assert _estado(tmp_path) == [{"id": 1, "valor": "9"}]


def test_mensagem_repetida_aplica_uma_vez(tmp_path):
    """A fila reentrega o lote inteiro (queda antes de confirmar o offset) e mais um repetido."""
    lote = [ev(1, "c", 10, valor="5"), ev(1, "u", 12, valor="9")]
    gravar_lote(tmp_path, lote, "t", 0, 0, 1)
    # outra faixa de offsets, com os repetidos dentro
    gravar_lote(tmp_path, lote + [ev(3, "c", 14, valor="1")], "t", 0, 0, 2)
    gravar_lote(tmp_path, [ev(1, "u", 12, valor="9")], "t", 0, 5, 5)
    assert _estado(tmp_path) == [{"id": 1, "valor": "9"}, {"id": 3, "valor": "1"}]


def test_ordem_de_chegada_nao_importa(tmp_path):
    """Vale o maior LSN, não o último arquivo gravado."""
    gravar_lote(tmp_path, [ev(1, "u", 20, valor="novo")], "t", 0, 0, 0)
    gravar_lote(tmp_path, [ev(1, "c", 10, valor="velho")], "t", 0, 1, 1)
    assert _estado(tmp_path) == [{"id": 1, "valor": "novo"}]


def test_reprocessar_mesma_faixa_reescreve_o_arquivo(tmp_path):
    for _ in range(3):
        gravar_lote(tmp_path, [ev(1, "c", 10, valor="5")], "loja.public.pedidos", 0, 0, 0)
    esperado = [nome_arquivo("loja.public.pedidos", 0, 0, 0)]
    assert [p.name for p in tmp_path.glob("*.parquet")] == esperado
    assert not list(tmp_path.glob("*.tmp"))


def test_destino_vazio(tmp_path):
    assert _estado(tmp_path) == []


def test_envelope_debezium():
    valor = json.dumps({"op": "u", "ts_ms": 5, "before": None,
                        "after": {"id": 7, "valor": "1.00"},
                        "source": {"table": "pedidos", "lsn": 123}})
    e = de_debezium(json.dumps({"id": 7}), valor)
    assert e == Evento("pedidos", 7, "u", 123, 5, {"id": 7, "valor": "1.00"})
    apagado = de_debezium(None, json.dumps({"op": "d", "before": {"id": 7}, "after": None,
                                            "source": {"table": "pedidos", "lsn": 124}}))
    assert apagado is not None and apagado.linha is None and apagado.chave == 7
    assert de_debezium(b'{"id": 7}', None) is None   # tombstone


def test_reconciliar_acha_falta_sobra_e_divergencia():
    cols = ["id", "valor"]
    origem = [{"id": 1, "valor": "5"}, {"id": 2, "valor": "7"}, {"id": 3, "valor": "1"}]
    destino = [{"id": 1, "valor": "5"}, {"id": 2, "valor": "8"}, {"id": 4, "valor": "1"}]
    r = comparar("pedidos", origem, destino, cols)
    assert (r.faltando, r.sobrando, r.divergentes, r.ok) == ([3], [4], [2], False)
    assert comparar("pedidos", origem, list(reversed(origem)), cols).ok


def test_apagar_e_reinserir_o_mesmo_id(tmp_path):
    """Delete seguido de insert da mesma chave: vale a linha reinserida (maior LSN)."""
    gravar_lote(tmp_path, [ev(1, "c", 10, valor="5"), ev(1, "d", 11),
                           ev(1, "c", 12, valor="8")], "t", 0, 0, 2)
    assert _estado(tmp_path) == [{"id": 1, "valor": "8"}]
    # e o inverso: reinserido e apagado de novo some do destino
    gravar_lote(tmp_path, [ev(1, "d", 13)], "t", 0, 3, 3)
    assert _estado(tmp_path) == []


def test_colisao_de_identidade_e_detectada(tmp_path):
    """Duas mudanças diferentes com o mesmo (tabela, chave, lsn) seriam fundidas em silêncio."""
    con = duckdb.connect()
    try:
        gravar_lote(tmp_path, [ev(1, "c", 10, valor="5")], "t", 0, 0, 0)
        gravar_lote(tmp_path, [ev(1, "c", 10, valor="5")], "t", 0, 1, 1)   # repetido: ok
        assert colisoes_de_identidade(con, tmp_path) == 0
        gravar_lote(tmp_path, [ev(1, "u", 10, valor="6")], "t", 0, 2, 2)   # colisão
        assert colisoes_de_identidade(con, tmp_path) == 1
    finally:
        con.close()
