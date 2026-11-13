"""Delta Lake: o MERGE só avança (maior LSN vence), o delete é lógico e reaplicar não muda nada.

As métricas que `aplicar_lote` devolve (linhas inseridas e atualizadas pelo MERGE) provam o
efeito real: lote reaplicado ou atrasado dá 0 nas duas.
"""
from deltalake import DeltaTable

from cdc import delta
from cdc.eventos import Evento


def ev(chave, op, lsn, **linha):
    return Evento("pedidos", chave, op, lsn, 0, None if op == "d" else {"id": chave, **linha})


def _estado(raiz):
    return delta.estado_atual(raiz, "pedidos")


def _linhas_fisicas(raiz):
    return DeltaTable(str(raiz / "pedidos")).to_pyarrow_table().num_rows


def test_reaplicar_o_mesmo_lote_nao_muda_nada(tmp_path):
    lote = [ev(1, "c", 10, valor="5"), ev(2, "c", 11, valor="7"), ev(1, "u", 12, valor="9")]
    primeira = delta.aplicar_lote(tmp_path, lote)
    antes = _estado(tmp_path)
    de_novo = delta.aplicar_lote(tmp_path, lote)    # a fila reentregou o lote inteiro
    assert _estado(tmp_path) == antes == [{"id": 1, "valor": "9"}, {"id": 2, "valor": "7"}]
    assert _linhas_fisicas(tmp_path) == 2
    assert primeira["num_target_rows_inserted"] == 2
    assert de_novo == {"num_source_rows": 2, "num_target_rows_inserted": 0,
                       "num_target_rows_updated": 0}


def test_evento_velho_depois_do_delete_nao_ressuscita_a_linha(tmp_path):
    delta.aplicar_lote(tmp_path, [ev(1, "c", 10, valor="5")])
    delta.aplicar_lote(tmp_path, [ev(1, "d", 20)])
    atrasado = delta.aplicar_lote(tmp_path, [ev(1, "u", 15, valor="9")])   # chegou atrasado
    assert atrasado["num_target_rows_updated"] == 0
    assert _estado(tmp_path) == []
    linha = DeltaTable(str(tmp_path / "pedidos")).to_pyarrow_table().to_pylist()[0]
    assert (linha["apagado"], linha["lsn"]) == (True, 20)       # o delete ficou, com o LSN dele


def test_duas_mudancas_da_mesma_chave_no_mesmo_lote(tmp_path):
    """Sem o dedup do lote, o MERGE receberia duas linhas da origem para a mesma chave."""
    delta.aplicar_lote(tmp_path, [ev(1, "u", 12, valor="9"), ev(1, "c", 10, valor="5"),
                                  ev(1, "u", 14, valor="11")])
    assert _estado(tmp_path) == [{"id": 1, "valor": "11"}]


def test_apagar_e_reinserir_o_mesmo_id(tmp_path):
    delta.aplicar_lote(tmp_path, [ev(1, "c", 10, valor="5")])
    delta.aplicar_lote(tmp_path, [ev(1, "d", 20), ev(1, "c", 21, valor="8")])
    assert _estado(tmp_path) == [{"id": 1, "valor": "8"}]


def test_ordem_dos_lotes_nao_importa(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    lotes = [[ev(1, "c", 10, valor="5"), ev(2, "c", 11, valor="7")],
             [ev(1, "u", 12, valor="9"), ev(2, "d", 13)], [ev(3, "c", 14, valor="1")]]
    for lote in lotes:
        delta.aplicar_lote(a, lote)
    for lote in reversed(lotes):
        delta.aplicar_lote(b, lote)
    assert _estado(a) == _estado(b) == [{"id": 1, "valor": "9"}, {"id": 3, "valor": "1"}]


def test_cada_merge_e_um_commit_no_log(tmp_path):
    delta.aplicar_lote(tmp_path, [ev(1, "c", 10, valor="5")])
    delta.aplicar_lote(tmp_path, [ev(1, "u", 12, valor="9")])
    assert delta.versoes(tmp_path, "pedidos") == 3   # criação + 2 MERGEs


def test_sem_o_dedup_do_lote_a_chave_duplica(tmp_path, monkeypatch):
    """Por que o dedup existe: sem ele, numa tabela nova, o MERGE insere a mesma chave 3 vezes,
    sem erro. Este teste prova a falha que o dedup evita."""
    monkeypatch.setattr(delta, "_mais_novo_por_chave", lambda evs: list(evs))
    delta.aplicar_lote(tmp_path, [ev(1, "u", 12, valor="9"), ev(1, "c", 10, valor="5"),
                                  ev(1, "u", 14, valor="11")])
    assert _linhas_fisicas(tmp_path) == 3


def test_dedup_do_lote_com_chave_ja_existente(tmp_path):
    """Chave que já está na tabela e chega 2 vezes no lote: vale a mais nova, numa linha só."""
    delta.aplicar_lote(tmp_path, [ev(1, "c", 10, valor="5")])
    res = delta.aplicar_lote(tmp_path, [ev(1, "u", 14, valor="11"), ev(1, "u", 12, valor="9")])
    assert _estado(tmp_path) == [{"id": 1, "valor": "11"}]
    assert _linhas_fisicas(tmp_path) == 1
    assert res["num_target_rows_updated"] == 1


def test_mesmo_lsn_com_conteudo_diferente_fica_o_primeiro(tmp_path):
    """Limite conhecido: o LSN é a identidade. Um 2º evento com o mesmo LSN não substitui o 1º
    (nem no mesmo lote, nem em outro). O relatório conta isso como colisão de identidade."""
    delta.aplicar_lote(tmp_path, [ev(1, "c", 10, valor="5"), ev(1, "u", 10, valor="9")])
    delta.aplicar_lote(tmp_path, [ev(1, "u", 10, valor="7")])
    assert _estado(tmp_path) == [{"id": 1, "valor": "5"}]
