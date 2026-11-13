# Resultado dos cenários

Execução: 2026-11-13T18:33:18+00:00 · commit 70f213b · semente 42 · resultado geral: **APROVADO**

Gerado por `python -m cdc.cenarios` (prova completa em `cenarios.json`). Dado sintético,
uma máquina, um broker. Cada cenário roda sobre o estado deixado pelo anterior.
Dois destinos conferidos contra a origem: o estado calculado do log em Parquet (DuckDB)
e a tabela Delta Lake mantida por MERGE.

| Cenário | Linhas origem × destino | Faltando, sobrando ou divergentes | Hash igual | Delta igual à origem | Repetidos descartados no cenário | Falha conferida | Resultado |
|---|---|---|---|---|---|---|---|
| Operação normal (snapshot + mudanças com o consumidor rodando) | clientes 200 × 200 · pedidos 1340 × 1340 | 0 | sim | sim | 0 | n/a | ok |
| Queda do consumidor entre gravar e confirmar o offset | clientes 200 × 200 · pedidos 1439 × 1439 | 0 | sim | sim | 500 | sim | ok |
| Queda do Debezium (SIGKILL) com a origem recebendo escrita | clientes 200 × 200 · pedidos 1614 × 1614 | 0 | sim | sim | 300 | sim | ok |
| Mensagens repetidas publicadas de novo no tópico | clientes 200 × 200 · pedidos 1614 × 1614 | 0 | sim | sim | 400 | sim | ok |

## Como cada falha foi conferida

- **Operação normal (snapshot + mudanças com o consumidor rodando):** sem falha injetada.
- **Queda do consumidor entre gravar e confirmar o offset:** o consumidor saiu com o código 3 (3 = queda simulada) depois de gravar 500 mensagens sem confirmar; na volta a fila reentregou 0 na mesma faixa de offsets (arquivo regravado igual) e 500 em outra faixa (descartadas na leitura).
- **Queda do Debezium (SIGKILL) com a origem recebendo escrita:** depois do SIGKILL o Debezium voltou do último offset salvo e publicou de novo 300 eventos.
- **Mensagens repetidas publicadas de novo no tópico:** 400 cópias publicadas no tópico, 400 descartadas no log; no Delta, o MERGE recebeu 372 linhas e alterou 0 (o predicado do LSN barrou todas).

## Identidade de cada mudança

Na mesma transação: 20 vezes dois updates da mesma linha e 20 vezes apagar e inserir de novo o mesmo id.
Colisões de identidade (o mesmo `(tabela, chave, lsn)` com conteúdo diferente), por cenário: 1: 0, 2: 0, 3: 0, 4: 0.

## Latência (cenário 1)

Da mudança na origem (`source.ts_ms`) até cada etapa, 3080 mudanças, medidas depois de o consumidor gravar o snapshot:

| Até | p50 | p95 | máximo |
|---|---|---|---|
| no tópico do Kafka | 483 ms | 859 ms | 965 ms |
| no arquivo Parquet gravado | 1435 ms | 2322 ms | 2515 ms |
| na tabela Delta (depois do MERGE) | 1608 ms | 2439 ms | 2696 ms |

Medida numa máquina local com Docker e um broker: serve para comparar execuções deste projeto, não é benchmark.
