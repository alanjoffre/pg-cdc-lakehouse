# Resultado dos cenários

Execução: 2026-11-13T17:46:14+00:00 · semente 42 · resultado geral: **APROVADO**

Gerado por `python -m cdc.cenarios` (prova completa em `cenarios.json`). Dado sintético,
uma máquina, um broker. Cada cenário roda sobre o estado deixado pelo anterior.

| Cenário | Linhas origem × destino | Faltando, sobrando ou divergentes | Hash igual | Repetidos descartados no cenário | Falha conferida | Resultado |
|---|---|---|---|---|---|---|
| Operação normal (snapshot + mudanças com o consumidor rodando) | clientes 200 × 200 · pedidos 1340 × 1340 | 0 | sim | 0 | sim | ok |
| Queda do consumidor entre gravar e confirmar o offset | clientes 200 × 200 · pedidos 1439 × 1439 | 0 | sim | 0 | sim | ok |
| Queda do Debezium (SIGKILL) com a origem recebendo escrita | clientes 200 × 200 · pedidos 1614 × 1614 | 0 | sim | 40 | sim | ok |
| Mensagens repetidas publicadas de novo no tópico | clientes 200 × 200 · pedidos 1614 × 1614 | 0 | sim | 400 | sim | ok |

## Como cada falha foi conferida

- **Operação normal (snapshot + mudanças com o consumidor rodando):** sem falha injetada.
- **Queda do consumidor entre gravar e confirmar o offset:** o consumidor saiu com o código 3 (3 = queda simulada) depois de gravar 500 mensagens sem confirmar; na volta a fila reentregou 500 na mesma faixa de offsets (arquivo regravado igual) e 0 em outra faixa (descartadas na leitura).
- **Queda do Debezium (SIGKILL) com a origem recebendo escrita:** depois do SIGKILL o Debezium voltou do último offset salvo e publicou de novo 40 eventos.
- **Mensagens repetidas publicadas de novo no tópico:** 400 cópias publicadas no tópico, 400 descartadas no destino.

## Identidade de cada mudança

Na mesma transação: 20 vezes dois updates da mesma linha e 20 vezes apagar e inserir de novo o mesmo id.
Colisões de identidade (o mesmo `(tabela, chave, lsn)` com conteúdo diferente), por cenário: 1: 0, 2: 0, 3: 0, 4: 0.

## Latência (cenário 1)

Do commit na origem ao arquivo gravado, 3080 mudanças: p50 4202 ms, p95 5690 ms, máximo 6996 ms.
Medida numa máquina local com Docker e um broker: serve para comparar execuções deste projeto, não é benchmark.
