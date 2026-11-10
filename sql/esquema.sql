-- Origem do case: uma loja fictícia. Dados sintéticos, gerados com semente fixa.
CREATE TABLE clientes (
  id          bigint PRIMARY KEY,
  nome        text        NOT NULL,
  cidade      text        NOT NULL,
  atualizado  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE pedidos (
  id          bigint PRIMARY KEY,
  cliente_id  bigint      NOT NULL REFERENCES clientes(id),
  valor       numeric(12,2) NOT NULL,
  status      text        NOT NULL,
  atualizado  timestamptz NOT NULL DEFAULT now()
);
-- REPLICA IDENTITY FULL: o delete chega com a linha inteira em "before" (sem ela, só a chave).
ALTER TABLE clientes REPLICA IDENTITY FULL;
ALTER TABLE pedidos  REPLICA IDENTITY FULL;
CREATE PUBLICATION cdc_loja FOR TABLE clientes, pedidos;
