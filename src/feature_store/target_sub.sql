-- Databricks notebook source
create table gold.gamersclub.target_sub AS
with tb_base_ativa AS (

  SELECT dtRef,
         idJogador
  FROM gold.gamersclub.gameplay

),

tb_atribuicao AS (

  select idJogador,
         dtCriacao,
         dtRemocao
  from silver.gamersclub.medalhas_jogadores
  where idMedalha IN (1,3)
  and dtCriacao >= '2019-01-13' -- para usar apenas medalhas sem BUG

),

tb_future (
-- MATCH DA PRÓXIMA ASSINATURA EM 15 DIAS
  SELECT t1.dtRef,
         t1.idJogador,
         CASE WHEN t2.idJogador is not null THEN 1 ELSE 0 END AS flSub
  FROM tb_base_ativa AS t1
  LEFT JOIN tb_atribuicao AS t2
  ON t1.idJogador = t2.idJogador
  AND t1.dtRef <= t2.dtCriacao
  AND t1.dtRef > t2.dtCriacao - interval 15 days

),

tb_sub_ativos AS (

    select t1.dtRef,
           t1.idjogador
    from tb_base_ativa as t1
    left join tb_atribuicao As t2
    on t1.idJogador = t2.idJogador

    WHERE t1.dtRef > t2.dtCriacao
    and t1.dtRef < t2.dtRemocao

),

tb_final AS (

  SELECT t1.*

  FROM tb_future AS t1

  LEFT JOIN tb_sub_ativos AS t2
  ON t1.idJogador = t2.idJogador
  AND t1.dtRef = t2.dtRef

  WHERE t2.idJogador IS NULL

)

SELECT * FROM tb_final

-- COMMAND ----------

SELECT * FROM gold.gamersclub.target_sub
