WITH tb_medalhas
AS (SELECT * 
    FROM silver.gamersclub.medalhas_jogadores 
    -- WHERE dtCriacao >= dateadd(MONTH,-6,'2022-02-11') 
    WHERE dtCriacao < '{date}'),

first_subscription
AS (SELECT 
  idJogador, 
  MIN(CASE WHEN idMedalha IN (1, 3) THEN dtCriacao END) AS first_date, 
  DATEDIFF(current_date(), MIN(CASE WHEN idMedalha IN (1, 3) THEN dtCriacao END)) AS days_since_first_subscription
FROM tb_medalhas
WHERE idMedalha IN (1, 3)
GROUP BY idJogador)

SELECT a.idJogador,
       CASE WHEN (b.idMedalha IN (1, 3) AND '{date}' < a.DtRemocao AND '{date}' > a.DtCriacao) THEN 1 ELSE 0 END AS flSubAtivo,
       sum(CASE WHEN b.idMedalha IN (1, 3) THEN 1 ELSE 0 END) AS qtdAssinaturas,
       sum(CASE WHEN b.idMedalha IN (1, 3) THEN 1 ELSE 0 END)/12 AS qtdAssinaturasMes,
       sum(CASE WHEN b.idMedalha NOT IN (1, 3) THEN 1 ELSE 0 END) AS qtdMedalhas,
       sum(CASE WHEN b.idMedalha NOT IN (1, 3) THEN 1 ELSE 0 END)/12 AS qtdMedalhasMes,
       sum(CASE WHEN (b.idMedalha IN (1, 3) AND '{date}' < a.DtRemocao) THEN 1 ELSE 0 END) AS qtdChurn,
       sum(CASE WHEN (b.idMedalha IN (1, 3) AND '{date}' < a.DtRemocao) THEN 1 ELSE 0 END)/12 AS qtdChurnMes,
       DATEDIFF('{date}', MIN(CASE WHEN b.idMedalha IN (1, 3) THEN dtCriacao END)) AS daysSinceFirstSub
FROM tb_medalhas as a
LEFT JOIN silver.gamersclub.medalhas as b
INNER JOIN first_subscription as c
ON a.idMedalha=b.idMedalha and a.idJogador=c.idJogador
GROUP BY 1, 2