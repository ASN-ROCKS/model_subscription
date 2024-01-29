WITH tb_partidas AS (
  SELECT * FROM silver.gamersclub.estatisticas_partidas_jogadores
  WHERE DATE(dtPartida)>=dateadd(MONTH,-6,'2022-02-11')
),
tb_est_maps as (
SELECT
    idJogador,
    ROUND(AVG(qtAcertos/qtDisparos),4) as avgPontaria,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_ancient' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_ancient_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_overpass' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_overpass_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_vertigo' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_vertigo_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_nuke' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_nuke_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_train' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_train_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_mirage' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_mirage_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_dust2' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_dust2_vitorias,
    ROUND(AVG(CASE WHEN descNomeMapa = 'de_inferno' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_inferno_vitorias,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_ancient' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_ancient_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_overpass' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_overpass_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_vertigo' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_vertigo_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_nuke' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_nuke_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_train' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_train_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_mirage' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_mirage_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_dust2' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_dust2_pct,
    ROUND(SUM(CASE WHEN descNomeMapa = 'de_inferno'THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_inferno_pct
    FROM tb_partidas GROUP BY idJogador),
tb_est_acertos AS (
SELECT 
idJogador,
ROUND(AVG(qtAcertosCabeca/qtAcertos),4) as qtAcertosCabeca_pct,
ROUND(AVG(qtAcertosPeito/qtAcertos),4) as qtAcertosPeito_pct,
ROUND(AVG(qtAcertosEstomago/qtAcertos),4) as qtAcertosEstomagoo_pct,
ROUND(AVG(qtAcertosBracoEsquerdo/qtAcertos),4) as qtAcertosBracoEsquerdo_pct,
ROUND(AVG(qtAcertosBracoDireito/qtAcertos),4) as qtAcertosBracoDireito_pct,
ROUND(AVG(qtAcertosPernaEsquerda/qtAcertos),4) as qtAcertosPernaEsquerda_pct,
ROUND(AVG(qtAcertosPernaDireita/qtAcertos),4) as qtAcertosPernaDireita_pct
FROM tb_partidas GROUP BY idJogador)

    SELECT * FROM tb_est_acertos