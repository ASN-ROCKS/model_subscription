WITH tb_partidas as (
    Select * FROM silver.gamersclub.estatisticas_partidas_jogadores
    WHERE date(dtPartida)>=dateadd(MONTH,-3, '{date}')
    and date(dtPartida)<'{date}'
),

tb_level as (
    Select idJogador,vlLevel
    FROM tb_partidas
    QUALIFY row_number() OVER (PARTITION BY idJogador order by dtPartida DESC)=1
),

tb_level_first as (
    Select idJogador
    ,vlLevel as vlLevel_first
    FROM tb_partidas
    QUALIFY row_number() OVER (PARTITION BY idJogador order by dtPartida ASC)=1
),

tb_evolucao as (
    Select tl.idJogador
    ,tl.vlLevel AS last_level
    ,(tl.vlLevel - tlf.vlLevel_first) as evolucao_level
    ,(tl.vlLevel/tlf.vlLevel_first) as evolucao_level_pct
    FROM tb_level tl
    LEFT JOIN tb_level_first tlf ON tl.idJogador = tlf.idJogador
),

stats_general AS (
  SELECT 
      tp.idJogador
      ,count(idLobbyJogo) as freq_jogo
      ,count(distinct date(dtPartida)) as qtd_dias
      ,count(idLobbyJogo)/count(distinct date(dtPartida)) as jogos_x_dia
      ,datediff('{date}',date(max(dtPartida))) as Recencia
      ,sum(int(flVitoria))/count(idLobbyJogo) as winrate
      ,avg(qtAbates) as avgAbates
      ,avg(qtdAssistencias) as avgAssistencias
      ,avg(qtMortes) as avgMortes
      ,avg(qtHS) as avgHS
      ,avg(qtBombasArmadas) as avgBomasArmadas
      ,avg(qtBombasDesarmadas) as avgBombasDesarmadas
      ,avg(qtAbateAmigo) as avgAbateAmigo
      ,avg(qtAssistenciaAbateAmigo) as avgAssistenciaAbateAmigo
  FROM tb_partidas tp

  GROUP BY idJogador
),

tb_est_maps as (
    SELECT
    idJogador
    ,ROUND(AVG(qtAcertos/qtDisparos),4) as avgPontaria
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_ancient' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_ancient_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_overpass' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_overpass_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_vertigo' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_vertigo_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_nuke' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_nuke_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_train' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_train_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_mirage' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_mirage_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_dust2' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_dust2_vitorias
    ,ROUND(AVG(CASE WHEN descNomeMapa = 'de_inferno' AND flVitoria = true THEN 1 ELSE 0 END),4) AS de_inferno_vitorias
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_ancient' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_ancient_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_overpass' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_overpass_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_vertigo' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_vertigo_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_nuke' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_nuke_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_train' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_train_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_mirage' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_mirage_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_dust2' THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_dust2_pct
    ,ROUND(SUM(CASE WHEN descNomeMapa = 'de_inferno'THEN 1 ELSE 0 END) / COUNT(DISTINCT (idLobbyJogo)),4) AS de_inferno_pct
    ,ROUND(AVG(qtAcertosCabeca/qtAcertos),4) as qtAcertosCabeca_pct
    ,ROUND(AVG(qtAcertosPeito/qtAcertos),4) as qtAcertosPeito_pct
    ,ROUND(AVG(qtAcertosEstomago/qtAcertos),4) as qtAcertosEstomagoo_pct
    ,ROUND(AVG(qtAcertosBracoEsquerdo/qtAcertos),4) as qtAcertosBracoEsquerdo_pct
    ,ROUND(AVG(qtAcertosBracoDireito/qtAcertos),4) as qtAcertosBracoDireito_pct
    ,ROUND(AVG(qtAcertosPernaEsquerda/qtAcertos),4) as qtAcertosPernaEsquerda_pct
    ,ROUND(AVG(qtAcertosPernaDireita/qtAcertos),4) as qtAcertosPernaDireita_pct
    FROM tb_partidas
    GROUP BY idJogador
),

tb_final AS (
  SELECT t1.*
         ,t2.freq_jogo
         ,t2.qtd_dias
         ,t2.jogos_x_dia
         ,t2.Recencia
         ,t2.winrate
         ,t2.avgAbates
         ,t2.avgAssistencias
         ,t2.avgMortes
         ,t2.avgHS
         ,t2.avgBomasArmadas
         ,t2.avgBombasDesarmadas
         ,t2.avgAbateAmigo
         ,t2.avgAssistenciaAbateAmigo
         ,t3.last_level
         ,t3.evolucao_level
         ,t3.evolucao_level_pct

  FROM tb_est_maps AS t1
  LEFT JOIN stats_general AS t2
  ON t1.idJogador = t2.idJogador

  LEFT JOIN tb_evolucao t3
  ON t1.idJogador = t3.idJogador

)

SELECT date('{date}') AS dtRef
       ,*
FROM tb_final

