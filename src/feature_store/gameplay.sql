WITH tb_partidas as (
    Select * FROM silver.gamersclub.estatisticas_partidas_jogadores
    WHERE date(dtPartida)>=dateadd(MONTH,-6, '2022-02-11')
    and date(dtPartida)<'2022-02-11'
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
    ,tl.vlLevel
    ,(tl.vlLevel - tlf.vlLevel_first) as evolucao_level
    FROM tb_level tl
    LEFT JOIN tb_level_first tlf ON tl.idJogador = tlf.idJogador
)

SELECT 
    tp.idJogador
    ,te.vlLevel
    ,te.evolucao_level
    ,count(idLobbyJogo) as freq_jogo
    ,count(distinct date(dtPartida)) as qtd_dias
    ,count(idLobbyJogo)/count(distinct date(dtPartida)) as jogos_x_dia
    ,datediff('2022-02-11',date(max(dtPartida))) as Recencia
    ,int(flVitoria)/freq_jogo as winrate
    ,avg(qtAbates) as avgAbates
    ,avg(qtdAssistencias) as avgAssistencias
    ,avg(qtMortes) as avgMortes
    ,avg(qtHS) as avgHS
    ,avg(qtBombasArmadas) as avgBomasArmadas
    ,avg(qtBombasDesarmadas) as avgBombasDesarmadas
    ,avg(qtAbateAmigo) as avgAbateAmigo
    ,avg(qtAssistenciaAbateAmigo) as avgAssistenciaAbateAmigo
FROM tb_partidas tp
LEFT JOIN tb_evolucao te
    ON tp.idJogador = te.idJogador
GROUP BY tp.idJogador,te.vlLevel,te.evolucao_level,flVitoria



