import sys
from pathlib import Path
# Set the module paths
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import polars as pl

from api.configuration.config import TAB_DIR


# Carga de datos históricos tabulares
###################################################################
print('Cargando datos...')
data = pl.read_parquet(f'{TAB_DIR}/historia.parquet')
print(f'data.shape: {data.shape}')
# Creación de variables auxiliares
###################################################################
print('Variables auxiliares...')
data = data.with_columns(
    pl.when(pl.col("stage") == "preflop").then(0)
    .when(pl.col("stage") == "flop").then(1)
    .when(pl.col("stage") == "turn").then(2)
    .when(pl.col("stage") == "river").then(3)
    .otherwise(None).alias("stage_aux")
)
data = data.with_columns(
    pl.when(pl.col("action").str.contains("folds")).then(1).otherwise(0).alias("fold"),
    pl.when(pl.col("action").str.contains("checks")).then(1).otherwise(0).alias("check"),
    pl.when(pl.col("action").str.contains("calls")).then(1).otherwise(0).alias("call"),
    pl.when(pl.col("action").str.contains("bets")).then(1).otherwise(0).alias("bet"),
    pl.when(pl.col("action").str.contains("raises")).then(1).otherwise(0).alias("raise")
    
)

# Para cada posición, fase máxima a la que llegan, y distintos valores relacionados con su juego hasta esa fase
###################################################################
print('Tratamiento de la fase máxima')
print('FASE 1')
# FASE 1
# Fase máxima a la que llega en cada mano (cada mano se juego en una y sólo una posición), nº de folds, checks, calls, bets, raises
print('max_stage...')
max_stage =data.group_by(['player','hand_id']).agg(
    pl.col('chips_inicial').max().alias('chips_inicial'),
    pl.col('stage_aux').max().alias('max_stage_aux'),
    pl.col('fold').sum().alias('n_folds'),
    pl.col('check').sum().alias('n_checks'),
    pl.col('call').sum().alias('n_calls'),
    pl.col('bet').sum().alias('n_bets'),
    pl.col('raise').sum().alias('n_raises')
)
max_stage =max_stage.with_columns(
    pl.when(pl.col("max_stage_aux") == 0).then(pl.lit("preflop"))
    .when(pl.col("max_stage_aux") == 1).then(pl.lit("flop"))
    .when(pl.col("max_stage_aux") == 2).then(pl.lit("turn"))
    .when(pl.col("max_stage_aux") == 3).then(pl.lit("river"))
    .otherwise(None).alias("max_stage")
)
# Calculamos lo que han invertido en cada mano
print('hand_invest...')
hand_invest = data.group_by(['player','hand_id']).agg(
    pl.col('chips_inicial').max().alias('chips_inicial'),
    pl.col('chips').min().alias('min_chips')
)	
hand_invest = hand_invest.with_columns(
    (pl.col("chips_inicial") - pl.col("min_chips")).alias("hand_invest")
).select(['player','hand_id','hand_invest'])
# Calculamos lo que han ganado en cada mano (siempore que hayan ganado algo)
print('winners...')
winners = data.filter((pl.col("stage") == "showdown")).drop_nulls("winner")
winners = winners.group_by(['player','hand_id']).agg(
    pl.col('chips_won').max().alias('chips_won')
)	
# Calculamos el 'seat' de cada jugador en cada mano
print('seats...')
seats = data.select(['player','hand_id','seat']).unique()
# Unimos: En cada mano tenemos a qué fase han llegado, queremos saber con cuánto inicia la mano, cuánto invierte y cuánto gana
print('info_stage_hand...')
info_stage_hand =max_stage.join(hand_invest, on=['player', 'hand_id'], how="left", coalesce=True)\
.join(seats, on=['player', 'hand_id'], how="left", coalesce=True)\
.join(winners, on=['player', 'hand_id'], how="left", coalesce=True)
# Completamos nulos y más
info_stage_hand = info_stage_hand.with_columns(
    pl.col("chips_won").fill_null(0)
)
info_stage_hand = info_stage_hand.with_columns(
    pl.when((pl.col("hand_invest") > 0)&(pl.col("chips_won") <= 0)).then(1).otherwise(0).alias("lose_hand"),
    pl.when((pl.col("chips_won") > 0)).then(1).otherwise(0).alias("win_hand"),
    pl.when((pl.col("hand_invest") <= 0)&(pl.col("chips_won") <= 0)).then(1).otherwise(0).alias("no_win_lose_hand")
)

# FASE 2
print('FASE 2...')

stages_reached = info_stage_hand.group_by(['player','max_stage','seat']).agg(
    pl.col('hand_id').n_unique().alias('stage_reached'), # Nº de veces que se llega a cada fase
    pl.col('win_hand').sum().alias('hands_won'), # Nº de veces que se gana según las fases que alcanza
    pl.col('lose_hand').sum().alias('hands_lost'), # Nº de veces que se pierde según las fases que alcanza
    pl.col('no_win_lose_hand').sum().alias('hands_null'), # Nº de veces que abandona en cada fase sin perder dinero
    pl.col('n_folds').sum().alias('hands_folds'), # Nº de manos foldeadas desde cada posición
    pl.col('n_checks').sum().alias('n_checks'), # Nº n_checks hasta cada posición
    pl.col('n_calls').sum().alias('n_calls'), # Nº n_calls hasta cada posición
    pl.col('n_bets').sum().alias('n_bets'), # Nº n_bets hasta cada posición
    pl.col('n_raises').sum().alias('n_raises') # Nº n_raises hasta cada posición

)


# Para cada posición, se calculan parámetros asociados a cada fase eb esa posición
###################################################################
print('Tratamiento por fases...')

# Marcamos las manos ganadoras de cada jugador
data=data.join(
    data.group_by(['player','hand_id']).agg(
        pl.col('chips_won').max().alias('chips_won')
    ).with_columns(
        pl.when(pl.col("chips_won")>0).then(1).otherwise(0).alias("hand_won")
    )[['player','hand_id','hand_won']]
    , on=['player', 'hand_id'], how="left", coalesce=True)

# Checks, calls, bets, y raises de manos ganadoras
data = data.with_columns(
        (pl.col("check")*pl.col("hand_won")).alias("checks_won"),
        (pl.col("call")*pl.col("hand_won")).alias("calls_won"),
        (pl.col("bet")*pl.col("hand_won")).alias("bets_won"),
        (pl.col("raise")*pl.col("hand_won")).alias("raises_won")
    )	

by_stage = data.group_by(['player','seat','stage']).agg(
    pl.col('hand_id').n_unique().alias('n_hands'),
    pl.col('fold').sum().alias('n_folds'),# Nº de manos foldeadas desde cada posición y en cada fase
    pl.col('check').sum().alias('n_checks'),# Nº n_checks desde cada posición y en cada fase
    pl.col('call').sum().alias('n_calls'),# Nº n_calls desde cada posición y en cada fase
    pl.col('bet').sum().alias('n_bets'),# Nº n_bets desde cada posición y en cada fase
    pl.col('raise').sum().alias('n_raises'),# Nº n_raises desde cada posición y en cada fase
    pl.col('checks_won').sum().alias('n_checks_won'), # Nº n_checks de manos ganadores desde cada posición y en cada fase
    pl.col('calls_won').sum().alias('n_calls_won'), # Nº n_calls de manos ganadores desde cada posición y en cada fase
    pl.col('bets_won').sum().alias('n_bets_won'), # Nº n_bets de manos ganadores desde cada posición y en cada fase
    pl.col('raises_won').sum().alias('n_raises_won') # Nº n_raises de manos ganadores desde cada posición y en cada fase

)
by_stage = by_stage.with_columns( # Ratios
    (pl.col('n_raises')/pl.col('n_hands')).alias('raises_per_hand'), 
    (pl.col('n_raises_won')/pl.col('n_raises')).alias('per_raises_won')
)



# Guardamos
###################################################################
print('Guardando...')
stages_reached.write_parquet(f'{TAB_DIR}/stages_reached.parquet')
print(f'stages_reached con shape: {stages_reached.shape}')
by_stage.write_parquet(f'{TAB_DIR}/by_stage.parquet')
print(f'by_stage con shape: {by_stage.shape}')
