import sys
from pathlib import Path
# Set the module paths
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from tqdm import tqdm

import aiofiles
import asyncio
import joblib
import polars as pl
import re
import os
from api.configuration.config import RAW_DIR, CONFIG_DIR, TAB_DIR


# FUNCIONES DE TRATAMIENTO DE PERSEO DE TEXTO
# Función para determinar la posición de un jugador
def get_position(seat, button_seat, total_players=6):
    positions = ['BTN', 'SB', 'BB', 'UTG', 'MP', 'HJ']
    button_seat = int(button_seat)
    seat = int(seat)
    position_index = (seat - button_seat) % total_players
    return positions[position_index]

# Función para procesar una mano de poker
def process_hand(hand_text):
    lines = hand_text.strip().split('\n')
    
    hand_id = re.search(r'PokerStars Hand #(\d+):', lines[0]).group(1)
    stakes = re.search(r'\(\$(\d+\.\d+)/\$(\d+\.\d+)\)', lines[0])
    date_time = re.search(r' - (.+)$', lines[0]).group(1)
    table = re.search(r"Table '([^']+)'", lines[1]).group(1)
    button_seat = re.search(r'Seat #(\d+)', lines[1]).group(1)
    
    players = {}
    actions = []
    current_stage = 'preflop'
    current_pot = 0
    sequential_number = 1
    hands = {}
    
    for line in lines[2:]:
        if line.startswith('Seat '):
            player = re.search(r'Seat (\d+): (.+) \(\$(\d+(?:\.\d+)?) in chips\)', line)
            if player:
                seat_number = player.group(1)
                players[player.group(2)] = {
                    'seat': get_position(seat_number, button_seat),
                    'chips_inicial': float(player.group(3)),
                    'chips': float(player.group(3)),
                    'seat_number': seat_number
                }
        elif line.startswith('*** HOLE CARDS ***'):
            current_stage = 'preflop'
            #sequential_number = 1
        elif line.startswith('*** FLOP ***'):
            current_stage = 'flop'
            sequential_number = 1
            current_pot = 0  # Reset pot for the new stage
        elif line.startswith('*** TURN ***'):
            current_stage = 'turn'
            sequential_number = 1
        elif line.startswith('*** RIVER ***'):
            current_stage = 'river'
            sequential_number = 1
        elif line.startswith('*** SHOWDOWN ***'):
            current_stage = 'showdown'
            sequential_number = 1
        elif line.startswith('*** SUMMARY ***'):
            current_stage = 'summary'
            sequential_number = 1
        else:
            action = re.search(r'(.+): (.+)', line)
            action2 = re.search(r'(.+) collected \$(\d+(?:\.\d+)?) (.+)', line)
            if action:
                player = action.group(1)
                action_text = action.group(2)
                action_value = re.search(r'\$(\d+\.\d+)', action_text)
                if action_value:
                    bet_amount = float(action_value.group(1))
                    current_pot += bet_amount
                    if player in players:
                        players[player]['chips'] -= bet_amount
                # if current_stage == 'showdown' and 'shows' in action_text:
                #     hands[player] = re.search(r'\[(.+)\]', action_text).group(1)
                actions.append({
                    'hand_id': hand_id,
                    'stakes_sb': stakes.groups()[0],
                    'stakes_bb': stakes.groups()[1],
                    'date_time': date_time,
                    'table': table,
                    'button_seat': button_seat,
                    'seat': players[player]['seat'] if player in players else None,
                    'player': player,
                    'chips_inicial': players[player]['chips_inicial'] if player in players else None,
                    'chips': players[player]['chips'] if player in players else None,
                    'stage': current_stage,
                    'pot': current_pot,
                    'sequential_number': sequential_number,
                    'hand': hands.get(player, None),
                    'action': action_text,
                    'winner': None,
                    'chips_won': None
                })
                sequential_number += 1
            if current_stage == 'showdown' and action2:
                player = action2.group(1)
                action_text = None
                action_value = action2.group(2)
                actions.append({
                    'hand_id': hand_id,
                    'stakes_sb': stakes.groups()[0],
                    'stakes_bb': stakes.groups()[1],
                    'date_time': date_time,
                    'table': table,
                    'button_seat': button_seat,
                    'seat': players[player]['seat'] if player in players else None,
                    'player': player if player in players else None,
                    'chips_inicial': players[player]['chips_inicial'] if player in players else None,
                    'chips': players[player]['chips'] if player in players else None,
                    'stage': current_stage,
                    'pot': current_pot,
                    'sequential_number': sequential_number,
                    'hand': hands.get(player, None),
                    'action': action_text,
                    'winner': player,
                    'chips_won':action_value
                })
                sequential_number += 1
    
    return actions

# Función para procesar todas las manos de poker en un texto
def process_hands(text):
    hands = text.strip().split('\n\n\n')
    all_actions = []
    for hand in hands:
        all_actions.extend(process_hand(hand))
    return all_actions


# PROCESAMIENTO DE ARCHIVOS
async def read_file(semaphore, file_path):
    async with semaphore:
        async with aiofiles.open(file_path, 'r') as file:
            contents = await file.read()
            return contents

async def read_multiple_files(file_paths, max_concurrent_tasks=10):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    tasks = [read_file(semaphore, file_path) for file_path in file_paths]
    results = await asyncio.gather(*tasks)
    return results


async def file_process(TAB_DIR,file_paths):
    contents = await read_multiple_files(file_paths)
    return contents
    # Procesar los contenidos y convertirlos en polars
    # actions = process_hands(text)

def dataframe(contents):
# Definir el esquema del DataFrame
    schema = {
        "hand_id": pl.Utf8,
        "stakes_sb": pl.Utf8,
        "stakes_bb": pl.Utf8,
        "date_time": pl.Utf8,
        "table": pl.Utf8,
        "button_seat": pl.Utf8,
        "seat": pl.Utf8,
        "player": pl.Utf8,
        "chips_inicial": pl.Float64,
        "chips": pl.Float64,
        "stage": pl.Utf8,
        "pot": pl.Float64,
        "sequential_number": pl.Int64,
        "hand": pl.Utf8,
        "action": pl.Utf8,
        "winner": pl.Utf8,
        "chips_won": pl.Float64
    }

    df = pl.DataFrame(schema=schema)
    for content in tqdm(contents):
        df = df.vstack(pl.DataFrame(process_hands(content),schema=schema))
    return df
    
    

# CARGA DE ARCHIVOS Y CREACIÓN DE DATA TABULAR
if ('archivos.joblib' in os.listdir(CONFIG_DIR)):
    archivos=joblib.load(f'{CONFIG_DIR}/archivos.joblib')
else:
    archivos=[]
archivos_ok = archivos+[f'{RAW_DIR}/{x}' for x in os.listdir(RAW_DIR) if not(f'{RAW_DIR}/{x}' in archivos)]
archivos_ok = [x for x in archivos_ok  if x.split('/')[-1] in os.listdir(RAW_DIR)]

if (len(archivos_ok)==len(archivos)):
    print('No hay archivos nuevos.')
    
elif len(archivos_ok)!=len(archivos):
    joblib.dump(archivos_ok, f'{CONFIG_DIR}/archivos.joblib')
    contents = asyncio.run(file_process(TAB_DIR,archivos_ok))
    data = dataframe(contents)
    data.write_parquet(f'{TAB_DIR}/historia.parquet')

if not('historia.parquet' in os.listdir(TAB_DIR)):
    contents = asyncio.run(file_process(TAB_DIR,archivos_ok))
    data = dataframe(contents)
    data.write_parquet(f'{TAB_DIR}/historia.parquet')
    
#archivos = joblib.load(file_path)
# # Texto de ejemplo con manos de poker
# text = """
# PokerStars Hand #01343321597: Hold'em No Limit ($0.01/$0.02) - 2023/07/31 18:16:38
# Table 'GG_NLHYellow2' 6-max Seat #2 is the button
# Seat 1: S13MVP_uzi ($1.84 in chips)
# Seat 2: MrHadaward ($2.04 in chips)
# Seat 3: pokerkingp ($0.37 in chips)
# Seat 4: Nojingu ($2 in chips)
# Seat 5: IHunteRl ($2.25 in chips)
# Seat 6: Giovanni_George ($0.96 in chips)
# pokerkingp: posts small blind $0.01
# Nojingu: posts big blind $0.02
# *** HOLE CARDS ***
# IHunteRl: calls $0.02
# S13MVP_uzi: folds
# MrHadaward: folds
# pokerkingp: calls $0.01
# Nojingu: checks
# *** FLOP *** [Ah 5c 7c]
# pokerkingp: checks
# Nojingu: checks
# IHunteRl: bets $0.05
# pokerkingp: calls $0.05
# Nojingu: folds
# *** TURN *** [Ah 5c 7c] [9c]
# pokerkingp: checks
# IHunteRl: checks
# *** RIVER *** [Ah 5c 7c 9c] [2c]
# pokerkingp: checks
# IHunteRl: bets $0.12
# pokerkingp: folds
# Uncalled bet ($0.12) returned to IHunteRl
# *** SHOWDOWN ***
# IHunteRl collected $0.16 from pot
# *** SUMMARY ***
# Total pot $0.16 | Rake $0 | Jackpot $0 | Bingo $0
# Board [Ah 5c 7c 9c 2c]
# Seat 1: S13MVP_uzi folded before Flop (didn't bet)
# Seat 2: MrHadaward (button) folded before Flop (didn't bet)
# Seat 3: pokerkingp (small blind) folded on the River
# Seat 4: Nojingu (big blind) folded on the Flop
# Seat 5: IHunteRl collected ($0.16)

# """

# # Procesar el texto y convertirlo en un DataFrame
# actions = process_hands(text)
# df = pd.DataFrame(actions)

# #import ace_tools as tools; tools.display_dataframe_to_user(name="Poker Actions DataFrame", dataframe=df)

# # Mostrar el DataFrame resultante
# print(df)
