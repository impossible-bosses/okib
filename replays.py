from enum import Enum, unique

@unique
class Difficulty(Enum):
    VERY_EASY = "Very Easy"
    EASY = "Easy"
    MODERATE = "Moderate"
    NORMAL = "Normal"
    HARD = "Hard"

@unique
class Class(Enum):
    DEATH_KNIGHT = "Death Knight"
    DRUID = "Druid"
    FIRE_MAGE = "Fire Mage"
    ICE_MAGE = "Ice Mage"
    PALADIN = "Paladin"
    PRIEST = "Priest"
    RANGER = "Ranger"
    ROGUE = "Rogue"
    WARLOCK = "Warlock"
    WARRIOR = "Warrior"

@unique
class Boss(Enum):
    FIRE = "fire"
    WATER = "water"
    BRUTE = "brute"
    THUNDER = "thunder"
    DRUID = "druid"
    SHADOW = "shadow"
    ICE = "ice"
    LIGHT = "light"
    ANCIENT = "ancient"
    DEMONIC = "demonic"

class PlayerStats:
    def __init__(self, json):
        self.deaths = json["deaths"]
        self.dmg = json["damage"]
        self.hl = json["healing"]
        self.hlr = json["healingReceived"]
        self.hlrSw = json["sWHealingReceived"]
        self.degen = json["degen"]

class PlayerData:
    def __init__(self, json):
        self.name = json["name"]
        self.is_host = json["isHost"]
        self.slot = json["slot"]
        self.color = json["colour"]
        mmd_vars = json["variables"]
        self.class_ = Class(mmd_vars["class"])
        self.health = mmd_vars["health"]
        self.mana = mmd_vars["mana"]
        self.ability = mmd_vars["ability"]
        self.ms = mmd_vars["movementSpeed"]
        self.coins = mmd_vars["coins"]
        self.stats_overall = PlayerStats(mmd_vars)
        self.stats_boss = {}
        for boss in Boss:
            mmd_vars_boss = {}
            for k, v in mmd_vars.items():
                if k[:len(boss.value)] == boss.value:
                    k_trim = k[len(boss.value):]
                    if len(k_trim) > 0:
                        k_trim[0] = lower(k_trim[0])
                        mmd_vars_boss[k_trim] = v
            self.stats_boss[boss] = PlayerStats(mmd_vars_boss)

class ReplayData:
    def __init__(self, json):
        game = json["body"]["data"]["game"]
        self.id = json["body"]["id"]
        self.game_name = game["name"]
        self.map = game["map"]
        self.host = game["host"]
        self.players = [PlayerData(player) for player in game["players"]]
        flag = None
        difficulty = None
        continues = None
        for player in game["players"]:
            if len(player["flags"]) != 1:
                raise ValueError("more than 1 flag for player: {}".format(player))

            flag_player = player["flags"][0]
            if flag == None:
                flag = flag_player
            elif flag != flag_player:
                raise ValueError("Inconsistent flags: {} and {}".format(flag, flag_player))

            difficulty_player = player["variables"]["difficulty"]
            if difficulty == None:
                difficulty = difficulty_player
            elif difficulty != difficulty_player:
                raise ValueError("Inconsistent difficulties: {} and {}".format(difficulty, difficulty_player))

            continues_player = player["variables"]["continues"]
            if continues == None:
                continues = continues_player
            elif continues != continues_player:
                raise ValueError("Inconsistent difficulties: {} and {}".format(continues, continues_player))

        if flag == "winner":
            self.win = True
        elif flag == "loser":
            self.win = False
        else:
            raise ValueError("Invalid flag: {}".format(flag))

        self.difficulty = Difficulty(difficulty)

        if continues == "yes":
            self.continues = True
        elif continues == "no":
            self.continues = False
        else:
            raise ValueError("Invalid continues: {}".format(continues))
