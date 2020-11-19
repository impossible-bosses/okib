import discord
import functools
import logging
import requests
import traceback

from main import ensure_display

# lobbies
_open_lobbies = set()
_wc3stats_down_message = None

class MapVersion:
    def __init__(self, file_name, ent_only = False, deprecated = False, counterfeit = False):
        self.file_name = file_name
        self.ent_only = ent_only
        self.deprecated = deprecated
        self.counterfeit = counterfeit

KNOWN_VERSIONS = [
    MapVersion("Impossible.Bosses.v1.10.5.w3x"),
    MapVersion("Impossible.Bosses.v1.10.5-ent.w3x", ent_only=True),
    MapVersion("Impossible.Bosses.v1.10.4-ent.w3x", ent_only=True, deprecated=True),
    MapVersion("Impossible.Bosses.v1.10.3-ent.w3x", ent_only=True, deprecated=True),
    MapVersion("Impossible.Bosses.v1.10.2-ent.w3x", ent_only=True, deprecated=True),
    MapVersion("Impossible.Bosses.v1.10.1-ent.w3x", ent_only=True, deprecated=True),

    MapVersion("Impossible_BossesReforgedV1.09Test.w3x", deprecated=True),
    MapVersion("ImpossibleBossesEnt1.09.w3x", ent_only=True, deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.09_UFWContinues.w3x", counterfeit=True),
    MapVersion("Impossible_BossesReforgedV1.09UFW30.w3x", counterfeit=True),
    MapVersion("Impossible_BossesReforgedV1.08Test.w3x", deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.07Test.w3x", deprecated=True),
    MapVersion("Impossible_BossesTestversion1.06.w3x", deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.05.w3x", deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.02.w3x", deprecated=True),

    MapVersion("Impossible Bosses BetaV3V.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV3R.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV3P.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV3E.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV3C.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV3A.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2X.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2W.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2S.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2J.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2F.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2E.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2D.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2C.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV2A.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1Y.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1X.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1W.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1V.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1R.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1P.w3x", deprecated=True),
    MapVersion("Impossible Bosses BetaV1C.w3x", deprecated=True),
]

def get_map_version(map_file):
    for version in KNOWN_VERSIONS:
        if map_file == version.file_name:
            return version

    return None

class Lobby:
    def __init__(self, lobby_dict):
        self.id = lobby_dict["id"]
        self.name = lobby_dict["name"]
        self.server = lobby_dict["server"]
        self.map = lobby_dict["map"]
        self.host = lobby_dict["host"]
        self.slots_taken = lobby_dict["slotsTaken"]
        self.slots_total = lobby_dict["slotsTotal"]
        self.created = lobby_dict["created"]
        self.last_updated = lobby_dict["lastUpdated"]
        self.message_id = None

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return self.id

    def __str__(self):
        return "[id={} name=\"{}\" server={} map=\"{}\" host={} slots={}/{}]".format(
            self.id, self.name, self.server, self.map, self.host, self.slots_taken, self.slots_total
        )

    def to_discord_message_info(self, open = True):
        COLOR_OPEN = discord.Colour.from_rgb(0, 255, 0)
        COLOR_CLOSED = discord.Colour.from_rgb(255, 0, 0)

        if self.map[-4:] != ".w3x":
            raise Exception("Bad map file: {}".format(self.map))
        if self.slots_total != 9 and self.slots_total != 12:
            raise Exception("Expected 9 or 12 total players, not {}, for map file {}".format(self.slots_total, self.map))

        version = get_map_version(self.map)
        mark = ""
        message = ""
        if version == None:
            mark = ":question:"
            message = ":warning: *WARNING: Unknown map version* :warning:"
        elif version.counterfeit:
            mark = ":x:"
            message = ":warning: *WARNING: Counterfeit version* :warning:"
        elif version.ent_only:
            mark = ":x:"
            message = ":warning: *WARNING: Incompatible version* :warning:"
        elif version.deprecated:
            mark = ":x:"
            message = ":warning: *WARNING: Old map version* :warning:"

        description = "" if open else "*started/unhosted*"
        color = COLOR_OPEN if open else COLOR_CLOSED
        embed_title = self.map[:-4] + "  " + mark

        embed = discord.Embed(title=embed_title, description=description, color=color)
        embed.add_field(name="Lobby Name", value=self.name, inline=False)
        embed.add_field(name="Host", value=self.host, inline=True)
        embed.add_field(name="Region", value=self.server, inline=True)
        players_str = "{} / {}".format(self.slots_taken - 1, self.slots_total - 1)
        embed.add_field(name="Players", value=players_str, inline=True)

        return {
            "message": message,
            "embed": embed,
        }

def is_ib_lobby(lobby):
    return lobby.map.find("Uther Party") != -1 # test
    return lobby.map.find("Impossible") != -1 and lobby.map.find("Bosses") != -1

def get_ib_lobbies():
    response = requests.get("https://api.wc3stats.com/gamelist")
    games = response.json()["body"]
    if not isinstance(games, list):
        raise Exception("Property 'games' in HTTP response is not a list, {}".format(type(games)))

    lobbies = [Lobby(game) for game in games]
    ib_lobbies = set([lobby for lobby in lobbies if is_ib_lobby(lobby)])
    logging.info("{} total lobbies, {} IB lobbies".format(len(lobbies), len(ib_lobbies)))

    return ib_lobbies

async def send_message(channel, content, embed):
    message = await channel.send(content=content, embed=embed)
    return message.id

async def report_ib_lobbies(channel):
    global _open_lobbies, _wc3stats_down_message

    try:
        lobbies = get_ib_lobbies()
    except Exception as e:
        logging.error("Error getting IB lobbies")
        traceback.print_exc()

        if _wc3stats_down_message is None:
            _wc3stats_down_message = await channel.send(content=":warning: WARNING: https://wc3stats.com/gamelist API down, no lobby list :warning:")
        return

    if _wc3stats_down_message is not None:
        try:
            await _wc3stats_down_message.delete()
        except Exception as e:
            pass
        _wc3stats_down_message = None

    new_open_lobbies = set()
    for lobby in _open_lobbies:
        try:
            message = await channel.fetch_message(lobby.message_id)
        except Exception as e:
            logging.error("Error fetching message with ID {}".format(lobby.message_id))
            traceback.print_exc()
            continue

        still_open = lobby in lobbies
        lobby_latest = lobby
        if still_open:
            for lobby2 in lobbies:
                if lobby2 == lobby:
                    lobby_latest = lobby2
                    lobby_latest.message_id = lobby.message_id
                    break
            new_open_lobbies.add(lobby_latest)

        try:
            message_info = lobby_latest.to_discord_message_info(still_open)
            if message_info is None:
                logging.info("Lobby skipped: {}".format(lobby_latest))
                continue
            logging.info("Lobby updated (open={}): {}".format(still_open, lobby_latest))
            await message.edit(embed=message_info["embed"])
        except Exception as e:
            logging.error("Failed to edit message for lobby \"{}\"".format(lobby_latest.name))
            traceback.print_exc()

    _open_lobbies = new_open_lobbies

    for lobby in lobbies:
        if lobby not in _open_lobbies:
            try:
                message_info = lobby.to_discord_message_info()
                if message_info is None:
                    logging.info("Lobby skipped: {}".format(lobby))
                    continue
                logging.info("Lobby created: {}".format(lobby))
                message_id = await ensure_display(send_message, channel, message_info["message"], message_info["embed"])
                logging.info(message_id)
                # message = await channel.send(content=message_info["message"], embed=message_info["embed"])
            except Exception as e:
                logging.error("Failed to send message for lobby \"{}\"".format(lobby.name))
                traceback.print_exc()
                continue

            lobby.message_id = message_id
            # lobby.message_id = message.id
            _open_lobbies.add(lobby)
