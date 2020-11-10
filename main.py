import asyncio
import datetime
import discord
from enum import Enum, auto
import logging
import os
import requests
import time
import traceback

from secret import DISCORD_TOKEN

# DISCORD_GUILD = "IB CAFETERIA"
# DISCORD_CHANNEL = "pub-games"
DISCORD_GUILD = "Test"
DISCORD_CHANNEL = "general"

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
    MapVersion("Impossible Bosses BetaV3V.w3x", deprecated=True),
]

# Should persist this in a DB or something
open_lobbies = set()

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
        mark = ":white_check_mark:"
        message = ""
        if version == None:
            mark = ":question:"
            message = ":warning: *WARNING: Unknown map version* :warning:"
        elif version.counterfeit:
            # mark = ":exclamation:"
            return None
        elif version.ent_only:
            mark = ":x:"
            message = ":warning: *WARNING: Incompatible version* :warning:"
        elif version.deprecated:
            mark = ":x:"
            message = ":warning: *WARNING: Old map version* :warning:"

        embed_title = self.map[:-4] + "  " + mark
        embed = discord.Embed(title=embed_title, color=(COLOR_OPEN if open else COLOR_CLOSED))
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
    # return lobby.map.find("Uther Party") != -1 # test
    return lobby.map.find("Impossible") != -1 and lobby.map.find("Bosses") != -1

def get_ib_lobbies():
    response = requests.get("https://api.wc3stats.com/gamelist")
    games = response.json()["body"]
    if not isinstance(games, list):
        raise Exception("Property 'games' in HTTP response is not a list, {}".format(type(games)))

    lobbies = [Lobby(game) for game in games]
    return set([lobby for lobby in lobbies if is_ib_lobby(lobby)])

async def report_ib_lobbies(channel):
    global open_lobbies

    try:
        lobbies = get_ib_lobbies()
    except Exception as e:
        logging.error("Error getting IB lobbies")
        traceback.print_exc()
        return

    new_open_lobbies = set()
    for lobby in open_lobbies:
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

    open_lobbies = new_open_lobbies

    for lobby in lobbies:
        if lobby not in open_lobbies:
            try:
                message_info = lobby.to_discord_message_info()
                if message_info is None:
                    logging.info("Lobby skipped: {}".format(lobby))
                    continue
                logging.info("Lobby created: {}".format(lobby))
                message = await channel.send(content=message_info["message"], embed=message_info["embed"])
            except Exception as e:
                logging.error("Failed to send message for lobby \"{}\"".format(lobby.name))
                traceback.print_exc()
                continue

            lobby.message_id = message.id
            open_lobbies.add(lobby)

class DiscordClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())
        self.guild = None

    async def on_ready(self):
        found_guild = False
        for guild in self.guilds:
            if guild.name == DISCORD_GUILD:
                found_guild = True
                break

        if not found_guild:
            raise Exception("Guild not found: \"{}\"".format(DISCORD_GUILD))

        found_channel = False
        for channel in guild.text_channels:
            if channel.name == DISCORD_CHANNEL:
                found_channel = True
                break

        if not found_channel:
            raise Exception("Channel not found: \"{}\" in guild \"{}\"".format(DISCORD_CHANNEL, guild.name))

        self.guild = guild
        self.channel = channel
        logging.info("Bot \"{}\" connected to Discord on guild \"{}\", posting to channel \"{}\"".format(self.user, guild.name, channel.name))

    async def my_background_task(self):
        await self.wait_until_ready()
        while not self.is_closed():
            logging.info("Refreshing lobby list")
            if self.guild is not None and self.channel is not None:
                try:
                    await report_ib_lobbies(self.channel)
                except Exception as e:
                    logging.error("Exception in report_ib_lobbies")
                    traceback.print_exc()

            await asyncio.sleep(5)

if __name__ == "__main__":
    datetime_now = datetime.datetime.now()
    log_file_name = "logs/{}.log".format(datetime_now.strftime("%Y%m%d_%H%M%S"))
    logging.basicConfig(filename=log_file_name, level=logging.INFO)

    while True:
        client = DiscordClient()
        client.run(DISCORD_TOKEN)
        time.sleep(10)
