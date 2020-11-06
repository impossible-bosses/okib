import asyncio
from datetime import datetime
import discord
import os
import pytz
import requests
import traceback

from secret import DISCORD_TOKEN

DISCORD_GUILD = "IB CAFETERIA"
DISCORD_CHANNEL = "pub-games"
# DISCORD_GUILD = "Test"
# DISCORD_CHANNEL = "general"

# Should persist this in a DB or something
open_lobbies = set()

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

    def to_discord_embed(self, open = True):
        COLOR_OPEN = discord.Colour.from_rgb(0, 255, 0)
        COLOR_CLOSED = discord.Colour.from_rgb(255, 0, 0)

        if self.map[-4:] != ".w3x":
            raise Exception("Bad map file: {}".format(self.map))
        if self.slots_total != 9:
            raise Exception("Expected 9 total players, not {}, for map file {}".format(self.slots_total, self.map))

        map_trimmed = self.map[:-4]
        embed = discord.Embed(title=map_trimmed, color=(COLOR_OPEN if open else COLOR_CLOSED),
            timestamp=datetime.fromtimestamp(self.created, tz=pytz.utc))
        embed.add_field(name="Lobby Name", value=self.name, inline=False)
        embed.add_field(name="Host", value=self.host, inline=True)
        embed.add_field(name="Region", value=self.server, inline=True)
        players_str = "{} / {}".format(self.slots_taken - 1, self.slots_total - 1)
        embed.add_field(name="Players", value=players_str, inline=True)

        return embed

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
        traceback.print_exc()
        return

    new_open_lobbies = set()
    for lobby in open_lobbies:
        try:
            message = await channel.fetch_message(lobby.message_id)
        except Exception as e:
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
            await message.edit(embed=lobby_latest.to_discord_embed(still_open))
        except Exception as e:
                traceback.print_exc()

    open_lobbies = new_open_lobbies

    for lobby in lobbies:
        if lobby not in open_lobbies:
            try:
                message = await channel.send(content="", embed=lobby.to_discord_embed())
            except Exception as e:
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
        print("Bot \"{}\" connected to Discord on guild \"{}\", posting to channel \"{}\"".format(self.user, guild.name, channel.name))

    async def my_background_task(self):
        await self.wait_until_ready()
        while not self.is_closed():
            if self.guild is not None and self.channel is not None:
                await report_ib_lobbies(self.channel)

            await asyncio.sleep(5)


client = DiscordClient()
client.run(DISCORD_TOKEN)
