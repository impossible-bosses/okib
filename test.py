import asyncio
import discord
import os
import requests
import traceback

from secret import DISCORD_GUILD, DISCORD_TOKEN

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

def get_ib_lobbies():
    response = requests.get("https://api.wc3stats.com/gamelist")
    games = response.json()["body"]
    if not isinstance(games, list):
        raise Exception("Property 'games' in HTTP response is not a list, {}".format(type(games)))

    lobbies = [Lobby(game) for game in games]
    ib_lobbies = []
    for lobby in lobbies:
        if lobby.map.find("Impossible") != -1 and lobby.map.find("Bosses") != -1:
            ib_lobbies.append(lobby)

    return ib_lobbies

class DiscordClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())
        self.guild = None

    async def on_ready(self):
        for guild in self.guilds:
            if guild.name == DISCORD_GUILD:
                break

        for channel in guild.text_channels:
            if channel.name == "general":
                break

        self.guild = guild
        self.channel = channel
        print("Bot \"{}\" connected to Discord on guild \"{}\"".format(self.user, guild.name))

    async def my_background_task(self):
        await self.wait_until_ready()
        while not self.is_closed():
            if self.guild is not None and self.channel is not None:
                try:
                    lobbies = get_ib_lobbies()
                    for lobby in lobbies:
                        await self.channel.send("{} | {} | {}/{}".format(lobby.name, lobby.map, lobby.slots_taken, lobby.slots_total))
                except Exception as e:
                    traceback.print_exc()

            await asyncio.sleep(5)


client = DiscordClient()
client.run(DISCORD_TOKEN)
