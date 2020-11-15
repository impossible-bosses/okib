import asyncio
import datetime
import discord
from discord.ext import commands
from enum import Enum, auto
import functools
import git
import logging
import os
import requests
import sqlite3
import time
import traceback

import params
import secret

client = discord.ext.commands.Bot(command_prefix='+')

#load parameters
COM_CHANNEL = None
TOKEN = params.TOKEN
GUILD2 = params.GUILD
this_bot_id = params.bot_id
ARCHIBOTS_GUILD = params.virtualguildid
general_channel = params.virtualchannelid #general channel
peon_id = params.peon_id
shaman_id = params.shaman_id

#communication params
imMaster = False
ALIVE_INSTANCES = []
master_instance = None
callback = None
testvariable = None

VERSION = 20

#DISCORD_GUILD = "IB CAFETERIA"
#DISCORD_CHANNEL = "pub-games"
DISCORD_GUILD = "Test"
DISCORD_CHANNEL = "general"

root_dir = os.path.dirname(os.path.realpath(__file__))

async def self_promote(case = None):
    global imMaster
    global master_instance
    global ALIVE_INSTANCES
    
    imMaster = True
    await com("ALL","letmaster")
    master_instance = this_bot_id
    if case == "forced":
        ALIVE_INSTANCES.append(this_bot_id)
    print("i'm in charge !")

@client.command()
async def test(ctx):
    print("working")

@client.event
async def on_ready():
    global COM_CHANNEL

    guild_ib = None
    guild_com = None
    for guild in client.guilds:
        if guild.name == DISCORD_GUILD:
            guild_ib = guild

        #if guild.id == ARCHIBOTS_GUILD:
            #guild_com = guild

    if guild_ib is None: # or guild_com is None:
        raise Exception("Guilds not found, {} | {}".format(guild_ib, guild_com))

    channel_ib = None
    for channel in guild_ib.text_channels:
        if channel.name == DISCORD_CHANNEL:
            channel_ib = channel

    if channel_ib is None:
        raise Exception("Channel not found: \"{}\" in guild \"{}\"".format(DISCORD_CHANNEL, guild_ib.name))

    """
    channel_com = None
    for channel in guild_com.text_channels:
        if channel.id == general_channel:
            channel_com = channel

    if channel_com is None:
        raise Exception("failed to get comm channel")
    """

    client.guild = guild_ib
    client.channel = channel
    logging.info("Bot \"{}\" connected to Discord on guild \"{}\", posting to channel \"{}\"".format(client.user, guild_ib.name, channel.name))

    COM_CHANNEL = client.get_guild(ARCHIBOTS_GUILD).get_channel(general_channel) # channel_com
    print(COM_CHANNEL)
    await com("ALL", "connect", str(VERSION))
    t = functools.partial(self_promote,"forces")
    callback = Timer(3, t)

if __name__ == "__main__":
    logs_dir = os.path.join(root_dir, "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    datetime_now = datetime.datetime.now()
    log_file_path = os.path.join(logs_dir, "{}.log".format(datetime_now.strftime("%Y%m%d_%H%M%S")))
    print("Log file: {}".format(log_file_path))

    logging.basicConfig(
        filename=log_file_path, level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    #client = DiscordClient(command_prefix='+')
    client.run(secret.DISCORD_TOKEN)
    time.sleep(10)

class Timer:
    def __init__(self, timeout, callback):
        self._timeout = timeout
        self._callback = callback
        self._task = asyncio.ensure_future(self._job())

        
    async def _job(self):
        await asyncio.sleep(self._timeout)
        await self._callback()

    def cancel(self):
        self._task.cancel()