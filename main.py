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

# @archi: I removed explicit param loading for now, but we can bring it back if you want
import params

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
DB_FILE_PATH = os.path.join(ROOT_DIR, "IBCE.db")
DB_ARCHIVE_PATH = os.path.join(ROOT_DIR, "archive", "IBCE.db")

# communication params
COM_CHANNEL = None
imMaster = False
ALIVE_INSTANCES = []
master_instance = None
callback = None
testvariable = None

# DB related
conn = sqlite3.connect(DB_FILE_PATH)
cursor = conn.cursor()

VERSION = 20

open_lobbies = set()
wc3stats_down_message = None

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
    global open_lobbies, wc3stats_down_message

    try:
        lobbies = get_ib_lobbies()
    except Exception as e:
        logging.error("Error getting IB lobbies")
        traceback.print_exc()

        if wc3stats_down_message is None:
            wc3stats_down_message = await channel.send(content=":warning: WARNING: https://wc3stats.com/gamelist API down, no lobby list :warning:")
        return

    if wc3stats_down_message is not None:
        try:
            await wc3stats_down_message.delete()
        except Exception as e:
            pass
        wc3stats_down_message = None

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

async def com(to_id, key, value = ""):
    await COM_CHANNEL.send(str(params.BOT_ID) + "/" + str(to_id) + "/" + key + "&" + value)

async def self_promote(case = None):
    global imMaster
    global master_instance
    global ALIVE_INSTANCES
    
    imMaster = True
    await com("ALL","letmaster")
    master_instance = params.BOT_ID
    if case == "forced":
        ALIVE_INSTANCES.append(params.BOT_ID)
    print("i'm in charge !")

async def ensureDisplay(fun, tobereturned = None):
    global callback
    if imMaster:
        if tobereturned != None:
            globals()[tobereturned] = await fun()
            await com("ALL", "rb", str(tobereturned) + "&" + str(testvariable))
        else:
            await fun()
            await com("ALL", "rb", "&")
    else:
        bakup = functools.partial(backupMaster, fun, tobereturned)
        callback = Timer(2, bakup)

async def backupMaster(fun, tobereturned):
    global ALIVE_INSTANCES
    global master_instance
    global callback
    print(ALIVE_INSTANCES)
    print(master_instance)
    
    if master_instance == None:
        ALIVE_INSTANCES.remove(max(ALIVE_INSTANCES))
    else:
        ALIVE_INSTANCES.remove(master_instance)
        master_instance = None
    if max(ALIVE_INSTANCES) == params.BOT_ID:
        await self_promote()
        if tobereturned is not None:
            globals()[tobereturned] = await fun()
        else:
            await fun()
    else:
        bakup = functools.partial(backupMaster, fun, tobereturned)
        callback = Timer(2, bakup)

async def updateDB(att):
    global conn
    global cursor
    archiveDB()
    await att.save(fp=att.filename)
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    #last_db_entry_startup = getlastdb_entry()
    await com("ALL", "is_syncronized", "")

def archiveDB():
    conn.close()
    try:
        os.mkdir(os.path.dirname(DB_ARCHIVE_PATH))
    except FileExistsError:
        pass
    os.replace(DB_FILE_PATH, DB_ARCHIVE_PATH)

async def sendDB(to_id):
    with open(DB_FILE_PATH, "rb") as f:
        await COM_CHANNEL.send(str(params.BOT_ID) + "/" + str(to_id) + "/DB&", file=discord.File(f.name))

async def parseBotCom(from_id, botcom, att = None):
    global imMaster
    global ALIVE_INSTANCES
    global master_instance
    global callback
 
    tag = botcom.split("&")[0]
    param1 = botcom.split("&")[1]
    if tag == "connect":
        #param1 = sender version number
        if int(param1) == VERSION:
            if imMaster:
                await com(from_id, "v", str(VERSION) + "&yes")
                #this is master job to send database and workspace to synchronise newcomer
                await sendDB(from_id)
                #TODO
                #await sendWS(from_id)
            else:
                await com(from_id, "v", str(VERSION) + "&no")
        else:
            #VERSION MISSMATCH
            #TODO
            pass
            
#             await requestUpdate()
#         elif int(param1) < VERSION:
#             await letMaster()
#             await sendPython()
#         else:
#             bot_master = False
#             await giveMaster()
    if tag == "rb" :
        #stop monitoring display integrity
        callback.cancel()
        #we might have some value to affect that can only be done by the master
        param2 = botcom.split('&')[2]
        if param1 != "":
            globals()[param1] = param2
        if from_id != master_instance :
            #some bot has backed up the fallen master
            #consider the previous master down
            ALIVE_INSTANCES.remove(master_instance)
            master_instance = from_id
            print("master is now " + str(from_id))           
    if tag == "v":
        #alive callback from other bots
        #needed so every instance knows who's master and increments ALIVE_INSTANCE
        isMaster = botcom.split("&")[2]
        if isMaster == "yes":     
            master_instance = from_id
            callback.cancel()    
        ALIVE_INSTANCES.append(from_id)   
    if tag == "pythonFile":
        await updatePython(att)
    if tag == "updateMe":
        await sendPython()
    if tag == "DB":
        ALIVE_INSTANCES.append(params.BOT_ID)
        await updateDB(att)
    if tag == "is_syncronized":
        pass

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

class DiscordClient(discord.ext.commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        #self.bg_task = self.loop.create_task(self.refresh_ib_lobbies())
        self.guild = None
        self.pub_channel = None

        @self.command()
        async def test(ctx):
            p = functools.partial(ctx.channel.send, "working!")
            await ensureDisplay(p)

        @self.command()
        async def update(ctx, key):
            if key == params.UPDATE_KEY:
                p = functools.partial(ctx.channel.send, "Received update key. Pulling latest code and rebooting...")
                await ensureDisplay(p)
                repo = git.Repo(ROOT_DIR)
                for remote in repo.remotes:
                    if remote.name == "origin":
                        logging.info("Pulling latest code from remote {}".format(remote))
                        remote.pull()
                        logging.info("Rebooting")
                        exit()
                        # os.system("shutdown /r /t 1")

    async def on_ready(self):
        global COM_CHANNEL
        global callback

        guild_ib = None
        guild_com = None
        for guild in self.guilds:
            if guild.name == params.GUILD_NAME:
                guild_ib = guild
            if guild.id == params.COM_GUILD_ID:
                guild_com = guild

        if guild_ib is None:
            raise Exception("IB guild not found: \"{}\"".format(params.GUILD_NAME))
        if guild_com is None:
            raise Exception("Com virtual guild not found")

        channel_pub = None
        for channel in guild_ib.text_channels:
            if channel.name == params.PUB_CHANNEL_NAME:
                channel_pub = channel
        if channel_pub is None:
            raise Exception("Pub channel not found: \"{}\" in guild \"{}\"".format(params.PUB_CHANNEL_NAME, guild_ib.name))

        channel_com = None
        for channel in guild_com.text_channels:
            if channel.id == params.COM_CHANNEL_ID:
                channel_com = channel
        if channel_com is None:
            raise Exception("Com channel not found")

        self.guild = guild_ib
        self.channel_pub = channel_pub
        logging.info("Bot \"{}\" connected to Discord on guild \"{}\", posting to channel \"{}\"".format(self.user, guild_ib.name, channel_pub.name))

        COM_CHANNEL = channel_com
        await com("ALL", "connect", str(VERSION))
        callback = Timer(3, functools.partial(self_promote, "forces"))

    async def on_message(self, message):
        if message.author.id == 698490662143655967 and message.channel == COM_CHANNEL:
            #from this bot
            FROM_id = int(message.content.split("/")[0])
            TO = message.content.split("/")[1]
            real_content = message.content.split("/")[2]

            if FROM_id != params.BOT_ID and (TO == "ALL" or TO == str(params.BOT_ID)):
                #from another bot
                print("communication received from : " + str(FROM_id) + " to " + TO + " content = " + real_content)
                if not message.attachments:
                    await parseBotCom(FROM_id,real_content)
                else :
                    await parseBotCom(FROM_id,real_content,message.attachments[0])
        else:
            await client.process_commands(message)

    async def refresh_ib_lobbies(self):
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
    logs_dir = os.path.join(ROOT_DIR, "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    datetime_now = datetime.datetime.now()
    log_file_path = os.path.join(logs_dir, "{}.log".format(datetime_now.strftime("%Y%m%d_%H%M%S")))
    print("Log file: {}".format(log_file_path))

    logging.basicConfig(
        filename=log_file_path, level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    client = DiscordClient(command_prefix="+")
    client.run(params.BOT_TOKEN)
    time.sleep(10)
