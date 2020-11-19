import asyncio
import datetime
import discord
from discord.ext.tasks import loop
from discord.ext import commands
from enum import Enum, auto
import git
import io
import logging
import os
import pickle
import requests
import sqlite3
import sys
import traceback

import params

# CONSTANTS
VERSION = 20
ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
DB_FILE_PATH = os.path.join(ROOT_DIR, "IBCE.db")
DB_ARCHIVE_PATH = os.path.join(ROOT_DIR, "archive", "IBCE.db")

# discord connection
_client = discord.ext.commands.Bot(command_prefix="+")
_guild = None
_pub_channel = None

# communication
_initialized = False
_kv_entries = []
_com_channel = None
_im_master = False
_alive_instances = set()
_master_instance = None

# DB
_db_conn = sqlite3.connect(DB_FILE_PATH)
_db_cursor = _db_conn.cursor()

class MessageType(Enum):
    CONNECT = "connect"
    CONNECT_ACK = "connectack"
    LET_MASTER = "letmaster"
    ENSURE_DISPLAY = "ensure"
    SEND_DB = "senddb"
    SEND_DB_ACK = "senddback"
    SEND_WORKSPACE = "sendws"
    SEND_WORKSPACE_ACK = "sendwsack"

class MessageWaitQueue:
    def __init__(self):
        self._waiting = False
        self._event = asyncio.Event()
        self._messages = []

    async def wait(self, timeout):
        if self._waiting:
            raise Exception("Already waiting")

        self._waiting = True
        did_timeout = False
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            assert len(self._messages) == 0
            raise asyncio.TimeoutError
        finally:
            self._waiting = False
        
        self._event.clear()
        messages = list(self._messages)
        self._messages = []
        return messages

class MessageHub:
    def __init__(self):
        self._wait_queues = {}
        for message_type in MessageType:
            self._wait_queues[message_type] = MessageWaitQueue()

    def on_message(self, message_type, message):
        assert isinstance(message_type, MessageType)
        assert isinstance(message, str)
        assert message_type in self._wait_queues

        self._wait_queues[message_type]._messages.append(message)
        self._wait_queues[message_type]._event.set()

    async def wait(self, message_type, timeout):
        return await self._wait_queues[message_type].wait(timeout)

_com_hub = MessageHub()

async def com(to_id, message_type, message = "", file = None):
    assert isinstance(to_id, int)
    assert isinstance(message_type, MessageType)
    assert isinstance(message, str)

    payload = "/".join([
        str(params.BOT_ID),
        str(to_id),
        message_type.value,
        message
    ])
    if file is None:
        await _com_channel.send(payload)
    else:
        await _com_channel.send(payload, file=file)

def archive_db():
    global _db_conn

    _db_conn.close()
    try:
        os.mkdir(os.path.dirname(DB_ARCHIVE_PATH))
    except FileExistsError:
        pass
    os.replace(DB_FILE_PATH, DB_ARCHIVE_PATH)

async def update_db(db_bytes):
    global _db_conn

    archive_db()
    with open(DB_FILE_PATH, "wb") as f:
        f.write(db_bytes)

    _db_conn = sqlite3.connect(DB_FILE_PATH)
    _db_cursor = _db_conn.cursor()

async def send_db(to_id):
    with open(DB_FILE_PATH, "rb") as f:
        await com(to_id, MessageType.SEND_DB, "", discord.File(f))

def update_workspace(workspace_bytes):
    global _open_lobbies, _wc3stats_down_message_id

    workspace_obj = pickle.loads(workspace_bytes)
    _open_lobbies = workspace_obj["open_lobbies"]
    _wc3stats_down_message_id = workspace_obj["wc3stats_down_message_id"]

async def send_workspace(to_id):
    workspace_obj = {
        "open_lobbies": _open_lobbies,
        "wc3stats_down_message_id": _wc3stats_down_message_id
    }
    workspace_bytes = io.BytesIO(pickle.dumps(workspace_obj))
    await com(to_id, MessageType.SEND_WORKSPACE, "", discord.File(workspace_bytes))

async def parse_bot_com(from_id, message_type, message, attachment):
    global _im_master, _alive_instances, _master_instance

    if message_type == MessageType.CONNECT:
        if _im_master:
            await com(from_id, MessageType.CONNECT_ACK, str(VERSION) + "+")
            # It is master's responsibility to send DB and workspace to synchronize newcomer
            await send_db(from_id)
            await send_workspace(from_id)
        else:
            await com(from_id, MessageType.CONNECT_ACK, str(VERSION))

        version = int(message)
        if version == VERSION:
            _alive_instances.add(from_id)
            # TODO maybe explicitly wait for acks from other instances, too?
        else:
            pass # TODO version mismatch
    elif message_type == MessageType.CONNECT_ACK:
        message_trim = message
        if message[-1] == "+":
            _master_instance = from_id
            message_trim = message[:-1]
        version = int(message_trim)
    elif message_type == MessageType.LET_MASTER:
        if _im_master:
            logging.warning("I was unworthy :(")
            _im_master = False

        _master_instance = from_id
    elif message_type == MessageType.ENSURE_DISPLAY:
        pass
    elif message_type == MessageType.SEND_DB:
        db_bytes = await attachment.read()
        await update_db(db_bytes)
        await com(from_id, MessageType.SEND_DB_ACK)
    elif message_type == MessageType.SEND_DB_ACK:
        pass
    elif message_type == MessageType.SEND_WORKSPACE:
        workspace_bytes = await attachment.read()
        update_workspace(workspace_bytes)
        await com(from_id, MessageType.SEND_WORKSPACE_ACK)
    elif message_type == MessageType.SEND_WORKSPACE_ACK:
        pass
    else:
        raise Exception("Unhandled message type {}".format(message_type))

    _com_hub.on_message(message_type, message)

async def self_promote():
    global _im_master, _master_instance

    _im_master = True
    _master_instance = params.BOT_ID
    await com(-1, MessageType.LET_MASTER)
    logging.info("I'm in charge!")

def get_function_hash_string(func, *args, **kwargs):
    # TODO eh, whatever
    return func.__name__ + "." + str(len(args)) + "." + str(len(kwargs))

async def ensure_display(timeout, func, *args, **kwargs):
    func_hash_str = get_function_hash_string(func, *args, **kwargs)
    if _im_master:
        result = await func(*args, **kwargs)
        func_hash_str += ":"
        if result is not None:
            type_str = ""
            if isinstance(result, float):
                type_str = "f"
            elif isinstance(result, int):
                type_str = "i"
            elif isinstance(result, str):
                type_str = "s"
            else:
                raise ValueError("Unhandled return type {}".format(type(result)))
            func_hash_str += type_str + str(result)

        await com(-1, MessageType.ENSURE_DISPLAY, func_hash_str)
        return result
    else:
        response = None
        try:
            logging.info("Waiting for master to run {}".format(func_hash_str))
            response = await _com_hub.wait(MessageType.ENSURE_DISPLAY, 5)
        except asyncio.TimeoutError:
            logging.warning("Timeout on ensure display from master")

        for message in response:
            message_split = message.split(":")
            if len(message_split) != 2:
                raise Exception # TODO eh

            if message_split[0] == func_hash_str:
                if message_split[1] == "":
                    return None

                return_type = message_split[1][0]
                return_value = message_split[1][1:]
                if return_type == "f":
                    return float(return_value)
                elif return_type == "i":
                    return int(return_value)
                elif return_type == "s":
                    return return_value
                else:
                    raise ValueError("Unhandled return type {}".format(return_type))

        raise Exception # TODO master is dead I guess :(

@_client.command()
async def test(ctx):
    await ensure_display(5, ctx.channel.send, "working!")

@_client.command()
async def update(ctx, key):
    if key == params.UPDATE_KEY:
        # No ensure_display here because this isn't a distributed action
        await ctx.channel.send("Received update key. Pulling latest code and rebooting...")

        repo = git.Repo(ROOT_DIR)
        for remote in repo.remotes:
            if remote.name == "origin":
                logging.info("Pulling latest code from remote {}".format(remote))
                remote.pull()
                logging.info("Rebooting")
                exit()
                # os.system("shutdown /r /t 1")

@_client.event
async def on_ready():
    global _guild, _pub_channel, _com_channel, _initialized

    guild_ib = None
    guild_com = None
    for guild in _client.guilds:
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

    _guild = guild_ib
    _pub_channel = channel_pub
    logging.info("Bot \"{}\" connected to Discord on guild \"{}\", pub channel \"{}\"".format(_client.user, guild_ib.name, channel_pub.name))

    _com_channel = channel_com

    logging.info("Connecting to bot network...")
    await com(-1, MessageType.CONNECT, str(VERSION))

    connect_timeout = 10
    try:
        await _com_hub.wait(MessageType.CONNECT_ACK, connect_timeout)
        await _com_hub.wait(MessageType.SEND_DB, connect_timeout)
        await _com_hub.wait(MessageType.SEND_WORKSPACE, connect_timeout)
    except asyncio.TimeoutError:
        logging.info("No connect acks after timeout, assuming control")
        await self_promote()

    logging.info("Connected and synchronized to bot network")
    _initialized = True

@_client.event
async def on_message(message):
    if message.author.id == _client.user.id and message.channel == _com_channel:
        # from this bot user
        message_split = message.content.split("/")
        if len(message_split) != 4:
            logging.error("Invalid bot com: {}".format(message.content))
            return

        from_id = int(message_split[0])
        to_id = int(message_split[1])
        message_type = MessageType(message_split[2])
        content = message_split[3]
        if from_id != params.BOT_ID and (to_id == -1 or to_id == params.BOT_ID):
            # from another bot instance
            logging.info("Communication received from {} to {}, message type {}, content = {}".format(from_id, to_id, message_type, content))

            attachment = None
            if message.attachments:
                attachment = message.attachments[0]
            await parse_bot_com(from_id, message_type, content, attachment)
    else:
        await _client.process_commands(message)

# ==========

LOBBY_REFRESH_RATE = 5

# lobbies
_open_lobbies = set()
_wc3stats_down_message_id = None

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

    def is_updated(self, new):
        return self.name != new.name or self.server != new.server or self.map != new.map or self.host != new.host or self.slots_taken != new.slots_taken or self.slots_total != new.slots_total

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
    global _open_lobbies, _wc3stats_down_message_id

    timeout = LOBBY_REFRESH_RATE * 2

    try:
        lobbies = get_ib_lobbies()
    except Exception as e:
        logging.error("Error getting IB lobbies")
        traceback.print_exc()

        if _wc3stats_down_message_id is None:
            _wc3stats_down_message_id = await ensure_display(timeout, send_message, channel, ":warning: WARNING: https://wc3stats.com/gamelist API down, no lobby list :warning:")
        return

    if _wc3stats_down_message_id is not None:
        message = None
        try:
            message = await channel.fetch_message(_wc3stats_down_message_id)
        except Exception as e:
            pass

        _wc3stats_down_message_id = None
        if message is not None:
            await ensure_display(timeout, message.delete)

    new_open_lobbies = set()
    for lobby in _open_lobbies:
        still_open = lobby in lobbies
        lobby_latest = lobby
        should_update = False
        if still_open:
            for lobby2 in lobbies:
                if lobby2 == lobby:
                    should_update = lobby.is_updated(lobby2)
                    lobby_latest = lobby2
                    lobby_latest.message_id = lobby.message_id
                    break
            new_open_lobbies.add(lobby_latest)

        logging.info("Lobby open={}, updated={}: {}".format(still_open, should_update, lobby_latest))
        if should_update:
            try:
                message = await channel.fetch_message(lobby.message_id)
            except Exception as e:
                logging.error("Error fetching message with ID {}".format(lobby.message_id))
                traceback.print_exc()
                continue

            try:
                message_info = lobby_latest.to_discord_message_info(still_open)
                if message_info is None:
                    logging.info("Lobby skipped: {}".format(lobby_latest))
                    continue
            except Exception as e:
                logging.error("Failed to get lobby as message info for \"{}\"".format(lobby_latest.name))
                traceback.print_exc()
                continue

            await ensure_display(timeout, message.edit, embed=message_info["embed"])

    _open_lobbies = new_open_lobbies

    for lobby in lobbies:
        if lobby not in _open_lobbies:
            try:
                message_info = lobby.to_discord_message_info()
                if message_info is None:
                    logging.info("Lobby skipped: {}".format(lobby))
                    continue
                logging.info("Lobby created: {}".format(lobby))
                message_id = await ensure_display(timeout, send_message, channel, message_info["message"], message_info["embed"])
            except Exception as e:
                logging.error("Failed to send message for lobby \"{}\"".format(lobby.name))
                traceback.print_exc()
                continue

            lobby.message_id = message_id
            _open_lobbies.add(lobby)

@loop(seconds=LOBBY_REFRESH_RATE)
async def refresh_ib_lobbies():
    if not _initialized:
        return

    logging.info("Refreshing lobby list")
    try:
        await report_ib_lobbies(_pub_channel)
    except Exception as e:
        logging.error("Exception in report_ib_lobbies")
        traceback.print_exc()

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
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    refresh_ib_lobbies.start()
    _client.run(params.BOT_TOKEN)


"""
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

async def ensureDisplay(fun, tobereturned=None):
    global _callback

    if _im_master:
        if tobereturned != None:
            globals()[tobereturned] = await fun()
            await com("ALL", "rb", str(tobereturned) + "&" + str(""))#testvariable))
        else:
            await fun()
            await com("ALL", "rb", "&")
    else:
        bakup = functools.partial(backupMaster, fun, tobereturned)
        _callback = Timer(2, bakup)

async def backupMaster(fun, tobereturned):
    global _master_instance, _alive_instances, _callback

    logging.info(_alive_instances)
    logging.info(_master_instance)
    
    if _master_instance == None:
        _alive_instances.remove(max(_alive_instances))
    else:
        _alive_instances.remove(_master_instance)
        _master_instance = None
    if max(_alive_instances) == params.BOT_ID:
        await self_promote()
        if tobereturned is not None:
            globals()[tobereturned] = await fun()
        else:
            await fun()
    else:
        bakup = functools.partial(backupMaster, fun, tobereturned)
        _callback = Timer(2, bakup)

async def updateDB(att):
    global _db_conn, _db_conn

    archiveDB()
    await att.save(fp=att.filename)
    _db_conn = sqlite3.connect(DB_FILE_PATH)
    _db_cursor = _db_conn.cursor()
    #last_db_entry_startup = getlastdb_entry()
    await com("ALL", "is_syncronized", "")

def archiveDB():
    _db_conn.close()
    try:
        os.mkdir(os.path.dirname(DB_ARCHIVE_PATH))
    except FileExistsError:
        pass
    os.replace(DB_FILE_PATH, DB_ARCHIVE_PATH)

async def sendDB(to_id):
    with open(DB_FILE_PATH, "rb") as f:
        await _com_channel.send(str(params.BOT_ID) + "/" + str(to_id) + "/DB&", file=discord.File(f.name))

async def parseBotCom(from_id, botcom, att = None):
    global _im_master, _alive_instances, _master_instance, _callback
 
    tag = botcom.split("&")[0]
    param1 = botcom.split("&")[1]
    if tag == "connect":
        #param1 = sender version number
        if int(param1) == VERSION:
            if _im_master:
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
    if tag == "rb":
        #stop monitoring display integrity
        _callback.cancel()
        #we might have some value to affect that can only be done by the master
        param2 = botcom.split('&')[2]
        if param1 != "":
            globals()[param1] = param2
        if from_id != _master_instance :
            #some bot has backed up the fallen master
            #consider the previous master down
            _alive_instances.remove(_master_instance)
            _master_instance = from_id
            logging.info("master is now " + str(from_id))           
    if tag == "v":
        #alive callback from other bots
        #needed so every instance knows who's master and increments ALIVE_INSTANCE
        isMaster = botcom.split("&")[2]
        if isMaster == "yes":     
            _master_instance = from_id
            _callback.cancel()    
        _alive_instances.append(from_id)   
    if tag == "pythonFile":
        await updatePython(att)
    if tag == "updateMe":
        await sendPython()
    if tag == "DB":
        _alive_instances.append(params.BOT_ID)
        await updateDB(att)
    if tag == "is_syncronized":
        pass
"""