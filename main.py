import asyncio
import datetime
import discord
from discord.ext.tasks import loop
from discord.ext import commands
from enum import Enum, auto
import functools
import git
import logging
import os
import sqlite3
import sys
import traceback

# @archi: I removed explicit param loading for now, but we can bring it back if you want
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
# _callback = None

# DB
_db_conn = sqlite3.connect(DB_FILE_PATH)
_db_cursor = _db_conn.cursor()

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
"""

class MessageType(Enum):
    CONNECT = "connect"
    CONNECT_ACK = "connectack"
    LET_MASTER = "letmaster"
    ENSURE_DISPLAY = "ensure"

class MessageWaitQueue:
    def __init__(self):
        self._waiting = False
        self._event = asyncio.Event()
        self._messages = []

    async def wait(self, timeout):
        if self._waiting:
            raise Exception("Already waiting")

        assert len(self._messages) == 0

        self._waiting = True
        did_timeout = False
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
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

async def com(to_id, message_type, message = ""):
    assert isinstance(to_id, int)
    assert isinstance(message_type, MessageType)
    assert isinstance(message, str)

    await _com_channel.send("/".join([
        str(params.BOT_ID),
        str(to_id),
        message_type.value,
        message
    ]))

async def parse_bot_com(from_id, message_type, message, attachment):
    global _im_master, _alive_instances, _master_instance

    logging.info("{}, {}, {}".format(from_id, message_type, message))
    if message_type == MessageType.CONNECT:
        if _im_master:
            await com(from_id, MessageType.CONNECT_ACK, str(VERSION) + "+")
            # TODO send DB
        else:
            await com(from_id, MessageType.CONNECT_ACK, str(VERSION))

        version = int(message)
        if version == VERSION:
            _alive_instances.add(from_id)
            # TODO maybe explicitly wait for acks from other instances, too?
        else:
            pass # TODO version mismatch
    elif message_type == MessageType.CONNECTACK:
        message_trim = message
        if message[-1] == "+":
            _master_instance = from_id
            message_trim = message[:-1]
        version = int(message_trim)
    elif message_type == MessageType.LET_MASTER:
        if _im_master:
            logging.warn("I was unworthy :(")
            _im_master = False

        _master_instance = from_id
    elif message_type == MessageType.ENSURE_DISPLAY:
        pass

    _com_hub.on_message(message_type, message)

"""
async def com(to_id, key, value = ""):
    await _com_channel.send(str(params.BOT_ID) + "/" + str(to_id) + "/" + key + "&" + value)
"""

async def self_promote():
    global _im_master, _master_instance

    _im_master = True
    await com(-1, MessageType.LET_MASTER)
    #await com("ALL", "letmaster")
    _master_instance = params.BOT_ID
    logging.info("I'm in charge!")

"""
async def self_promote(case=None):
    global _im_master, _master_instance, _alive_instances
    
    _im_master = True
    await com("ALL", "letmaster")
    _master_instance = params.BOT_ID
    if case == "forced":
        _alive_instances.append(params.BOT_ID)
    logging.info("I'm in charge!")
"""

async def wait_for_master_exec(func):
    return (True, None)

async def ensure_display(func, *args, **kwargs):
    print(_im_master)

    func_str = func.__name__ + "." + str(len(args)) + "." + str(len(kwargs))
    if _im_master:
        result = await func(*args, **kwargs)
        print(result)

        if result is not None:
            if isinstance(result, float):
                func_str += ".f" + str(result)
            if isinstance(result, int):
                func_str += ".i" + str(result)
            if isinstance(result, str):
                func_str += ".s" + str(result)

        await com(-1, MessageType.ENSURE_DISPLAY, func_str)
        return result
    else:
        while True:
            try:
                result = await wait_for_master_exec(func, *args, **kwargs)
            except Exception as e:
                # TODO get master
                pass

            if result[0]:
                return result[1]

"""
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

@_client.command()
async def test(ctx):
    p = functools.partial(ctx.channel.send, "working!")
    await ensureDisplay(p)

@_client.command()
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

@_client.event
async def on_ready():
    global _guild, _pub_channel, _initialized, _com_channel, _im_master, _master_instance#, _callback

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

    logging.info("Connecting to bot network")
    await com(-1, MessageType.CONNECT, str(VERSION))
    try:
        response = await _com_hub.wait(MessageType.CONNECT, 5)
    except asyncio.TimeoutError:
        logging.info("No connect acks after timeout, I'm in charge now")
        _im_master = True
        _master_instance = params.BOT_ID

    logging.info("Connected to bot network")
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

@loop(seconds=5)
async def refresh_ib_lobbies():
    from lobbies import report_ib_lobbies

    if not _initialized:
        return False

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
