import aiohttp
import asyncio
import datetime
import discord
from discord.ext.tasks import loop
from discord.ext import commands
from enum import Enum, auto
import functools
import git
import io
import logging
import os
import pickle
import sqlite3
import sys
import traceback

import params

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))

def get_source_version():
    repo = git.Repo(ROOT_DIR)
    head_commit_sha = repo.head.commit.binsha
    all_commits = repo.iter_commits()
    total = 0
    index = -1
    for commit in all_commits:
        if commit.binsha == head_commit_sha:
            index = total
        total += 1

    if index == -1:
        raise Exception("HEAD commit sha not found: {}".format(repo.head.commit.hexsha))
    return total - index

class MessageType(Enum):
    CONNECT = "connect"
    CONNECT_ACK = "connectack"
    LET_MASTER = "letmaster"
    ENSURE_DISPLAY = "ensure"
    SEND_DB = "senddb"
    SEND_DB_ACK = "senddback"
    SEND_WORKSPACE = "sendws"
    SEND_WORKSPACE_ACK = "sendwsack"

class Message:
    def __init__(self, timestamp, message):
        self.timestamp = timestamp
        self.message = message

class MessageHub:
    MAX_AGE_SECONDS = 30

    def __init__(self):
        self._message_queues = {}
        for message_type in MessageType:
            self._message_queues[message_type] = []

    def on_message(self, message_type, message):
        assert isinstance(message_type, MessageType)
        assert isinstance(message, str)
        assert message_type in self._message_queues

        # TODO should I use the "real" message timestamp?
        timestamp_now = datetime.datetime.now()
        msg = Message(timestamp_now, message)
        self._message_queues[message_type].append(msg)

        # Trim old messages based on max age
        timestamp_cutoff = timestamp_now - datetime.timedelta(seconds=MessageHub.MAX_AGE_SECONDS)
        for message_type in self._message_queues.keys():
            self._message_queues[message_type] = [
                m for m in self._message_queues[message_type] if m.timestamp > timestamp_cutoff
            ]

    def got_message(self, message_type, window_seconds):
        assert isinstance(message_type, MessageType)
        assert message_type in self._message_queues

        timestamp_cutoff = datetime.datetime.now() - datetime.timedelta(seconds=window_seconds)
        messages_in_window = [
            m for m in self._message_queues[message_type] if m.timestamp > timestamp_cutoff
        ]
        return len(messages_in_window) > 0

# constants
DB_FILE_PATH = os.path.join(ROOT_DIR, "IBCE_WARN.db")
DB_ARCHIVE_PATH = os.path.join(ROOT_DIR, "archive", "IBCE_WARN.db")
VERSION = get_source_version()
print("Source version {}".format(VERSION))

# discord connection
client_intents = discord.Intents().default()
client_intents.members = True
_client = discord.ext.commands.Bot(command_prefix="+", intents=client_intents)
_guild = None
_pub_channel = None

# communication
_initialized = False
_kv_entries = []
_com_channel = None
_im_master = False
_alive_instances = set()
_master_instance = None
_callbacks = []
_message_hub = MessageHub()
_is_master_timeout = True

# globals / workspace
_open_lobbies = set()
_api_down_tries = 0

class TimedCallback:
    def __init__(self, t, func, *args, **kwargs):
        self._timeout = t
        self.callback = functools.partial(func, *args, **kwargs)
        self._task = asyncio.ensure_future(self._job())
        
    async def _job(self):
        await asyncio.sleep(self._timeout)
        await self.callback()

    def cancel(self):
        self._task.cancel()

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
    try:
        os.mkdir(os.path.dirname(DB_ARCHIVE_PATH))
    except FileExistsError:
        pass
    os.replace(DB_FILE_PATH, DB_ARCHIVE_PATH)

async def update_db(db_bytes):
    archive_db()
    with open(DB_FILE_PATH, "wb") as f:
        f.write(db_bytes)

async def send_db(to_id):
    with open(DB_FILE_PATH, "rb") as f:
        await com(to_id, MessageType.SEND_DB, "", discord.File(f))

def update_workspace(workspace_bytes):
    global _open_lobbies

    workspace_obj = pickle.loads(workspace_bytes)
    logging.info("Updating workspace: {}".format(workspace_obj))

    _open_lobbies = workspace_obj["open_lobbies"]
    for key, value in workspace_obj["lobby_message_ids"].items():
        globals()[key] = value

async def send_workspace(to_id):
    lobby_message_ids = {}
    for key, value in globals().items():
        if "lobbymsg" in key:
            lobby_message_ids[key] = value

    workspace_obj = {
        "open_lobbies": _open_lobbies,
        "lobby_message_ids": lobby_message_ids
    }
    logging.info("Sending workspace: {}".format(workspace_obj))

    workspace_bytes = io.BytesIO(pickle.dumps(workspace_obj))
    await com(to_id, MessageType.SEND_WORKSPACE, "", discord.File(workspace_bytes))

def update_source_and_reset():
    repo = git.Repo(ROOT_DIR)
    for remote in repo.remotes:
        if remote.name == "origin":
            logging.info("Pulling latest code from remote {}".format(remote))
            remote.pull()

            new_version = get_source_version()
            if new_version <= VERSION:
                logging.error("Attempted to update, but version didn't upgrade ({} to {})".format(VERSION, new_version))

            if params.REBOOT_ON_UPDATE:
                logging.info("Rebooting")
                os.system("sudo shutdown -r now")
            else:
                logging.info("Exiting")
                exit()

async def parse_bot_com(from_id, message_type, message, attachment):
    global _initialized, _im_master, _alive_instances, _master_instance, _callbacks, _message_hub

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
        elif version > VERSION:
            _alive_instances.add(from_id)
            logging.info("Bot instance {} running newer version {}, updating...".format(from_id, version))
            update_source_and_reset()
        else:
            # TODO outdated version
            pass
        logging.info("After CONNECT message, instances {}".format(_alive_instances))
    elif message_type == MessageType.CONNECT_ACK:
        message_trim = message
        if message[-1] == "+":
            logging.info("Received connect ack from master instance {}".format(from_id))
            message_trim = message[:-1]
            _alive_instances.add(params.BOT_ID)
            _master_instance = from_id
            for callback in _callbacks:
                callback.cancel()
            _callbacks = []
        version = int(message_trim)
        _alive_instances.add(from_id)
        logging.info("After CONNECT_ACK message, instances {}, master {}".format(_alive_instances, _master_instance))
    elif message_type == MessageType.LET_MASTER:
        if _im_master:
            logging.warning("I was unworthy :(")
            _im_master = False
        _master_instance = from_id
    elif message_type == MessageType.ENSURE_DISPLAY:
        for callback in _callbacks:
            callback.cancel()
        _callbacks = []
        if message != "":
            kv = message.split("=")
            value = None
            if len(kv[1]) > 0:
                data_type = kv[1][0]
                value_str = kv[1][1:]
                if data_type == "f":
                    value = float(value_str)
                elif data_type == "i":
                    value = int(value_str)
                elif data_type == "s":
                    value = value_str
                else:
                    raise ValueError("Unhandled return type {}".format(data_type))
            globals()[kv[0]] = value
        if from_id != _master_instance:
            _alive_instances.remove(_master_instance)
            _master_instance = from_id
            logging.info("Master is now {}".format(from_id))
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
        # This is the last step for bot instance connection
        _initialized = True
    elif message_type == MessageType.SEND_WORKSPACE_ACK:
        pass
    else:
        raise Exception("Unhandled message type {}".format(message_type))

    _message_hub.on_message(message_type, message)

# Promotes this bot instance to master
async def self_promote():
    global _initialized, _im_master, _master_instance

    _initialized = True
    _im_master = True
    _master_instance = params.BOT_ID
    # Needed for initialization. Alternatively, can use function arg (what archi was doing)
    if params.BOT_ID not in _alive_instances:
        _alive_instances.add(params.BOT_ID)
    await com(-1, MessageType.LET_MASTER)
    logging.info("I'm in charge!")

# Wrapper around channel.send that only returns the int message ID
async def send_message(channel, *args, **kwargs):
    message = await channel.send(*args, **kwargs)
    return message.id

async def ensure_display_backup(func, *args, window=2, return_name=None, **kwargs):
    global _master_instance, _alive_instances, _callbacks, _is_master_timeout

    logging.info("ensure_display_backup: old master {}, instances {}".format(_master_instance, _alive_instances))

    if _is_master_timeout:
        if _master_instance == None:
            _alive_instances.remove(max(_alive_instances))
        else:
            _alive_instances.remove(_master_instance)
            _master_instance = None

        if max(_alive_instances) == params.BOT_ID:
            await self_promote()

        _is_master_timeout = False
        # Other active callbacks just need to execute, but not resolve master's timeout
        for callback in _callbacks:
            callback.cancel()
            await callback.callback()
        _is_master_timeout = True
    else:
        await ensure_display(func, *args, window=window, return_name=return_name, **kwargs)

async def ensure_display(func, *args, window=2, return_name=None, **kwargs):
    global _callbacks

    if _im_master:
        result = await func(*args, **kwargs)
        message = ""
        if return_name is not None:
            globals()[return_name] = result
            message = return_name + "="
            # TODO should we allow return_name to be set if result is None?
            if result is not None:
                if isinstance(result, float):
                    message += "f"
                elif isinstance(result, int):
                    message += "i"
                elif isinstance(result, str):
                    message += "s"
                else:
                    raise ValueError("Unhandled return type {}".format(type(result)))
                message += str(result)

        await com(-1, MessageType.ENSURE_DISPLAY, message)
    else:
        # TODO: if return_name isn't None, we have to pass it into got_message to make sure we got the return value from master
        if not _message_hub.got_message(MessageType.ENSURE_DISPLAY, window):
            _callbacks.append(TimedCallback(window, ensure_display_backup, func, *args, window=window, return_name=return_name, **kwargs))

@_client.command()
async def ping(ctx):
    if isinstance(ctx.channel, discord.channel.DMChannel):
        logging.info("pingpong")
        await ensure_display(ctx.channel.send, "pong")

@_client.command()
async def update(ctx, bot_id):  # TODO default bot_id=None ??
    global _master_instance, _alive_instances

    bot_id = int(bot_id)
    if bot_id == params.BOT_ID:
        # No ensure_display here because this isn't a distributed action
        await ctx.channel.send("Updating code and restarting...")
        update_source_and_reset()
    else:
        if bot_id in _alive_instances:
            _alive_instances.remove(bot_id)
        else:
            logging.error("Updating instance not in alive instances: {}".format(_alive_instances))

        if _master_instance == bot_id:
            _master_instance = None
            if max(_alive_instances) == params.BOT_ID:
                await self_promote()


@_client.event
async def on_ready():
    global _guild, _pub_channel, _com_channel, _initialized, _alive_instances, _callbacks
    global OKIB_emote
    global NOIB_emote
    OKIB_emote = _client.get_emoji(okib_emoji_id)
    NOIB_emote = _client.get_emoji(noib_emoji_id)

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
    await _client.change_presence(activity=None)

    _com_channel = channel_com

    logging.info("Connecting to bot network...")
    await com(-1, MessageType.CONNECT, str(VERSION))
    _callbacks.append(TimedCallback(3, self_promote))

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
            logging.info("Communication received from {} to {}, {}, content = {}".format(from_id, to_id, message_type, content))

            attachment = None
            if message.attachments:
                attachment = message.attachments[0]
            await parse_bot_com(from_id, message_type, content, attachment)
    else:
        await _client.process_commands(message)

# ==== OKIB ========================================================================================

THIS_BOT_USER = None
OKIB_channel =  None
OKIB_message = None
OKIB_list_message = None
OKIB_message_id = None
okib_reaction = None
noib_reaction = None
OKIB_emote = None
NOIB_emote = None
OKIB_1 = None
NO_POWER_MSG = "You do not have enough power to perform such an action."
okib_emoji_id = 506072066039087164 # ok
okib_emoji_string = '<:okib:' + str(okib_emoji_id)+ '>'
noib_emoji_id = 477544228629512193 # ok
noib_emoji_string = '<:noib:' + str(noib_emoji_id)+ '>'
ibgather_emoji_string ='<:ib:' + str(451846742661398528)+ '>' + '<:ib2:' + str(590986772734017536)+ '>'
okib_members = []
noib_members = []
gatherer = None
gathered = False
gatherTime = datetime.datetime.now()
peon_id = 431854796748619777
shaman_id = 431854421635366912

async def gather():
    gatherstring = ""
    for member in okib_members:
        gatherstring = gatherstring + " " + member.mention

    await ensure_display(OKIB_channel.send, gatherstring + " Time to play !\n" + okib_emoji_string)
    # TODO 2 lines, big emojis
    # await OKIB_channel.send(gatherstring + " Time to play !")
    # await OKIB_channel.send(okib_emoji_string)

def get_okib_list():
    if len(okib_members) == 0:
        return ""
    if len(okib_members) == 1:
        if okib_members[0].nick is not None:
            okiblist = okib_members[0].nick
        else:
            okiblist = okib_members[0].name
    else :
        first = True
        okiblist = ''
        for member in okib_members:
            if first == True:
                if member.nick is not None:
                    okiblist = okiblist + member.nick
                else:
                    okiblist = okiblist + member.name
                first = False
            else:
                if member.nick is not None:
                    okiblist = okiblist + ', ' + member.nick
                else:
                    okiblist = okiblist + ', ' + member.name
    return okiblist

def get_noib_list():
    if len(noib_members) == 0:
        return ""
    if len(noib_members) == 1:
        if noib_members[0].nick is not None:
            noiblist = noib_members[0].nick
        else:
            noiblist = noib_members[0].name
    else:
        first = True
        noiblist = ''
        for member in noib_members:
            if first == True:
                if member.nick is not None:
                    noiblist = noiblist + member.nick
                else:
                    noiblist = noiblist + member.name
                first = False
            else:
                if member.nick is not None:
                    noiblist = noiblist + ', ' + member.nick
                else:
                    noiblist = noiblist + ', ' + member.name
    return noiblist

async def check8():
    global gathered
    if len(okib_members) == 8 and not gathered:
        gathered = True
        await gather()

async def check7():
    global gathered
    if len(okib_members) == 7 and gathered:
        gathered = False

async def list_update():
    global gatherer

    if gatherer.nick is not None:
        g = gatherer.nick
    else:
        g = gatherer.name

    list_content = g + " asks : \n" + okib_emoji_string  + " " + str(len(okib_members)) + "/8 : " + get_okib_list() + '\n' + noib_emoji_string  + " : " + get_noib_list()
    await ensure_display(OKIB_list_message.edit, content=list_content)
    await check8()
    await check7()

async def up(ctx):
    global OKIB_channel
    global OKIB_list_message
    global OKIB_message
    global okib_emoji_string
    global noib_emoji_string
    global ibgather_emoji_string
    global OKIB_list_message_id
    global OKIB_message_id
    global OKIB_emote
    global NOIB_emote

    await OKIB_message.delete()
    await OKIB_list_message.delete()

    OKIB_list_message_id = (await ctx.send(okib_emoji_string +' : \n' + noib_emoji_string +' : ' )).id
    OKIB_message_id = (await ctx.send(ibgather_emoji_string)).id
    OKIB_list_message = await OKIB_channel.fetch_message(OKIB_list_message_id)
    OKIB_message = await OKIB_channel.fetch_message(OKIB_message_id)
    await OKIB_message.add_reaction(OKIB_emote)
    await OKIB_message.add_reaction(NOIB_emote)
    await list_update()

@_client.command()
async def okib(ctx, arg=None):
    global OKIB_channel
    global OKIB_list_message
    global OKIB_message
    global OKIB_list_message_id
    global OKIB_message_id
    global okib_members
    global noib_members
    global gatherer
    global gathered

    logging.info("okib")
    adv = False
    if ctx.message.author.roles[len(ctx.message.author.roles) - 1] <= _guild.get_role(peon_id):
        await ensure_display(ctx.message.channel.send, NO_POWER_MSG)
        return
    if ctx.message.author.roles[len(ctx.message.author.roles) - 1] >= _guild.get_role(shaman_id) or ctx.message.author == gatherer:
        adv = True
    if adv == False and arg != None:
        await ensure_display(ctx.message.channel.send, NO_POWER_MSG)
        return
    await ctx.message.delete()


    if OKIB_channel is None:
        gatherer = ctx.message.author
        gatherTime = datetime.datetime.now()
        #Check for option
        if adv and arg == 'retrieve':
            pass
        else:
            gathered = False
            okib_members = []
            noib_members = []

        OKIB_channel = ctx.message.channel
        OKIB_list_message_id = (await ctx.send(okib_emoji_string +' : \n' + noib_emoji_string +' : ' )).id
        OKIB_message_id = (await ctx.send(ibgather_emoji_string)).id
        OKIB_list_message = await OKIB_channel.fetch_message(OKIB_list_message_id)
        OKIB_message = await OKIB_channel.fetch_message(OKIB_message_id)
        await OKIB_message.add_reaction(OKIB_emote)
        await OKIB_message.add_reaction(NOIB_emote)
        await list_update()
    elif arg == None:
        await up(ctx)
    modify = False
    for user in ctx.message.mentions:
        if user not in okib_members:
            okib_members.append(user)
            modify = True
        if user in noib_members:
            noib_members.remove(user)
            modify = True
    if modify or arg == 'retrieve':
        await list_update()

@_client.command()
async def noib(ctx):
    if ctx.message.author.roles[len(ctx.message.author.roles) - 1] <= _guild.get_role(peon_id):
        await ensure_display(ctx.message.channel.send, NO_POWER_MSG)
        return
    if (ctx.message.author.roles[len(ctx.message.author.roles) - 1] < _guild.get_role(shaman_id)) and ctx.message.author != gatherer:
        if (gatherTime + datetime.timedelta(hours=2)) > datetime.datetime.now():
            await ensure_display(ctx.message.channel.send, NO_POWER_MSG)
            return
        pass

    await ctx.message.delete()

    global OKIB_channel
    global OKIB_list_message
    global OKIB_message

    if not ctx.message.mentions:
        OKIB_channel = None
        if OKIB_list_message is not None:
            await OKIB_list_message.delete()
        if OKIB_message is not None:
            await OKIB_message.delete()
        OKIB_list_message = None
        OKIB_message = None

    modify = False
    for user in ctx.message.mentions:
        if user not in noib_members:
            noib_members.append(user)
            modify = True
        if user in okib_members:
            okib_members.remove(user)
            modify = True
    if modify:
        await list_update()

@_client.event
async def on_reaction_add(reaction, user):
    global okib_members, noib_members

    if reaction.message.id == OKIB_message_id and user.bot == False:
        modify = False
        if user.roles[len(user.roles) - 1] >= _guild.get_role(peon_id):
            try:
                if reaction.emoji == OKIB_emote:
                    if user not in okib_members:
                        okib_members.append(user)
                        modify = True
                    if user in noib_members:
                        noib_members.remove(user)
                        modify = True
                    await reaction.remove(user)

                elif reaction.emoji == NOIB_emote:
                    if user not in noib_members:
                        noib_members.append(user)
                        modify = True
                    if user in okib_members:
                        okib_members.remove(user)
                        modify = True
                    await reaction.remove(user)
                else:
                    await reaction.remove(user)


            except AttributeError:
                await reaction.remove(user)
            if modify:
                await list_update()
        else:
            await reaction.remove(user)

async def peon_promote(member):
    channel = await member.create_dm()
    await ensure_display(channel.send, "Congratulation on being promoted to peon !\nYou are now able to register for official ENT games. To do so, you have to use the :okib: and the :noib: reactions when the clan is looking for ENT players. By declaring you up for a game, you're confirming you can join the game when it starts within 20 mins. You'll get notified when we reach desired number of players and when the game is actually hosted.")

async def grunt_promote(member):
    channel = await member.create_dm()
    await ensure_display(channel.send, "Congratulation on being promoted to grunt !\nYou are now able to start your own gather with the !okib command in the #general channel. When you do so, you have access to the !noib command to cancel your gather, don't forget to cancel it before you leave, so you don't leave an old gather for the next bot user.\nYou can now cancel anyone's gather after at least 2 hours of the first !okib command.\nYou can also remove player from your gather with the !noib @player command. Use these rights wisely.")

async def shaman_promote(member):
    channel = await member.create_dm()
    await ensure_display(channel.send, "Congratulation on being promoted to shaman !\nYou have now full access to all commands of anyone's gather. This include manually adding players (by-passing peon rank requirement) with the !okib @player command and removing any player with the !noib @player command. You can cancel anyone's gather at any time with the basic !noib. Additionally, if you find that someone accidentally cancels a gather, retrieve old list of players with the !okib retrieve command, only if a new gather hasn't been started already.")

@_client.event
async def on_member_update(before, after):
    if before.guild == _guild:
        #promoted
        if before.roles[len(before.roles)-1] < _guild.get_role(shaman_id) and before.roles[len(before.roles)-1] > _guild.get_role(peon_id):
            #was grunt
            if after.roles[len(after.roles)-1] >= _guild.get_role(shaman_id):
                #promoted to shaman
                await shaman_promote(after)
        elif before.roles[len(before.roles)-1] == _guild.get_role(peon_id):
            #was peon
            if after.roles[len(after.roles)-1] > _guild.get_role(peon_id) and after.roles[len(after.roles)-1] < _guild.get_role(shaman_id):
                #promoted to grunt
                await grunt_promote(after)
            elif after.roles[len(after.roles)-1] >= _guild.get_role(shaman_id):
                #promoted to shaman
                await grunt_promote(after)
                await shaman_promote(after)
        elif before.roles[len(before.roles)-1] < _guild.get_role(peon_id):
            #was nothing
            if after.roles[len(after.roles)-1] == _guild.get_role(peon_id):
                #promoted to peon3
                await peon_promote(after)
            elif after.roles[len(after.roles)-1] > _guild.get_role(peon_id) and after.roles[len(after.roles)-1] < _guild.get_role(shaman_id):
                #promoted to grunt
                await peon_promote(after)
                await grunt_promote(after)
            elif after.roles[len(after.roles)-1] >= _guild.get_role(shaman_id):
                #promoted to shaman
                await peon_promote(after)
                await grunt_promote(after)
                await shaman_promote(after)

def nonquery(query):
    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()

@_client.command()
async def warn(ctx, arg1, *, arg2=""):
    if ctx.message.author.roles[len(ctx.message.author.roles) - 1] < _guild.get_role(shaman_id):
        await ensure_display(ctx.message.channel.send, NO_POWER_MSG)
        return

    for user in ctx.message.mentions:
        sqlquery = "INSERT INTO Events (Event_type,Player_id,Reason,Datetime,Warner) VALUES (666,{},\"{}\",\"{}\",\"{}\")".format(user.id, arg2, datetime.datetime.now(), ctx.message.author.display_name)
        nonquery(sqlquery)
        await ensure_display(ctx.message.channel.send, "User <@!{}> has been warned !".format(user.id))
        
@_client.command()
async def pedigree(ctx):
    if ctx.message.author.roles[len(ctx.message.author.roles) - 1] < _guild.get_role(peon_id):
        await ensure_display(ctx.message.channel.send, NO_POWER_MSG)
        return

    conn = sqlite3.connect(DB_FILE_PATH)
    cursor = conn.cursor()
    for user in ctx.message.mentions:
        sqlquery = "SELECT player_id,Reason,Datetime,Warner FROM Events WHERE Event_type = 666 AND Player_id = " + str(user.id)
        cursor.execute(sqlquery)
        row = cursor.fetchone()
        if row is None:
            await ensure_display(ctx.message.channel.send, "User <@!{}> has never been warned yet !".format(user.id))
        else:
            while row:
                await ensure_display(ctx.message.channel.send, "{} => User <@!{}> has been warned by {} for the following reason:\n{}".format(row[2], row[0], row[3], row[1]))
                row = cursor.fetchone()
    conn.close()

# ==== LOBBIES =====================================================================================

LOBBY_REFRESH_RATE = 5
QUERY_RETRIES_BEFORE_WARNING = 10

class MapVersion:
    def __init__(self, file_name, ent_only=False, deprecated=False, counterfeit=False, slots=[8,11]):
        self.file_name = file_name
        self.ent_only = ent_only
        self.deprecated = deprecated
        self.counterfeit = counterfeit
        self.slots = slots

KNOWN_VERSIONS = [
    MapVersion("Impossible.Bosses.v1.10.5"),
    MapVersion("Impossible.Bosses.v1.10.5-ent", ent_only=True),
    MapVersion("Impossible.Bosses.v1.10.4-ent", ent_only=True, deprecated=True),
    MapVersion("Impossible.Bosses.v1.10.3-ent", ent_only=True, deprecated=True),
    MapVersion("Impossible.Bosses.v1.10.2-ent", ent_only=True, deprecated=True),
    MapVersion("Impossible.Bosses.v1.10.1-ent", ent_only=True, deprecated=True),

    MapVersion("Impossible_BossesReforgedV1.09Test", deprecated=True),
    MapVersion("ImpossibleBossesEnt1.09", ent_only=True, deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.09_UFWContinues", counterfeit=True),
    MapVersion("Impossible_BossesReforgedV1.09UFW30", counterfeit=True),
    MapVersion("Impossible_BossesReforgedV1.08Test", deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.07Test", deprecated=True),
    MapVersion("Impossible_BossesTestversion1.06", deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.05", deprecated=True),
    MapVersion("Impossible_BossesReforgedV1.02", deprecated=True),

    MapVersion("Impossible Bosses BetaV3V", deprecated=True),
    MapVersion("Impossible Bosses BetaV3R", deprecated=True),
    MapVersion("Impossible Bosses BetaV3P", deprecated=True),
    MapVersion("Impossible Bosses BetaV3E", deprecated=True),
    MapVersion("Impossible Bosses BetaV3C", deprecated=True),
    MapVersion("Impossible Bosses BetaV3A", deprecated=True),
    MapVersion("Impossible Bosses BetaV2X", deprecated=True),
    MapVersion("Impossible Bosses BetaV2W", deprecated=True),
    MapVersion("Impossible Bosses BetaV2S", deprecated=True),
    MapVersion("Impossible Bosses BetaV2J", deprecated=True),
    MapVersion("Impossible Bosses BetaV2F", deprecated=True),
    MapVersion("Impossible Bosses BetaV2E", deprecated=True),
    MapVersion("Impossible Bosses BetaV2D", deprecated=True),
    MapVersion("Impossible Bosses BetaV2C", deprecated=True),
    MapVersion("Impossible Bosses BetaV2A", deprecated=True),
    MapVersion("Impossible Bosses BetaV1Y", deprecated=True),
    MapVersion("Impossible Bosses BetaV1X", deprecated=True),
    MapVersion("Impossible Bosses BetaV1W", deprecated=True),
    MapVersion("Impossible Bosses BetaV1V", deprecated=True),
    MapVersion("Impossible Bosses BetaV1R", deprecated=True),
    MapVersion("Impossible Bosses BetaV1P", deprecated=True),
    MapVersion("Impossible Bosses BetaV1C", deprecated=True),
]

def get_map_version(map_file):
    for version in KNOWN_VERSIONS:
        if map_file == version.file_name:
            return version

    return None

class Lobby:
    def __init__(self, lobby_dict, is_ent):
        self.is_ent = is_ent
        self.id = lobby_dict["id"]
        self.name = lobby_dict["name"]
        self.map = lobby_dict["map"]
        self.host = lobby_dict["host"]

        if is_ent:
            self.server = lobby_dict["location"]
            self.slots_taken = lobby_dict["slots_taken"]
            self.slots_total = lobby_dict["slots_total"]
            self.created = None
            self.last_updated = None
        else:
            if self.map[-4:] == ".w3x":
                self.map = self.map[:-4]
            self.server = lobby_dict["server"]
            self.slots_taken = lobby_dict["slotsTaken"]
            self.slots_total = lobby_dict["slotsTotal"]
            self.created = lobby_dict["created"]
            self.last_updated = lobby_dict["lastUpdated"]

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return self.id

    def __str__(self):
        return "[id={} ent={} name=\"{}\" server={} map=\"{}\" host={} slots={}/{} message_id={}]".format(
            self.id, self.is_ent, self.name, self.server, self.map, self.host, self.slots_taken, self.slots_total, self.get_message_id()
        )

    def is_ib(self):
        #return self.map.find("Legion") != -1 and self.map.find("TD") != -1 # test
        #return self.map.find("Uther Party") != -1 # test
        return self.map.find("Impossible") != -1 and self.map.find("Bosses") != -1

    def get_message_id_key(self):
        return "lobbymsg{}".format(self.id)

    def get_message_id(self):
        key = self.get_message_id_key()
        if key not in globals():
            return None
        return globals()[key]

    def is_updated(self, new):
        return self.name != new.name or self.server != new.server or self.map != new.map or self.host != new.host or self.slots_taken != new.slots_taken or self.slots_total != new.slots_total

    def to_discord_message_info(self, open=True):
        COLOR_BNET = discord.Colour.from_rgb(0, 255, 0)
        COLOR_ENT = discord.Colour.from_rgb(0, 255, 255)
        COLOR_CLOSED = discord.Colour.from_rgb(255, 0, 0)

        version = get_map_version(self.map)
        mark = ""
        message = ""
        if version is None:
            mark = ":question:"
            message = ":warning: *WARNING: Unknown map version* :warning:"
        elif version.counterfeit:
            mark = ":x:"
            message = ":warning: *WARNING: Counterfeit version* :warning:"
        elif not self.is_ent and version.ent_only:
            mark = ":x:"
            message = ":warning: *WARNING: Incompatible version* :warning:"
        elif version.deprecated:
            mark = ":x:"
            message = ":warning: *WARNING: Old map version* :warning:"

        slots_taken = self.slots_taken
        slots_total = self.slots_total

        if version is not None:
            if not self.is_ent:
                # Not sure why, but IB bnet lobbies have 1 extra slot
                slots_taken -= 1
                slots_total -= 1

            if slots_total not in version.slots:
                raise Exception("Invalid total slots {}, expected {}, for map file {}".format(self.slots_total, versions.slots, self.map))

        embed_title = self.map + "  " + mark
        description = "ENT" if self.is_ent else ""
        color = COLOR_ENT if self.is_ent else COLOR_BNET
        if not open:
            description += " *started/unhosted*"
            color = COLOR_CLOSED
        host = self.host if len(self.host) > 0 else "---"
        players_str = "{} / {}".format(slots_taken, slots_total)

        embed = discord.Embed(title=embed_title, description=description, color=color)
        embed.add_field(name="Lobby Name", value=self.name, inline=False)
        embed.add_field(name="Host", value=host, inline=True)
        embed.add_field(name="Server", value=self.server, inline=True)
        embed.add_field(name="Players", value=players_str, inline=True)

        return {
            "message": message,
            "embed": embed,
        }

async def get_ib_lobbies():
    timeout = aiohttp.ClientTimeout(total=LOBBY_REFRESH_RATE/2)
    session = aiohttp.ClientSession(timeout=timeout)

    # Query APIs
    responses = await asyncio.gather(
        session.get("https://api.wc3stats.com/gamelist"),
        session.get("https://host.entgaming.net/allgames")
    )
    await session.close()

    # Parse wc3stats lobbies
    wc3stats_response_json = await responses[0].json()

    if "body" not in wc3stats_response_json:
        raise Exception("wc3stats HTTP response has no 'body'")
    wc3stats_body = wc3stats_response_json["body"]
    if not isinstance(wc3stats_body, list):
        raise Exception("wc3stats HTTP response 'body' type is {}, not list".format(type(wc3stats_body)))

    wc3stats_lobbies = [Lobby(obj, is_ent=False) for obj in wc3stats_body]
    wc3stats_ib_lobbies = set([lobby for lobby in wc3stats_lobbies if lobby.is_ib()])

    # Parse ENT lobbies
    ent_response_json = await responses[1].json()
    if not isinstance(ent_response_json, list):
        raise Exception("ENT HTTP response type is {}, not list".format(type(ent_response_json)))

    ent_lobbies = [Lobby(obj, is_ent=True) for obj in ent_response_json]
    ent_ib_lobbies = set([lobby for lobby in ent_lobbies if lobby.is_ib()])


    logging.info("IB lobbies: {}/{} from wc3stats, {}/{} from ENT".format(
        len(wc3stats_ib_lobbies), len(wc3stats_lobbies), len(ent_ib_lobbies), len(ent_lobbies)
    ))
    return wc3stats_ib_lobbies | ent_ib_lobbies

async def report_ib_lobbies(channel):
    global _open_lobbies, _api_down_tries

    window = LOBBY_REFRESH_RATE * 2
    try:
        lobbies = await get_ib_lobbies()
    except Exception as e:
        logging.error("Error getting IB lobbies, {} tries, {}".format(_api_down_tries, e))
        traceback.print_exc()

        _api_down_tries += 1
        if _api_down_tries > QUERY_RETRIES_BEFORE_WARNING:
            await _client.change_presence(activity=discord.Activity(
                type=discord.ActivityType.listening,
                name="failed lobby APIs (no data)")
            )
        return

    if _api_down_tries > 0:
        _api_down_tries = 0
        await _client.change_presence(activity=None)

    new_open_lobbies = set()
    for lobby in _open_lobbies:
        lobby_latest = lobby
        still_open = lobby in lobbies
        should_update = not still_open
        if still_open:
            for lobby2 in lobbies:
                if lobby2 == lobby:
                    should_update = lobby.is_updated(lobby2)
                    lobby_latest = lobby2
                    break
            new_open_lobbies.add(lobby_latest)

        if should_update:
            message_id = lobby.get_message_id()
            if message_id is not None:
                message = None
                try:
                    message = await channel.fetch_message(message_id)
                except Exception as e:
                    logging.error("Error fetching message with ID {}, {}".format(message_id, e))
                    traceback.print_exc()

                if message is not None:
                    try:
                        message_info = lobby_latest.to_discord_message_info(still_open)
                        if message_info is None:
                            logging.info("Lobby skipped: {}".format(lobby_latest))
                            continue
                    except Exception as e:
                        logging.error("Failed to get lobby as message info for \"{}\", {}".format(lobby_latest.name, e))
                        traceback.print_exc()
                        continue

                    logging.info("Updating lobby (open={}): {}".format(still_open, lobby_latest))
                    await ensure_display(message.edit, content=message_info["message"], embed=message_info["embed"], window=window)
            else:
                logging.error("Missing message ID for lobby {}".format(lobby))

        if not still_open:
            key = lobby.get_message_id_key()
            if key in globals():
                del globals()[key]

    _open_lobbies = new_open_lobbies

    for lobby in lobbies:
        if lobby not in _open_lobbies:
            try:
                message_info = lobby.to_discord_message_info()
                if message_info is None:
                    logging.info("Lobby skipped: {}".format(lobby))
                    continue
                logging.info("Creating lobby: {}".format(lobby))
                key = lobby.get_message_id_key()
                await ensure_display(send_message, channel, content=message_info["message"], embed=message_info["embed"], window=window, return_name=key)
            except Exception as e:
                logging.error("Failed to send message for lobby \"{}\", {}".format(lobby.name, e))
                traceback.print_exc()
                continue

            _open_lobbies.add(lobby)

@loop(seconds=LOBBY_REFRESH_RATE)
async def refresh_ib_lobbies():
    if not _initialized:
        return

    logging.info("Refreshing lobby list")
    try:
        await report_ib_lobbies(_pub_channel)
    except Exception as e:
        logging.error("Exception in report_ib_lobbies, {}".format(e))
        traceback.print_exc()

# ==== MAIN ========================================================================================

if __name__ == "__main__":
    logs_dir = os.path.join(ROOT_DIR, "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    datetime_now = datetime.datetime.now()
    log_file_path = os.path.join(logs_dir, "{}.{}.log".format(VERSION, datetime_now.strftime("%Y%m%d_%H%M%S")))
    print("Log file: {}".format(log_file_path))

    logging.basicConfig(
        filename=log_file_path, level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    refresh_ib_lobbies.start()
    _client.run(params.BOT_TOKEN)
