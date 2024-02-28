# IBCE Bots

To run the bot, you need Python 3 and all the bot's dependencies. Install them with

```sh
python -m pip install -r requirements.txt
```

You also need to set up the `params.py` and `constants.py` files.

Example `params.py` file:
```py
# For the bot COM network.
BOT_ID = 123
# The Discord bot token. Ask Archi or Patio for the OKIB token, but you should test with your own.
BOT_TOKEN = "<discord_bot_token>"
# Controls if the whole machine will reboot after a code update, or only the Python instance.
REBOOT_ON_UPDATE = True
```

Example `constants.py` file:
```py
GUILD_NAME = "IB CAFETERIA"
BNET_CHANNEL_NAME = "pub-games"
ENT_CHANNEL_NAME = "general-chat"

# The COM guild. These are just random numbers - enter a valid Discord server and channel.
COM_GUILD_ID = 77929088919283945
COM_CHANNEL_ID = 77906795912383437

SHAMAN_ID = 431854421635366912
PEON_ID = 431854796748619777
PUB_HOST_ID = 791279611311947796

COMMAND_CHARACTER = '!'
```
