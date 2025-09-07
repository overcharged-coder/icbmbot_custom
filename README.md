# CURRENTLY BUILT FOR WINDOWS

This is my own version of the BotLi bot — tweaked and customized. It runs on YOUR OWN processor, so if your CPU is decent it should easily beat the original BotLi.  

First, install Git and clone this repo:  
    ```git clone https://github.com/overcharged-coder/icbmbot_custom```  

Then install the requirements using Python:  
    ```pip install -r requirements.txt```

---

## Setup

- Adjust paths in `config.yml` (engine, books, syzygy, etc).  
- Some advanced options also live in `trash_bot.py`.  
- Replace the API token with **your own Lichess bot token**. The one I currently have is an old token, and Berserk will throw an error. 
- Run the bot:  
    ```python run_botli.py --config config.yml```  

---

## Configuration

- **Opening books**: put your `.bin` files in `opening/bin`.  
  I recommend keeping the included books — that’s what I’m currently running.  

- **Messages / Talking**: set up the `messages:` section in `config.yml`.  
  The bot will greet players, greet spectators, and say goodbye.  
  Please keep the messages appropriate.  

- **Engine**: choose the Stockfish build for your CPU.  
  Default is `stockfish-windows-x86-64-avxvnni.exe`.  
  I tested by running it manually — if the window closes instantly, try another build (like AVX2 or BMI2).  

- **FEN_CACHE mode**: this exists but is **disabled by default**.  
  Please leave it off for now — it’s a beta feature I use locally.  

---

## Notes

- Currently tuned for **Windows** only.  
- Never commit your Lichess API token to GitHub.  
- Play around with settings — threads, hash size, opening books — to make it your own.  
