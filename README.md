# SKxLinksBot (@SKxLinksBot)

Single Telegram bot:
- **Admin** creates posts (step-by-step + bulk)
- **Share deep link** for users
- Users must join required channels (checked **every time**)
- Full **Rich Message** post delivery
- **Auto-delete after 2 minutes**
- **Multi MongoDB URI** storage (old posts stay on old cluster)

## Force-join channels
- https://t.me/The_Sk08 (`@The_Sk08`)
- https://t.me/Movielink_08 (`@Movielink_08`)

**Important:** Add this bot as **Admin** in both channels (so it can check membership).

## Render deploy (Free Web Service)

1. Push this folder to GitHub
2. Render → New → Web Service
3. **Environment variables:**

| Key | Value |
|-----|--------|
| `BOT_TOKEN` | from @BotFather for @SKxLinksBot |
| `BOT_USERNAME` | `SKxLinksBot` |
| `OWNER_ID` | `8723278238` |
| `MONGO_URI` | your Atlas URI (or `MONGO_URI_1`) |
| `MONGO_URI_2` | optional second cluster |
| `MONGO_URI_3` | optional third |
| `PORT` | `8080` |

4. Start command: `python bot.py`
5. UptimeRobot → HTTP monitor on your Render URL every 5 min

### MongoDB Atlas
1. Create free cluster
2. Database Access → user + password
3. Network Access → `0.0.0.0/0`
4. Connect → Drivers → copy URI  
   Example: `mongodb+srv://user:pass@cluster0.xxx.mongodb.net/skxlinks?retryWrites=true&w=majority`

When first cluster is full, create another Atlas account/cluster and set `MONGO_URI_2`.  
**New posts** go to the first writable URI.  
**Old share links** still load from the URI where they were saved.

## Admin usage
- `/new` or send poster photo → create post
- `/bulk` → poster + paste caption
- Confirm options:
  1. Send to Me
  2. Send to Channel (`/setchannel @Channel`)
  3. **Get Share Link** → `https://t.me/SKxLinksBot?start=CODE`

## User flow
1. Open share link
2. Join both channels
3. Tap **I Joined — Check Again**
4. Receive full post
5. Forward/save — **deleted in 2 minutes**

Leaving a channel removes access until they rejoin (re-checked every time).

## Commands menu
Set automatically on start: start, new, bulk, cancel, setchannel, getchannel, help
