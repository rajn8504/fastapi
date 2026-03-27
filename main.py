Error சிறியது — `SUPERTREND_PERIOD` `AlphaStrategy`-ல் இல்லை, `IndicatorEngine`-ல் உள்ளது. ஒரே line fix:

Viewed main.py:283-295
Edited main.py

Fix ஆனது. இப்போது **VS Code-ல் இருந்து சரியாக copy** செய்வதற்கு:

Ran command: `Get-Content "f:\n8n\main.py" -Raw | Set-Clipboard; Write-Host "DONE - $(((Get-Content 'f:\n8n\main.py').Count)) lines copied to clipboard"`

**1400 lines — சரியான main.py clipboard-ல் ready!**

இப்போது **உடனே** (வேறு எதுவும் copy செய்யாமல்):

1. **GitHub** → உங்கள் repo → `main.py` → ✏️ Edit
2. `Ctrl+A` (GitHub editor-ல் எல்லாம் select)
3. `Ctrl+V` (paste)
4. **Commit changes** → Railway auto-deploy

---

Deploy ஆனவுடன் logs-ல் இப்படி வர வேண்டும்:
```
🎮 Simulation loop started — tick every 3s, candle every 30s
📊 Demo candle #1 | Price=54xxx RSI=50.0 ST=UP
📊 Demo candle #2 | Price=54xxx RSI=52.3 ST=UP
...
📊 Demo candle #12 | Price=55xxx RSI=63.1 ST=UP
🎯 DEMO CALL | RSI=63.1 | ST=UP
✅ ORDER PLACED — CALL  ← Telegram-ல் வரும்!
```

**`/starttrade` → Paper Trade** select செய்யுங்கள் — சுமார் **6-8 நிமிடத்தில்** signal வரும் ✅
