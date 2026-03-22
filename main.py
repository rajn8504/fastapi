from fastapi import FastAPI
import uvicorn
import os
from datetime import datetime
import pytz

app = FastAPI()

@app.get("/")
async def root():
    # இந்திய நேரப்படி தற்போதைய நேரத்தை எடுக்கிறோம்
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')
    return {"status": "Algo_Live", "ist_time": current_time}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
