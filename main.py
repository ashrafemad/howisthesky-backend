from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from core import apis
from database import check_database_connection, ensure_indexes


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await check_database_connection()
        await ensure_indexes()
        yield
    except Exception as e:
        print(f"Unable to connect to database, check the exception: {e}")


app = FastAPI(lifespan=lifespan)
app.include_router(apis.router)


@app.get("/health")
async def health():
    return "We are up and raining"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
