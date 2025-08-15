from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from .controllers import math_controller, health_controller
from .middleware import RateLimitMiddleware, AuthMiddleware
from .config import settings
from .database import init_db, close_db
from .cache import init_cache, close_cache
from .messaging import init_nats, close_nats

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    await init_cache()
    await init_nats()
    yield
    # Shutdown
    await close_db()
    await close_cache()
    await close_nats()

app = FastAPI(
    title="Math Microservice",
    version="1.0.0",
    lifespan=lifespan
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RateLimitMiddleware, requests_per_minute=100)
app.add_middleware(AuthMiddleware)

# Instrumentation
Instrumentator().instrument(app).expose(app)
FastAPIInstrumentor.instrument_app(app)

# Routers
app.include_router(math_controller.router, prefix="/compute", tags=["mathematics"])
app.include_router(health_controller.router, prefix="/health", tags=["system"])