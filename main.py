from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from core.config import get_settings
from core.database import get_manager
from routers import health, sessions, query, chat, servers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)
settings = get_settings()


def _ensure_default_server_in_registry():
    """
    If the server_registry table is empty AND .env has DB_SERVER configured,
    auto-insert the .env server as the default entry so the frontend
    dropdown is never empty on first run.
    """
    if not settings.db_server:
        return

    from services.registry_service import list_servers, add_server
    from models.schemas import ServerCreateRequest

    try:
        existing = list_servers()
        if existing:
            return  # registry already has entries — nothing to do

        logger.info("Registry is empty — auto-registering .env default server...")
        req = ServerCreateRequest(
            display_name=settings.db_server.replace("\\", "-"),
            host_name=settings.db_server,
            port=1433,
            db_name=settings.db_name or "master",
            auth_type="sql" if settings.db_user else "windows",
            db_user=settings.db_user or None,
            db_password=settings.db_password or None,
            environment="prod",
            notes="Auto-registered from .env on first startup",
        )
        new_srv = add_server(req)
        logger.info(f"Auto-registered server_id={new_srv.server_id} '{new_srv.display_name}'")
    except Exception as e:
        logger.warning(f"Could not auto-register default server: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("DbaAgent starting — loading server registry...")
    try:
        from services.registry_service import list_servers, get_server_credentials
        from routers.servers import _load_into_pool

        # Auto-seed the registry if this is a fresh deployment
        _ensure_default_server_in_registry()

        servers_list = list_servers()
        mgr = get_manager()
        for srv in servers_list:
            try:
                auth = getattr(srv, "auth_type", "sql")
                if auth == "windows":
                    user, pwd = "", ""
                else:
                    user, pwd = get_server_credentials(srv.server_id)
                _load_into_pool(srv, user, pwd)
                logger.info(
                    f"  Loaded server_id={srv.server_id} "
                    f"'{srv.display_name}' ({srv.host_name}) [{auth}]"
                )
            except Exception as e:
                logger.warning(f"  Could not load server_id={srv.server_id}: {e}")

        logger.info(f"Registry loaded — {len(servers_list)} server(s) registered.")

    except Exception as e:
        logger.warning(f"Registry load failed (may not be configured yet): {e}")
        logger.info("Falling back to .env single-server mode (server_id=0).")

    yield

    get_manager().close_all()
    logger.info("DbaAgent stopped — all connections closed.")


app = FastAPI(
    title="DBA Agent API",
    description="Real-time SQL Server monitoring — multi-server.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(servers.router)
app.include_router(health.router)
app.include_router(sessions.router)
app.include_router(query.router)
app.include_router(chat.router)


@app.get("/", tags=["Root"])
def root():
    mgr = get_manager()
    return {
        "service": "DBA Agent API",
        "version": "2.0.0",
        "docs": "/docs",
        "registered_servers": mgr.registered_ids(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
