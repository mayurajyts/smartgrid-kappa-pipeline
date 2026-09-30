"""FastAPI serving layer (§9) — the read side of the Kappa architecture.

WHY THIS EXISTS IN THE ARCHITECTURE
-----------------------------------
Everything upstream is an immutable, replayable log; the serving tables are a
projection of it. This API is the only way a human or a dashboard reads that
projection, and it deliberately contains NO business logic:

  * No aggregation the streaming jobs did not already do.
  * No billing arithmetic — `processing/transforms/billing.py` is the single
    implementation (§2.2d), and an API that re-derived a total would be a second
    one, which is precisely the dual-logic bug class the Kappa decision rejects
    Lambda over.

So this layer reads rows and shapes JSON. That is the whole job, and keeping it
that narrow is what makes the architecture claim checkable.

WHY FastAPI (§3)
----------------
Async, minimal, and it generates the OpenAPI page at /docs — which is a real
demo asset, not a nicety: clicking through live endpoints is more convincing than
reading curl output, and it costs nothing to have.

`prometheus-client` mounts at /metrics in a few lines, and it shares the ONE
registry in common/metrics.py, so the API's own counters sit alongside the
pipeline's rather than in a second registry Prometheus would have to scrape
separately.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from common.logging_setup import STAGE_SERVE, configure_logging
from common.metrics import REGISTRY
from common.sim_clock import startup_banner
from serving.api import repositories as repo
from serving.api.routers import health, households, zones

SERVICE_NAME = "api"

log = configure_logging(SERVICE_NAME, stage=STAGE_SERVE)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the connection pool eagerly at startup.

    A misconfigured database then fails the container start, instead of letting
    it come up and 500 on every request — which looks like an API bug rather than
    a configuration one.
    """
    repo.init_pool()
    log.info("api_starting", stage=STAGE_SERVE, sim_clock=startup_banner())
    yield
    repo.close_pool()
    log.info("api_stopped", stage=STAGE_SERVE)


app = FastAPI(
    title="Smart Grid Serving API",
    description=(
        "Read side of a Kappa-architecture smart-grid platform. Serves the "
        "projections that Spark Structured Streaming writes to PostgreSQL: "
        "zone load and renewable mix (R1, R2), household bills (R5, R6), "
        "restatement history (R7) and pipeline freshness."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(zones.router)
app.include_router(households.router)


@app.get("/", include_in_schema=False)
def root():
    """Send a browser straight to the interactive docs.

    The demo opens this URL; landing on a bare JSON blob wastes a step.
    """
    return RedirectResponse(url="/docs")


@app.get("/metrics", include_in_schema=False)
def metrics():
    """Prometheus scrape endpoint.

    Serves the SHARED registry from common/metrics.py rather than the library
    default, so the §8 collectors this process touches are exported here and the
    API does not need a second scrape target.
    """
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
