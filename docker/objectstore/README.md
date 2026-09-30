# Object store identity config

`s3_config.json` must be **strict JSON** — SeaweedFS parses it at startup and
fails on comments, which is why this explanation lives in a separate file.

## Why the `anonymous` identity has `Admin`

SeaweedFS requires the `Admin` action to **create** a bucket. Without it, the
unauthenticated `PUT` in [`../objectstore-init/create_buckets.sh`](../objectstore-init/create_buckets.sh)
returns `403` and `make up` **fails on a genuinely empty store** — which is
exactly what a marker running a clean clone gets.

This went unnoticed through Phases 0–2 because the bucket already existed on a
long-lived Docker volume. It only surfaced in Phase 3 after a `docker compose
down -v`, and it is a real reproducibility bug: reproducibility from a clean
clone is directly assessed (plan §13).

Granting it is acceptable **here and nowhere else**: plan §14 states explicitly
that there is no authentication on the API or Kafka in this environment, so the
object store is not the one component holding a security line that everything
else has already conceded.

The alternative — signing an AWS SigV4 request inside a `curl`-only init
container — is a lot of fragile shell to create one bucket, and would be the
wrong place to spend the effort.

**Production would differ** (scoped IAM credentials per service, no anonymous
identity at all); that is noted in the report's production-scale section
alongside the other demonstration-scale concessions.
