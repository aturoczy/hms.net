# hms.net

**Hive Metastore .NET** — a drop-in replacement for the [Apache Hive Metastore](https://hive.apache.org/) written in modern C# / .NET 10.

`hms.net` speaks the **Thrift wire protocol** on port 9083 so existing Hive, Trino, Spark, Presto and HiveServer2 clients can connect without changes. It also exposes a **REST API** for humans and a separate **Apache Iceberg REST Catalog** service for Iceberg-native engines.

---

## Why

The original Hive Metastore is a JVM service with a heavy dependency tree. `hms.net` is a single self-contained .NET binary that:

- Talks the same Thrift wire protocol as the upstream Hive Metastore
- Persists catalog metadata in **PostgreSQL** or **SQLite** via EF Core 10
- Supports the **Apache Iceberg REST Catalog** spec on a separate port
- Caches read-heavy queries in **Redis** with tag-based invalidation
- Emits **OpenTelemetry** traces and metrics out of the box
- Runs happily in a container; multiple instances behind a load balancer stay coherent thanks to the shared Redis cache

---

## Features

- **Thrift Hive Metastore API** (port 9083) — databases, tables, partitions, column statistics, schema lookup, batch table fetch
- **REST API** — the same operations, JSON-friendly, documented with OpenAPI + Scalar
- **Iceberg REST Catalog v1** — namespaces, table load / commit / rename / drop
- **Dual-database support** — PostgreSQL for production, SQLite for local dev / tests; identical schema managed by EF Core migrations
- **Cache-aside with tag-based invalidation** — Redis-backed, transparent to handlers, configurable TTLs
- **CQRS** via MediatR with pipeline behaviors for caching and invalidation
- **Observability** — OpenTelemetry traces, metrics and runtime instrumentation exported over OTLP
- **169+ unit tests** covering handlers, Thrift protocol, Iceberg and caching

---

## Architecture

```
┌─────────────┐   Thrift 9083    ┌──────────────────┐
│  HiveServer2│ ───────────────▶ │                  │
│  Trino/Spark│                  │                  │
└─────────────┘                  │   Hmsnet.Api     │   ┌──────────────┐
                                 │                  │──▶│  PostgreSQL  │
┌─────────────┐   REST /api/*    │  (REST + Thrift) │   │  or SQLite   │
│  curl, UI   │ ───────────────▶ │                  │   └──────────────┘
└─────────────┘                  │                  │
                                 │                  │──▶┌──────────────┐
┌─────────────┐   REST /v1/*     │ Hmsnet.Iceberg   │   │   Redis      │
│  Iceberg    │ ───────────────▶ │ (REST Catalog)   │   └──────────────┘
│  clients    │                  │                  │
└─────────────┘                  └──────────────────┘──▶ OTLP collector
```

**Projects**

| Project | Role |
|---|---|
| `src/Hmsnet.Api` | Main host. Thrift server + REST controllers (databases, tables, partitions, column stats). |
| `src/Hmsnet.Iceberg` | Stand-alone Iceberg REST Catalog endpoint. Shares Core/Infrastructure. |
| `src/Hmsnet.Core` | Domain models, DTOs, MediatR commands/queries, caching abstractions. |
| `src/Hmsnet.Infrastructure` | EF Core `MetastoreDbContext`, services, migrations, Redis cache implementation. |
| `tests/Hmsnet.Tests` | Handler, behavior, caching and Thrift protocol tests (MSTest). |
| `tests/Hmsnet.Iceberg.Tests` | Iceberg catalog tests. |

---

## Requirements

- **.NET 10 SDK** (10.0.100 or newer)
- One of:
  - **PostgreSQL 13+** — recommended for production; migrations ship for this provider
  - **SQL Server 2019+** (also Azure SQL / SQL Edge)
  - **SQLite 3.35+** — zero-config, ideal for local dev and tests
- **Redis 6+** (optional, for distributed caching)
- **Docker & Docker Compose** (optional, for the local dev stack)

---

## Quick start

### Option A — run everything in Docker

A full dev stack (Postgres + Redis + OpenTelemetry + Jaeger + Prometheus + Grafana + HiveServer2) ships in `docker/`:

```bash
cd docker
docker compose up -d
```

Then run the .NET services on the host:

```bash
# main metastore (REST + Thrift)
dotnet run --project src/Hmsnet.Api

# Iceberg REST catalog (separate service)
dotnet run --project src/Hmsnet.Iceberg
```

Once up:

| What | Where |
|---|---|
| REST API docs (Scalar)     | http://localhost:5281/scalar/v1 |
| OpenAPI document           | http://localhost:5281/openapi/v1.json |
| Thrift metastore           | `thrift://localhost:9083` |
| Jaeger UI                  | http://localhost:16686 |
| Grafana                    | http://localhost:3000 (admin / admin) |
| Prometheus                 | http://localhost:9090 |
| HiveServer2 JDBC           | `jdbc:hive2://localhost:10000/` |

### Option B — SQLite, no Docker

Zero external dependencies — a file-backed SQLite database is created on first run.

```bash
export Database__Provider=sqlite
export ConnectionStrings__Metastore="Data Source=metastore.db"
dotnet run --project src/Hmsnet.Api
```

### Option C — container image

CI publishes an image to GitHub Container Registry on every push to `main`:

```bash
docker pull ghcr.io/aturoczy/hms.net:latest
docker run --rm -p 5281:8080 -p 9083:9083 \
  -e ConnectionStrings__Metastore="Host=db;Database=metastore;Username=hmsnet;Password=secret" \
  -e Database__Provider=postgresql \
  ghcr.io/aturoczy/hms.net:latest
```

---

## Configuration

All settings live in `appsettings.json` and can be overridden by environment variables using the standard ASP.NET Core convention (`Section__Key=value`).

```jsonc
{
  "Database": {
    "Provider": "postgresql"           // "postgresql" | "sqlserver" | "sqlite"
  },
  "ConnectionStrings": {
    "Metastore": "Host=localhost;Port=5432;Database=metastore;Username=hmsnet;Password=hmsnet_secret"
  },
  "Thrift": {
    "Port": 9083                       // Hive Metastore wire protocol
  },
  "Redis": {
    "Enabled": false,                  // flip to true to enable distributed cache
    "ConnectionString": "localhost:6379",
    "InstanceName": "hmsnet:",         // key prefix — isolate environments on shared Redis
    "Database": 0
  },
  "OpenTelemetry": {
    "Endpoint": "http://localhost:4317" // OTLP gRPC collector
  }
}
```

### Environment variable examples

```bash
export Database__Provider=postgresql
export ConnectionStrings__Metastore="Host=db;Database=metastore;Username=hmsnet;Password=s3cret"
export Redis__Enabled=true
export Redis__ConnectionString=redis:6379
export OpenTelemetry__Endpoint=http://otel-collector:4317
```

### Database providers

`Database:Provider` picks which EF Core provider backs `MetastoreDbContext`. Supported values (case-insensitive):

| Provider      | Value                      | Connection string example |
|---------------|----------------------------|---------------------------|
| PostgreSQL    | `postgresql` (or `postgres`) | `Host=localhost;Port=5432;Database=metastore;Username=hmsnet;Password=hmsnet_secret` |
| SQL Server    | `sqlserver` (or `mssql`)     | `Server=localhost,1433;Database=metastore;User Id=sa;Password=Hmsnet_secret1;TrustServerCertificate=True` |
| SQLite        | `sqlite`                     | `Data Source=metastore.db` |

The wiring lives in [`src/Hmsnet.Infrastructure/Data/MetastoreDbContextRegistration.cs`](src/Hmsnet.Infrastructure/Data/MetastoreDbContextRegistration.cs) and is shared between `Hmsnet.Api` and `Hmsnet.Iceberg`.

#### Switching providers

Set two keys — either in `appsettings.json`:

```jsonc
{
  "Database":       { "Provider": "sqlserver" },
  "ConnectionStrings": { "Metastore": "Server=localhost,1433;Database=metastore;User Id=sa;Password=Hmsnet_secret1;TrustServerCertificate=True" }
}
```

…or via environment variables (double underscore = section separator):

```bash
# PostgreSQL
export Database__Provider=postgresql
export ConnectionStrings__Metastore="Host=localhost;Port=5432;Database=metastore;Username=hmsnet;Password=hmsnet_secret"

# SQL Server
export Database__Provider=sqlserver
export ConnectionStrings__Metastore="Server=localhost,1433;Database=metastore;User Id=sa;Password=Hmsnet_secret1;TrustServerCertificate=True"

# SQLite
export Database__Provider=sqlite
export ConnectionStrings__Metastore="Data Source=metastore.db"
```

No code change is required — restart the process and EF Core picks up the new provider.

#### Running the backend in Docker

The default `docker compose up -d` only starts PostgreSQL. To use SQL Server instead:

```bash
cd docker
docker compose --profile mssql up -d mssql
# optionally stop postgres if you don't need it:  docker compose stop postgres
```

The image is `mcr.microsoft.com/mssql/server:2022-latest`; the SA password in the compose file (`Hmsnet_secret1`) satisfies SQL Server's complexity rules. First start takes ~30s while SQL Server initialises.

#### Migrations

Migrations run automatically on startup (`db.Database.MigrateAsync()`). The current migrations in `src/Hmsnet.Infrastructure/Migrations/` were generated against **PostgreSQL** and are also accepted by **SQLite** (SQLite is lenient about column types).

For **SQL Server**, the PostgreSQL-specific annotations (`Npgsql:ValueGenerationStrategy`, `character varying`, `boolean`, …) will not apply cleanly. Generate a SQL Server migration set of your own:

```bash
# create SQL Server migrations in a separate folder
dotnet ef migrations add InitialCreate \
  --project src/Hmsnet.Infrastructure \
  --startup-project src/Hmsnet.Api \
  --output-dir Migrations/SqlServer \
  -- sqlserver

# apply them against a running SQL Server
DB_PROVIDER=sqlserver \
DB_CONNECTIONSTRING="Server=localhost,1433;Database=metastore;User Id=sa;Password=Hmsnet_secret1;TrustServerCertificate=True" \
dotnet ef database update \
  --project src/Hmsnet.Infrastructure \
  --startup-project src/Hmsnet.Api \
  -- sqlserver
```

The `--` passes `sqlserver` as the first arg to [`MetastoreDbContextFactory`](src/Hmsnet.Infrastructure/Data/MetastoreDbContextFactory.cs) so the CLI builds the context against the right provider. The factory also honours `DB_PROVIDER` / `DB_CONNECTIONSTRING` environment variables.

For generic `dotnet ef` operations (any provider):

```bash
# add a migration (defaults to postgresql)
dotnet ef migrations add <Name> \
  --project src/Hmsnet.Infrastructure \
  --startup-project src/Hmsnet.Api

# apply migrations against the configured connection
dotnet ef database update \
  --project src/Hmsnet.Infrastructure \
  --startup-project src/Hmsnet.Api
```

### Caching strategy

When `Redis:Enabled=true`, every read query that implements `ICachedQuery` goes through a cache-aside MediatR pipeline:

1. Look up `CacheKey` in Redis — serve on hit.
2. On miss, execute the handler, store the JSON payload under the key, and file the key in one Redis SET per cache tag.
3. Every write command that implements `IInvalidatingCommand` lists the tags it dirties; after `SaveChangesAsync` succeeds the `InvalidationBehavior` reads each tag set, deletes all member keys, and drops the tag set itself.

TTLs are a safety net, not the primary freshness mechanism:

| Scope | Default TTL |
|---|---|
| Database / table lists | 1 hour |
| Table lookups, schemas, column stats | 15 minutes |
| Partition metadata, Iceberg table metadata | 5 minutes |
| Filter-based partition queries (unbounded key cardinality) | 1 minute |

Tag taxonomy (`src/Hmsnet.Core/Caching/CacheTags.cs`): `db:list`, `db:{name}`, `tables:{db}`, `table:{db}:{t}`, `partitions:{db}:{t}`, `stats:{db}:{t}`, `iceberg:{db}:{t}`.

Flip `Redis:Enabled=false` and the app transparently falls back to a no-op cache — no code changes required.

---

## Usage

### Connecting from Hive, Spark, Trino

Point `hive.metastore.uris` at the Thrift port:

```properties
hive.metastore.uris=thrift://hmsnet-host:9083
```

For Spark:

```properties
spark.hadoop.hive.metastore.uris=thrift://hmsnet-host:9083
spark.sql.catalogImplementation=hive
```

The `docker/hive-site.xml` shows a working configuration used by the bundled HiveServer2 container.

### REST API

```bash
# list databases
curl http://localhost:5281/api/databases

# create a database
curl -X POST http://localhost:5281/api/databases \
  -H 'Content-Type: application/json' \
  -d '{"name":"sales","description":"Sales warehouse"}'

# get a specific database
curl http://localhost:5281/api/databases/sales

# list tables in a database
curl http://localhost:5281/api/databases/sales/tables

# get a specific table's schema
curl http://localhost:5281/api/databases/sales/tables/orders

# get column statistics
curl "http://localhost:5281/api/databases/sales/tables/orders/stats?columns=amount,region"

# list partitions
curl http://localhost:5281/api/databases/sales/tables/orders/partitions

# drop a database (cascade deletes its tables)
curl -X DELETE "http://localhost:5281/api/databases/sales?cascade=true"
```

Interactive docs are at `http://localhost:5281/scalar/v1` in Development mode.

### Iceberg REST Catalog

Standard [Apache Iceberg REST Catalog](https://iceberg.apache.org/docs/latest/iceberg-rest/) layout:

```bash
# list namespaces
curl http://localhost:<port>/v1/namespaces

# create a namespace
curl -X POST http://localhost:<port>/v1/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace":["analytics"],"properties":{"owner":"data-team"}}'

# list Iceberg tables in a namespace
curl http://localhost:<port>/v1/namespaces/analytics/tables

# load a table's latest metadata
curl http://localhost:<port>/v1/namespaces/analytics/tables/events
```

Pointing pyiceberg at it:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "hmsnet",
    **{
        "uri": "http://hmsnet-host:<port>",
        "type": "rest",
    },
)
print(catalog.list_namespaces())
```

---

## Development

```bash
# restore + build the solution
dotnet build

# run the full test suite (handlers, Thrift, caching, Iceberg)
dotnet test

# run a single test file
dotnet test --filter "FullyQualifiedName~DatabaseHandlerTests"

# start the API with hot reload
dotnet watch --project src/Hmsnet.Api
```

### Layout

```
hms.net/
├── Hmsnet.slnx                     # solution file (.NET 10 slnx format)
├── Dockerfile                      # multi-stage build for Hmsnet.Api
├── docker/                         # local dev stack (compose + configs)
├── src/
│   ├── Hmsnet.Api/                 # REST controllers + Thrift server + composition root
│   ├── Hmsnet.Iceberg/             # Iceberg REST catalog host
│   ├── Hmsnet.Core/                # Domain, MediatR contracts, caching abstractions
│   └── Hmsnet.Infrastructure/      # EF Core, services, Redis cache, migrations
└── tests/
    ├── Hmsnet.Tests/
    └── Hmsnet.Iceberg.Tests/
```

### CI

`.github/workflows/ci.yml` runs on every push / PR to `main`:

1. Restore, build (`Release`), `dotnet test` with TRX + `XPlat Code Coverage`.
2. On push to `main`, build and push an image to `ghcr.io/aturoczy/hms.net` tagged with the commit SHA, branch name, and `latest`.

Artifact layer cache is backed by `type=gha` so rebuilds stay fast.

---

## Observability

Every request generates an OpenTelemetry trace span. With the dev compose stack running:

- **Traces** land in Jaeger at http://localhost:16686 (service names: `Hmsnet`, `Hmsnet.Iceberg`)
- **Metrics** are scraped by Prometheus (ASP.NET Core + HttpClient + .NET runtime meters)
- **Dashboards** in Grafana at http://localhost:3000

The exporter endpoint is configured via `OpenTelemetry:Endpoint` (OTLP gRPC, default `http://localhost:4317`). Point it at any OTLP-compatible backend — Jaeger, Tempo, Honeycomb, Datadog, etc.

---

## Roadmap / non-goals

Supported today: Hive 3/4 table metadata, single-level namespaces, Iceberg V1 + V2 metadata JSON, basic column statistics.

Not yet implemented: Kerberos/SASL auth on the Thrift port, multi-level Iceberg namespaces, ACID/transactional tables, Ranger-style authorization hooks. Contributions welcome — see the issues list.

---

## Contributing

1. Fork and create a feature branch (`feat/short-name` or `fix/…`).
2. `dotnet test` must pass locally before you open a PR.
3. Keep commits focused and well-described; no force-push on `main`.
4. Open a PR against `main` — CI runs build + tests automatically.

---

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).
