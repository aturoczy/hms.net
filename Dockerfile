# Multi-stage build for Hmsnet.Api.
# Build stage: restores against the full solution so project references
# resolve, then publishes only the API.
# Runtime stage: ASP.NET Core runtime only — smaller attack surface.

ARG DOTNET_VERSION=10.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build
WORKDIR /src

# Copy csproj files first so `dotnet restore` is cacheable when only source changes.
COPY Hmsnet.slnx ./
COPY src/Hmsnet.Api/Hmsnet.Api.csproj                     src/Hmsnet.Api/
COPY src/Hmsnet.Core/Hmsnet.Core.csproj                   src/Hmsnet.Core/
COPY src/Hmsnet.Infrastructure/Hmsnet.Infrastructure.csproj src/Hmsnet.Infrastructure/
COPY src/Hmsnet.Iceberg/Hmsnet.Iceberg.csproj             src/Hmsnet.Iceberg/
COPY tests/Hmsnet.Tests/Hmsnet.Tests.csproj               tests/Hmsnet.Tests/
COPY tests/Hmsnet.Iceberg.Tests/Hmsnet.Iceberg.Tests.csproj tests/Hmsnet.Iceberg.Tests/

RUN dotnet restore src/Hmsnet.Api/Hmsnet.Api.csproj

COPY . .

RUN dotnet publish src/Hmsnet.Api/Hmsnet.Api.csproj \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION} AS runtime
WORKDIR /app

# Run as non-root.
RUN groupadd --system --gid 1000 hmsnet \
 && useradd  --system --uid 1000 --gid hmsnet --shell /usr/sbin/nologin hmsnet \
 && chown -R hmsnet:hmsnet /app
USER hmsnet

COPY --from=build --chown=hmsnet:hmsnet /app/publish .

# HTTP API + Thrift metastore port.
EXPOSE 8080
EXPOSE 9083

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_gcServer=1

ENTRYPOINT ["dotnet", "Hmsnet.Api.dll"]
