# Multi-stage build for Hmsnet.Api.
# Build stage: restores the API project (pulls Core + Infrastructure in
# transitively). Test projects are intentionally not copied — they are
# excluded by .dockerignore and not needed to build the runtime image.
# Runtime stage: ASP.NET Core runtime only — smaller attack surface.

ARG DOTNET_VERSION=10.0

FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build
WORKDIR /src

# Copy csproj files first so `dotnet restore` is cacheable when only source changes.
COPY src/Hmsnet.Api/Hmsnet.Api.csproj                     src/Hmsnet.Api/
COPY src/Hmsnet.Core/Hmsnet.Core.csproj                   src/Hmsnet.Core/
COPY src/Hmsnet.Infrastructure/Hmsnet.Infrastructure.csproj src/Hmsnet.Infrastructure/
COPY src/Hmsnet.Iceberg/Hmsnet.Iceberg.csproj             src/Hmsnet.Iceberg/

RUN dotnet restore src/Hmsnet.Api/Hmsnet.Api.csproj

COPY . .

RUN dotnet publish src/Hmsnet.Api/Hmsnet.Api.csproj \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION} AS runtime
WORKDIR /app

COPY --from=build /app/publish .

# HTTP API + Thrift metastore port.
EXPOSE 8080
EXPOSE 9083

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true \
    DOTNET_gcServer=1

# The aspnet image ships a non-root `app` user (UID 1000); just use it.
USER $APP_UID

ENTRYPOINT ["dotnet", "Hmsnet.Api.dll"]
