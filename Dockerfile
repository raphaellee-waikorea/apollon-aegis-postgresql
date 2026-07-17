# =============================================================================
# apollon-aegis-collector / postgresql
#
# Standalone PostgreSQL 16 image with the pgvector extension pre-installed.
# This lets the collector project store both relational data and vector
# embeddings (matching the vector/graph data conventions used by the
# apollon-aegis stack) without needing a separately compiled extension.
#
# Build:
#   docker build -t apollon-aegis-collector-postgresql:16 .
#
# Run standalone (no compose):
#   docker run -d --name apollon-aegis-collector-postgresql \
#     -e POSTGRES_USER=apollon \
#     -e POSTGRES_PASSWORD=20tlwkr26! \
#     -e POSTGRES_DB=apollon \
#     -p 31110:5432 \
#     -v "$(pwd)/data:/var/lib/postgresql/data" \
#     apollon-aegis-collector-postgresql:16
#
# Preferred: use the accompanying docker-compose.yml in this folder instead.
# =============================================================================

FROM pgvector/pgvector:pg16

LABEL maintainer="apollon-aegis-collector"
LABEL description="Standalone PostgreSQL 16 + pgvector for apollon-aegis-collector"

# Match server timezone conventions used across the apollon-aegis stack.
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Any *.sql / *.sh dropped in init/ runs once, in filename order, the first
# time the data directory is initialized (i.e. when /var/lib/postgresql/data
# is empty). It will NOT re-run against an existing data volume.
COPY init/ /docker-entrypoint-initdb.d/

EXPOSE 5432
