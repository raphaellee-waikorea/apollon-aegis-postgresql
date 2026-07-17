# apollon-aegis-collector — Standalone PostgreSQL

`apollon-aegis-collector` 프로젝트에서 사용할 PostgreSQL 16(+pgvector) 단독 실행 환경입니다.
`/opt/apollon-aegis`의 기존 docker-compose 스택과는 완전히 독립적으로 동작하며(별도 컨테이너 이름,
별도 네트워크, 별도 호스트 포트, 별도 데이터 디렉터리), 이 프로젝트 폴더 하위에서만 관리됩니다.

## 구성

```
postgresql/
├── Dockerfile              # postgres:16 + pgvector 확장을 포함한 커스텀 이미지
├── docker-compose.yml       # 단독 실행용 compose 파일
├── .env.example             # 계정/DB 이름 환경변수 예시 (.env로 복사해서 사용)
├── .gitignore
├── init/
│   └── 01-init-extensions.sql   # 최초 초기화 시 1회 실행 (CREATE EXTENSION vector)
└── data/                    # PostgreSQL 데이터 디렉터리 (bind mount, git에는 미포함)
```

## 기존 스택과의 차이점

| 항목 | /opt/apollon-aegis (기존) | apollon-aegis-collector (이 프로젝트) |
|---|---|---|
| 컨테이너 이름 | `apollon-aegis-postgresql` | `apollon-aegis-collector-postgresql` |
| 호스트 포트 | `31010` → `5432` | `31110` → `5432` |
| 네트워크 | `apollon-aegis-network` | `apollon-aegis-collector-network` |
| 이미지 | `postgres:16` (공식 이미지) | 커스텀 빌드: `pgvector/pgvector:pg16` 기반 |
| 계정/DB | `apollon` / `apollon` | 동일 (`apollon` / `apollon`) — 필요 시 `.env`에서 변경 가능 |

계정과 비밀번호(`apollon` / `20tlwkr26!`)는 기존 스택과 동일하게 유지했습니다. 필요하면
`.env`에서 값만 바꾸면 됩니다.

## 사용 방법

```bash
cd postgresql
cp .env.example .env      # 필요 시 계정/비밀번호 수정
docker compose up -d --build
docker compose logs -f postgresql
```

접속 확인:

```bash
docker exec -it apollon-aegis-collector-postgresql \
  psql -U apollon -d apollon -c "\dx"     # vector 확장이 목록에 보이면 정상
```

호스트 로컬에서 접속 (포트는 `127.0.0.1`에만 바인딩되어 있어 외부 네트워크에서는 직접 접속할 수
없습니다 — 원격 서버에 배포된 경우 아래 "원격 서버 배포" 섹션의 SSH 터널을 사용하세요):

```bash
psql -h 127.0.0.1 -p 31110 -U apollon -d apollon
```

같은 docker network(`apollon-aegis-collector-network`)에 속한 다른 컨테이너(예: 나중에 추가할
FastAPI/수집기 서비스)에서는 호스트명 `postgresql`, 포트 `5432`로 접속하면 됩니다. 예:

```
postgresql+psycopg2://apollon:20tlwkr26!@postgresql:5432/apollon
```

## 데이터 초기화 스크립트 (`init/`)

`init/` 아래의 `*.sql` / `*.sh` 파일은 데이터 디렉터리(`data/`)가 **비어있는 상태로 최초
기동될 때 단 한 번만** 실행됩니다. 이미 초기화된 볼륨에는 다시 적용되지 않으므로, 스키마를
바꾸려면 별도 마이그레이션(Flyway/Alembic 등)을 사용하거나 `data/`를 비우고 재기동해야 합니다.

- `docker build` 시 `Dockerfile`이 `init/`을 이미지 안에 복사해두므로, compose 없이
  `docker run`으로 단독 기동해도 초기화 스크립트가 그대로 적용됩니다.
- `docker-compose.yml`은 추가로 `./init`을 컨테이너에 read-only로 마운트해두어, 스크립트를
  수정한 뒤 이미지를 다시 빌드하지 않고도 최초 초기화 시점에는 최신 파일이 반영됩니다
  (단, 위와 마찬가지로 이미 초기화된 볼륨에는 재적용되지 않습니다).

## 데이터 보존 / 초기화

- 데이터는 `postgresql/data/`에 바인드 마운트되어 컨테이너를 내리거나 재빌드해도 유지됩니다.
- 완전히 초기 상태로 되돌리려면:

```bash
docker compose down
rm -rf data/*
docker compose up -d --build
```

## pgvector

이미지가 `pgvector/pgvector:pg16`(공식 postgres:16 이미지에 pgvector 확장을 추가로 빌드해 넣은
드롭인 대체 이미지) 기반이라 별도 컴파일 없이 바로 `CREATE EXTENSION vector;`를 사용할 수
있습니다. `init/01-init-extensions.sql`에서 최초 기동 시 자동으로 활성화됩니다.

## 원격 서버 배포 (`deploy/`)

`deploy/` 폴더에는 이 Docker 환경을 원격 서버의 `/opt/apollon-postgresql`에 배포하고, 서비스를
기동한 뒤 정상 동작까지 확인하는 스크립트가 들어 있습니다.

```
deploy/
├── deploy.env.example    # 서버 접속 정보 템플릿
├── deploy.env             # 실제 접속 정보 (gitignore 처리됨, 커밋되지 않음)
├── deploy_remote.sh        # 배포 + 기동 + 검증을 한 번에 수행
└── verify_remote.sh        # 이미 배포된 서비스의 상태만 재확인 (재배포 없음)
```

**중요:** 이 스크립트들은 대상 서버로 실제 네트워크(SSH)가 연결되는 머신에서 실행해야 합니다.
샌드박스/오프라인 환경에서는 실행할 수 없습니다 — 본인 컴퓨터(맥/서버) 터미널에서 실행하세요.

```bash
cd postgresql/deploy
./deploy_remote.sh
```

`deploy_remote.sh`가 수행하는 작업:

1. SSH 접속 테스트
2. 원격 서버에 `/opt/apollon-postgresql` (및 `init/`, `data/`) 생성
3. `Dockerfile`, `docker-compose.yml`, `.env.example`, `init/`을 rsync/scp로 복사
4. `docker compose up -d --build`로 이미지 빌드 및 기동
5. 검증: `docker compose ps`, 헬스체크 상태(healthy) 대기, `pg_isready`, `pg_extension`으로
   vector 확장 확인, 호스트 포트(31110) listen 여부, 최근 로그 20줄 출력

이후 재배포 없이 상태만 다시 확인하려면:

```bash
./verify_remote.sh
```

서버가 `127.0.0.1`에만 포트를 바인딩하므로, 본인 컴퓨터에서 직접 psql로 접속하려면 SSH 터널을
사용하세요:

```bash
ssh -L 31110:127.0.0.1:31110 daigeunlee@220.74.81.41
# 다른 터미널에서:
psql -h 127.0.0.1 -p 31110 -U apollon -d apollon
```

`deploy.env`에는 서버 접속 정보(비밀번호 포함)가 평문으로 저장되므로, 장기적으로는 SSH 키 기반
인증으로 전환하는 것을 권장합니다.
