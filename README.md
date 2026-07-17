# apollon-aegis-collector — Standalone PostgreSQL

`apollon-aegis-collector` 프로젝트에서 사용할 PostgreSQL 16(+pgvector) 단독 실행 환경입니다.
`/opt/apollon-aegis`의 기존 docker-compose 스택과는 컨테이너 이름·호스트 포트·데이터 디렉터리가
분리되어 있어 같은 호스트에서 동시에 떠 있어도 충돌하지 않지만, 네트워크만큼은 기존 스택이 쓰는
`apollon-aegis-network`에도 함께 붙어서 그쪽의 다른 서비스(fast-api, airflow 등)에서 컨테이너
이름으로 바로 접근할 수 있게 했습니다.

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
| 컨테이너 이름 | `apollon-aegis-postgresql` | `apollon-aegis-postgresql` ⚠️ 기존과 동일 (아래 경고 참고) |
| 호스트 포트 | `31010` → `5432` | `31110` → `5432` |
| 네트워크 | `apollon-aegis-network` | `apollon-aegis-collector-network` **+** `apollon-aegis-network`(공유) |
| 이미지 | `postgres:16` (공식 이미지) | 커스텀 빌드: `pgvector/pgvector:pg16` 기반, 이미지 태그는
`apollon-aegis-collector-postgresql:16`로 구분됨 |
| 계정/DB | `apollon` / `apollon` | 동일 (`apollon` / `apollon`) — 필요 시 `.env`에서 변경 가능 |

> **⚠️ 컨테이너 이름 충돌 주의**
> 이 프로젝트의 컨테이너 이름을 기존 `/opt/apollon-aegis` 스택의 postgres와 동일한
> `apollon-aegis-postgresql`로 맞췄습니다. 같은 이름의 컨테이너는 같은 호스트에 동시에 뜰 수
> 없으므로, 이 스택을 기동하기 전에 기존 `/opt/apollon-aegis`의 postgres 컨테이너를 먼저
> 내리거나(`docker stop apollon-aegis-postgresql && docker rm apollon-aegis-postgresql`)
> 이름이 겹치지 않도록 직접 조정해야 합니다. 그렇지 않으면 `docker compose up`이
> `Conflict. The container name "/apollon-aegis-postgresql" is already in use` 오류로 실패합니다.
> 이미지 태그(`apollon-aegis-collector-postgresql:16`)는 겹치지 않으니 그대로 뒀습니다.

`apollon-aegis-network`는 `docker-compose.yml`에서 `external: true`로 선언되어 있어, 이 compose가
그 네트워크를 만들지는 않고 이미 존재하는 네트워크에 붙기만 합니다(기존 `/opt/apollon-aegis`
스택이 이미 떠 있으면 자동으로 존재). 네트워크가 아직 없는 상태(예: 이 스택을 먼저 띄우는 경우)에
대비해 `deploy_remote.sh` / `deploy_git_clone.sh`가 기동 전에 네트워크를 먼저 생성해두며,
독립적으로 붙이거나 재확인하고 싶을 땐 `deploy/attach_network.sh`를 사용합니다 (아래 참고).

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
docker exec -it apollon-aegis-postgresql \
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
├── deploy.env.example      # 서버 접속 정보 템플릿
├── deploy.env               # 실제 접속 정보 (gitignore 처리됨, 커밋되지 않음)
├── deploy_remote.sh          # 이 머신 → 서버로 rsync/scp 복사 + 네트워크 연결 + 기동 + 검증
├── deploy_git_clone.sh       # 서버가 GitHub에서 직접 git clone/pull + 네트워크 연결 + 기동 + 검증
├── attach_network.sh         # apollon-aegis-network 생성/연결만 별도로 (재배포 없이)
└── verify_remote.sh          # 이미 배포된 서비스의 상태만 재확인 (재배포 없음)
```

**중요:** 이 스크립트들은 대상 서버로 실제 네트워크(SSH)가 연결되는 머신에서 실행해야 합니다.
샌드박스/오프라인 환경에서는 실행할 수 없습니다 — 본인 컴퓨터(맥/서버) 터미널에서 실행하세요.

### 방법 A — 로컬 파일을 rsync로 전송 (`deploy_remote.sh`)

```bash
cd postgresql/deploy
./deploy_remote.sh
```

수행하는 작업:

1. SSH 접속 테스트
2. 원격 서버에 `/opt/apollon-postgresql` (및 `init/`, `data/`) 생성
3. 공유 네트워크 `apollon-aegis-network`가 없으면 생성 (있으면 그대로 사용)
4. `Dockerfile`, `docker-compose.yml`, `.env.example`, `init/`을 rsync/scp로 복사
5. `docker compose up -d --build`로 이미지 빌드 및 기동 (compose가
   `apollon-aegis-network`에도 자동으로 붙임)
6. 검증: `docker compose ps`, 헬스체크 상태(healthy) 대기, `pg_isready`, `pg_extension`으로
   vector 확장 확인, 호스트 포트(31110) listen 여부, `apollon-aegis-network` 연결 여부,
   최근 로그 20줄 출력

### 방법 B — 서버에서 직접 GitHub git clone (`deploy_git_clone.sh`)

이 저장소의 origin은 이미 `https://github.com/raphaellee-waikorea/apollon-aegis-postgresql.git`로
설정되어 있습니다. 로컬에서 파일을 복사하는 대신, 서버가 이 저장소를
**`/opt/apollon-postgresql/postgresql`**(=`${REMOTE_DIR}/${CLONE_DIR_NAME}`)로 직접 clone하게
하려면:

```bash
cd postgresql/deploy
./deploy_git_clone.sh
```

수행하는 작업:

1. SSH 접속 테스트
2. 원격 서버에 `/opt/apollon-postgresql` 생성
3. 공유 네트워크 `apollon-aegis-network`가 없으면 생성
4. `/opt/apollon-postgresql/postgresql`에 저장소가 없으면 `git clone`, 이미 있으면
   `git fetch` + `git reset --hard origin/main`으로 최신 상태로 갱신
5. `docker compose up -d --build`로 이미지 빌드 및 기동
6. 방법 A와 동일한 검증 절차 + 배포된 커밋 해시 출력

`deploy.env`의 `REPO_URL` / `CLONE_DIR_NAME`(기본값 `postgresql`) / `REPO_BRANCH`로 대상을
조정할 수 있습니다. 저장소가 private이면 `GIT_TOKEN`에 GitHub PAT(fine-grained,
`contents:read` 권한이면 충분)을 넣어야 인증 없이 clone/pull이 됩니다. 이후 GitHub에 새로
push했다면, 로컬 파일을 다시 옮길 필요 없이 서버에서 이 스크립트(또는 `git pull` +
`docker compose up -d --build`)만 다시 실행하면 됩니다.

### apollon-aegis-network 연결만 별도로 (`attach_network.sh`)

일반적인 배포 흐름에서는 위 두 스크립트가 알아서 처리하므로 따로 실행할 필요가 없습니다.
다만 컨테이너가 이미 떠 있는 상태에서 네트워크 연결 상태만 확인하거나, 어떤 이유로 네트워크에서
빠진 컨테이너를 재연결하고 싶을 때 사용합니다:

```bash
cd postgresql/deploy
./attach_network.sh
```

- `apollon-aegis-network`가 없으면 생성하고, 있으면 그대로 둡니다.
- `apollon-aegis-postgresql` 컨테이너가 떠 있는데 그 네트워크에 안 붙어 있으면
  `docker network connect`로 연결합니다. 이미 연결되어 있으면 아무 것도 하지 않습니다.
- 몇 번을 다시 실행해도 안전합니다(멱등적).

### 상태만 재확인

배포 방법과 무관하게, 재배포 없이 상태만 다시 확인하려면:

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
