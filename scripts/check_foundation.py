"""Read-only Phase 2 checks against the running local Compose deployment."""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
WORKERS = {f"worker{i}" for i in range(1, 5)}


def docker(*args):
    """Run a bounded Docker inspection command from the repository directory."""
    result = subprocess.run(["docker", *args], cwd=ROOT, check=True,
                            text=True, capture_output=True, timeout=45)
    return result.stdout


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    docker("compose", "config", "--quiet")
    ids = docker("compose", "ps", "--all", "--quiet").split()
    require(len(ids) == 6, "Expected six Compose containers; start with docker compose up -d --wait")
    containers = json.loads(docker("inspect", *ids))
    services = {c["Config"]["Labels"]["com.docker.compose.service"]: c for c in containers}
    require(set(services) == WORKERS | {"controller", "postgres"}, "Unexpected service topology")
    for name, container in services.items():
        state = container["State"]
        require(state["Running"] and state.get("Health", {}).get("Status") == "healthy",
                f"{name} is not running and healthy")
    print("PASS: Compose configuration and all six service health checks", flush=True)

    destinations = {**{name: "/data" for name in WORKERS},
                    "postgres": "/var/lib/postgresql/data", "controller": "/tmp"}
    volume_names = []
    for name, destination in destinations.items():
        mounts = [m for m in services[name]["Mounts"] if m["Destination"] == destination]
        require(len(mounts) == 1 and mounts[0]["Type"] == "volume" and mounts[0]["RW"],
                f"{name} needs a writable named volume at {destination}")
        volume_names.append(mounts[0]["Name"])
    require(len(set(volume_names)) == 6, "Services must not share their data/spool volumes")
    print("PASS: four isolated worker volumes, persistent PostgreSQL and upload spool", flush=True)

    # Execute inside the controller to also exercise Compose DNS and worker connectivity.
    check = '''
import json, time, urllib.request
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from object_store.database import make_database
from object_store.models import Base

def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)

def require(condition, message):
    if not condition:
        raise RuntimeError(message)

engine, _ = make_database()
with engine.connect() as connection:
    require(connection.dialect.name == "postgresql", "Deployment must use PostgreSQL")
    context = MigrationContext.configure(connection)
    head = ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
    require(context.get_current_revision() == head, "Database migration is not at head")
    require(not compare_metadata(context, Base.metadata), "Database schema differs from models")
engine.dispose()
print("PASS: PostgreSQL connection, migration head and schema consistency", flush=True)

get("http://localhost:8000/health")
first = {w["worker_id"]: w for w in get("http://localhost:8000/api/v1/status")["workers"]}
expected = {f"worker{i}" for i in range(1, 5)}
require(set(first) == expected, "All four workers must be registered")
for worker in sorted(expected):
    health = get(f"http://{worker}:8000/health")
    require(health["worker_id"] == worker and health["status"] == "ok", "Worker identity mismatch")
# Default heartbeats arrive every two seconds; allow up to ten seconds for progress.
deadline = time.monotonic() + 10
while True:
    second = {w["worker_id"]: w for w in get("http://localhost:8000/api/v1/status")["workers"]}
    if set(second) == expected and all(w["available"] and w["last_seen"] != first[name]["last_seen"] for name, w in second.items()):
        break
    require(time.monotonic() < deadline, "Worker heartbeats did not advance")
    time.sleep(1)
print("PASS: worker DNS, identities, registration and advancing heartbeats", flush=True)
'''
    print(docker("compose", "exec", "-T", "controller", "python", "-c", check), end="")
    print("Phase 2 foundation checks passed. No stored objects or containers were changed.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError, ValueError, KeyError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError):
            print(exc.stderr, file=sys.stderr)
        sys.exit(1)
