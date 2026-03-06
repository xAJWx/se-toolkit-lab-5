"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any, TypedDict

import httpx
from sqlalchemy import func
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# Type definitions for API responses
class ItemDict(TypedDict):
    lab: str
    task: str | None
    title: str
    type: str  # "lab" | "task"


class LogDict(TypedDict):
    id: int
    lab: str
    task: str | None
    student_id: str
    group: str
    score: float | None
    passed: int | None
    total: int | None
    submitted_at: str


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[ItemDict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    auth = httpx.BasicAuth(settings.autochecker_email, settings.autochecker_password)
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient(auth=auth) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


async def fetch_logs(since: datetime | None = None) -> list[LogDict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    auth = httpx.BasicAuth(settings.autochecker_email, settings.autochecker_password)
    base_url = f"{settings.autochecker_api_url}/api/logs"
    all_logs: list[LogDict] = []
    current_since = since

    while True:
        params: dict[str, Any] = {"limit": 500}
        if current_since:
            params["since"] = current_since.isoformat()

        async with httpx.AsyncClient(auth=auth) as client:
            response = await client.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

        logs = data.get("logs", [])
        all_logs.extend(logs)

        if not data.get("has_more", False):
            break

        # Update since to the last log's submitted_at for next page
        if logs:
            last_log = logs[-1]
            current_since = datetime.fromisoformat(last_log["submitted_at"])
        else:
            break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[ItemDict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    from app.models.item import ItemRecord
    from sqlmodel import select

    new_count = 0
    lab_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        title = item["title"]
        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab", ItemRecord.title == title
            )
        )
        lab_record = existing.first()

        if not lab_record:
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            new_count += 1

        # Map short lab ID (e.g., "lab-01") to the record
        lab_short_id = item["lab"]
        lab_id_to_record[lab_short_id] = lab_record

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        title = item["title"]
        lab_short_id = item["lab"]
        parent_lab = lab_id_to_record.get(lab_short_id)

        if not parent_lab:
            # Skip task if parent lab not found
            continue

        # Check if task already exists with this title and parent_id
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_record = existing.first()

        if not task_record:
            task_record = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[LogDict], items_catalog: list[ItemDict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner
    from sqlmodel import select

    # Build lookup: (lab_short_id, task_short_id | None) -> title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item["lab"]
        task_short_id = item.get("task")
        title = item["title"]
        item_title_lookup[(lab_short_id, task_short_id)] = title

    new_count = 0

    for log in logs:
        # 1. Find or create Learner
        student_id = log["student_id"]
        learner = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner_record = learner.first()

        if not learner_record:
            learner_record = Learner(
                external_id=student_id, student_group=log.get("group", "")
            )
            session.add(learner_record)
            await session.flush()  # Get the ID

        # 2. Find matching item
        lab_short_id = log["lab"]
        task_short_id = log.get("task")
        item_title = item_title_lookup.get((lab_short_id, task_short_id))

        if not item_title:
            # Skip if no matching item found
            continue

        item_record = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_record.first()

        if not item:
            # Skip if item not in DB
            continue

        # 3. Check if InteractionLog with this external_id already exists
        log_external_id = log["id"]
        existing_interaction = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        )
        if existing_interaction.first():
            # Skip duplicate (idempotent upsert)
            continue

        # 4. Create InteractionLog
        # At this point, learner_record and item are guaranteed to exist
        # and have valid IDs (due to the checks above and flush())
        assert learner_record.id is not None
        assert item.id is not None

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner_record.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    from app.models.interaction import InteractionLog
    from sqlmodel import select

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Get last synced timestamp
    # Query for the most recent record by ordering by id DESC
    last_interaction = await session.exec(
        select(InteractionLog)
        .order_by(InteractionLog.id.desc())  # type: ignore[attr-defined]
        .limit(1)
    )
    last_record = last_interaction.first()
    since = last_record.created_at if last_record else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total count
    total_stmt = select(func.count()).select_from(InteractionLog)
    total = await session.exec(total_stmt)
    total_count = total.one()

    return {"new_records": new_records, "total_records": total_count}
