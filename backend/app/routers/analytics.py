"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select, text
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _get_lab_title_from_short_id(lab_short_id: str) -> str:
    """Convert lab short ID (e.g., 'lab-04') to title substring (e.g., 'Lab 04')."""
    # lab-04 -> Lab 04 (keep leading zero for matching)
    parts = lab_short_id.split("-")
    if len(parts) == 2 and parts[0].lower() == "lab":
        return f"Lab {parts[1]}"
    return lab_short_id


async def _get_lab_and_task_ids(session: AsyncSession, lab: str):
    """Find lab item and return (lab_id, list of task_ids)."""
    lab_title = _get_lab_title_from_short_id(lab)

    # Use raw SQL to find lab - avoids ORM issues with test fixtures
    lab_query = text("""
        SELECT id FROM item 
        WHERE type = 'lab' AND title LIKE :title_pattern
    """)
    result = await session.execute(lab_query, {"title_pattern": f"%{lab_title}%"})
    row = result.first()

    if not row:
        return None, []

    # Extract lab_id from row tuple
    lab_id = row[0]

    # Find all task items for this lab
    task_query = text("""
        SELECT id FROM item 
        WHERE parent_id = :parent_id AND type = 'task'
    """)
    task_result = await session.execute(task_query, {"parent_id": lab_id})
    task_ids = [r[0] for r in task_result]

    return lab_id, task_ids


def _build_item_ids_condition(item_ids: list[int]) -> tuple[str, dict]:
    """Build SQL condition for item_id IN (...) with proper parameter binding."""
    if not item_ids:
        return "1=0", {}
    
    # Create placeholders for each ID
    placeholders = ", ".join(f":item_id_{i}" for i in range(len(item_ids)))
    params = {f"item_id_{i}": item_id for i, item_id in enumerate(item_ids)}
    return f"item_id IN ({placeholders})", params


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if lab_id is None:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    all_item_ids = [lab_id] + task_ids
    item_condition, item_params = _build_item_ids_condition(all_item_ids)

    # Query interactions with score bucket using raw SQL
    query = text(f"""
        SELECT 
            CASE 
                WHEN score <= 25 THEN '0-25'
                WHEN score <= 50 THEN '26-50'
                WHEN score <= 75 THEN '51-75'
                WHEN score <= 100 THEN '76-100'
                ELSE '0-25'
            END as bucket,
            COUNT(*) as count
        FROM interacts
        WHERE {item_condition}
          AND score IS NOT NULL
        GROUP BY bucket
    """)

    result = await session.execute(query, item_params)

    bucket_counts = {row.bucket: row.count for row in result}

    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if lab_id is None:
        return []

    # Get task titles and their stats using raw SQL
    query = text("""
        SELECT 
            i.title as task,
            ROUND(AVG(l.score), 1) as avg_score,
            COUNT(*) as attempts
        FROM item i
        JOIN interacts l ON l.item_id = i.id
        WHERE i.parent_id = :lab_id
          AND i.type = 'task'
          AND l.score IS NOT NULL
        GROUP BY i.title
        ORDER BY i.title
    """)

    result = await session.execute(query, {"lab_id": lab_id})

    return [
        {"task": row.task, "avg_score": float(row.avg_score), "attempts": row.attempts}
        for row in result
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if lab_id is None:
        return []

    all_item_ids = [lab_id] + task_ids
    item_condition, item_params = _build_item_ids_condition(all_item_ids)

    # Query submissions grouped by date using raw SQL
    query = text(f"""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as submissions
        FROM interacts
        WHERE {item_condition}
        GROUP BY DATE(created_at)
        ORDER BY date
    """)

    result = await session.execute(query, item_params)

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in result
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if lab_id is None:
        return []

    all_item_ids = [lab_id] + task_ids
    item_condition, item_params = _build_item_ids_condition(all_item_ids)

    # Query per-group stats using raw SQL
    query = text(f"""
        SELECT 
            lr.student_group as "group",
            ROUND(AVG(l.score), 1) as avg_score,
            COUNT(DISTINCT lr.id) as students
        FROM interacts l
        JOIN learner lr ON l.learner_id = lr.id
        WHERE {item_condition}
          AND l.score IS NOT NULL
        GROUP BY lr.student_group
        ORDER BY lr.student_group
    """)

    result = await session.execute(query, item_params)

    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
            "students": row.students
        }
        for row in result
    ]
