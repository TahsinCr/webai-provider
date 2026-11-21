"""Native Gemini read contracts; parsing has no session or frontend dependency."""

import re
from itertools import islice

from ...errors import ProviderError
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.types import (
    Content,
    MessageData,
    actions_for,
    target_message,
)
from ...resources.project.types import ProjectData
from ...resources.response.source import sources_from
from .errors import GeminiCompatibilityError, GeminiError
from .media import _field, candidate_files, resolve_image_links

_CITATION_MARKER = re.compile(r"\[cite:\s*([\d,\s]+)\]")
_CITATION_NUMBER = re.compile(r"\d+")


def history_value(value, *path, default=None):
    try:
        for key in path:
            value = value[key]
        return value
    except (IndexError, KeyError, TypeError):
        return default


def timestamp(value):
    if isinstance(value, list) and len(value) == 2 and all(type(x) in (int, float) for x in value):
        return value[0] + value[1] / 1e9
    return None


def candidate_metadata(candidate, activity=None):
    """Extract published citations separately from the candidate's thought summary."""
    groups = _field(history_value(candidate, 12), 43, [])
    citations = {}
    for group in groups if isinstance(groups, list) else ():
        marker = history_value(group, 0, 0)
        entries = history_value(group, 1)
        if not isinstance(marker, str) or not isinstance(entries, list):
            continue
        match = _CITATION_MARKER.search(marker)
        if match is None:
            continue
        for number, entry in zip(_CITATION_NUMBER.findall(match.group(1)), entries, strict=False):
            citation_id = int(number)
            if citation_id in citations:
                continue
            url = history_value(entry, 3, 0, 1)
            if isinstance(url, str):
                citations[citation_id] = {
                    "url": url,
                    "title": history_value(entry, 3, 0, 2),
                }
    _, activity_sources = activity if activity is not None else published_activity(candidate)
    if not citations and not activity_sources:
        return {"sources": ()}
    ordered = [citations[key] for key in sorted(citations)]
    ordered.extend(activity_sources)
    return {"sources": sources_from({"sources": ordered})}


def published_activity(candidate):
    """Return provider-published search steps and sources from Gemini history."""
    activities = history_value(candidate, 37, 1, default=[])
    if not isinstance(activities, list) or not activities:
        return (), ()
    steps, seen_steps, sources = [], set(), []
    for activity in activities[:64]:
        if not isinstance(activity, list):
            continue
        for index in (1, 5):
            label = history_value(activity, index)
            if isinstance(label, str):
                label = " ".join(label.split())
                if label and label not in seen_steps:
                    seen_steps.add(label)
                    steps.append(label[:500])
        results = history_value(activity, 7, default=[])
        if not isinstance(results, list):
            continue
        for result in results[:128]:
            url = history_value(result, 1)
            title = history_value(result, 2)
            if isinstance(url, str):
                sources.append({"url": url, "title": title})
    return tuple(steps), sources_from({"sources": sources}) if sources else ()


def published_thoughts(candidate, steps=None, previous=""):
    """Combine Gemini's public thought summary with visible search activity."""
    if steps is None:
        steps, _ = published_activity(candidate)
    summary = history_value(candidate, 37, 0, 0)
    summary = summary.strip() if isinstance(summary, str) else ""
    if not steps:
        return summary or previous
    parts = [summary] if summary else []
    parts.extend(step for step in steps if step not in parts)
    return "\n".join(parts) or previous


def parse_chat_page(pages, *, offset=0, limit=None):
    rows_by_id = {}
    for page in pages:
        if page == []:
            continue
        rows = history_value(page, 2)
        if not isinstance(rows, list):
            raise GeminiCompatibilityError(
                "The Gemini conversation-list contract changed; synchronization was not completed."
            )
        for row in rows:
            cid = history_value(row, 0)
            if isinstance(cid, str) and cid.startswith("c_"):
                rows_by_id[cid] = row

    selected = islice(rows_by_id.items(), offset, None if limit is None else offset + limit)
    return tuple(
        Conversation(
            cid,
            history_value(row, 1) or "Gemini Chat",
            project=(
                ProjectData(pid, "")
                if isinstance(pid := history_value(row, 7), str) and pid.startswith("notebooks/")
                else None
            ),
            metadata={
                "native": row,
                "updated_at": timestamp(history_value(row, 5)),
                "project_membership_known": len(row) > 7,
            },
        )
        for cid, row in selected
    )


def _history_turns(data):
    turns = history_value(data, 0)
    if not isinstance(turns, list):
        raise GeminiError("Gemini history is temporarily unavailable; saved history was preserved")
    return turns


def _turn_id(turn):
    rid = history_value(turn, 0, 1)
    if not isinstance(rid, str):
        raise GeminiCompatibilityError("Gemini history is missing a response ID")
    return rid


def _turn_messages(turn, rid):
    user = history_value(turn, 2, 0, 0)
    if isinstance(user, str) and user:
        yield MessageData(
            rid + ":user",
            "user",
            (Content("text", user),),
            metadata={"turn_id": rid},
            actions=actions_for("user"),
        )
    candidates = history_value(turn, 3, 0, default=[])
    if not candidates:
        return
    candidate = candidates[0]
    text = history_value(candidate, 1, 0, default="")
    # Published thinking summaries only; never infer hidden reasoning.
    activity = published_activity(candidate)
    steps = activity[0]
    thoughts = published_thoughts(candidate, steps)
    if thoughts:
        yield MessageData(
            rid + ":model:thoughts",
            "assistant",
            (Content("text", thoughts),),
            channel="analysis",
            metadata={"turn_id": rid, "published_activity": bool(steps)},
        )
    images = candidate_files(candidate)
    text = resolve_image_links(text if isinstance(text, str) else "", images)
    if text or images:
        yield MessageData(
            rid + ":model",
            "assistant",
            (Content("text", text),)
            + tuple(
                Content(
                    "resource",
                    resource=Attachment(
                        image["alias"], image["name"], image.get("media_type"), image["url"]
                    ),
                )
                for image in images
            ),
            metadata={
                "native_candidate": candidate,
                "turn_id": rid,
                "candidate_id": history_value(candidate, 0),
                **candidate_metadata(candidate, activity),
                "files": tuple(
                    {
                        "id": image["alias"],
                        "name": image["name"],
                        "remote_url": image["url"],
                        "media_type": image.get("media_type"),
                    }
                    for image in images
                ),
            },
            actions=actions_for("assistant", branch=True),
        )


def action_turn_index(data, message_id: str, role: str, action: str) -> int:
    """Validate one selected turn without building the rest of the conversation."""
    turns = _history_turns(data)
    suffix = ":user" if role == "user" else ":model"
    wanted = (
        message_id.removesuffix(suffix)
        if isinstance(message_id, str) and message_id.endswith(suffix)
        else None
    )
    selected = None
    for index, turn in enumerate(turns):
        rid = _turn_id(turn)
        if rid == wanted:
            if selected is not None:
                raise GeminiCompatibilityError(
                    "Gemini history contains a duplicate target response ID"
                )
            selected = index
    if selected is None:
        raise ProviderError("The selected message is no longer on the active conversation path")
    messages = tuple(_turn_messages(turns[selected], wanted))
    _, target = target_message(messages, message_id, role)
    if action not in target.actions:
        raise ProviderError("This Gemini message action is not available")
    return selected


def parse_chat_history(data, cid):
    turns = _history_turns(data)
    messages = []
    for turn in reversed(turns):
        messages.extend(_turn_messages(turn, _turn_id(turn)))
    return Conversation(
        cid,
        "Gemini Chat",
        tuple(messages),
        metadata={"history_partial": len(turns) >= 1000, "title_unavailable": True},
    )
