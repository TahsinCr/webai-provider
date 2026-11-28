"""Resolve image placeholders using provider media records rather than invented URLs."""

import mimetypes
import re
from urllib.parse import urlsplit

from ...resources.response.artifact import FileTargetRule

PLACEHOLDER = re.compile(r"https?://googleusercontent\.com/image_generation_content/[^\s)\]<>]+")
CHIP_PLACEHOLDER = re.compile(r"https?://googleusercontent\.com/immersive_entry_chip/[^\s)\]<>]+")
FILE_TAG_PLACEHOLDER = re.compile(r"\[file-tag:\s*[^\]]+\]")
_GEMINI_IDENTITY_PREFIXES = (
    "gemini-media:",
    "http://googleusercontent.com/image_generation_content/",
    "https://googleusercontent.com/image_generation_content/",
)

GEMINI_FILE_RULE = FileTargetRule(
    parse_target=lambda target, parsed: (
        parsed.scheme == "gemini-media"
        and bool(parsed.path)
        and not parsed.netloc
        and len(target) <= 512
    ),
    matches_identity=lambda identity, _target: identity.startswith(_GEMINI_IDENTITY_PREFIXES),
)


def valid_media_url(url):
    try:
        parsed = urlsplit(url)
    except ValueError:
        return False
    host = parsed.hostname or ""
    return (
        parsed.scheme == "https"
        and not parsed.username
        and not parsed.password
        and (
            host.endswith((".googleusercontent.com", ".googlevideo.com"))
            or host in ("storage.googleapis.com", "contribution.usercontent.google.com")
        )
    )


def _nested_value(data, path, default=None):
    current = data
    for key in path:
        if (
            isinstance(current, list)
            and isinstance(key, int)
            and -len(current) <= key < len(current)
        ):
            current = current[key]
        elif isinstance(current, dict) and isinstance(key, str):
            current = current.get(key, default)
        else:
            return default
        if current is None:
            return default
    return current


def _field(container, index, default=None):
    if not isinstance(container, list) or not container:
        return default
    value = container[index] if index < len(container) else None
    if value is None or value == [] or isinstance(value, dict):
        tail = container[-1]
        value = tail.get(str(index + 1)) if isinstance(tail, dict) else None
    return default if value is None or value == [] or value == {} else value


def candidate_files(candidate):
    rich = _nested_value(candidate, [12])
    if not isinstance(rich, list) or not rich:
        return []
    images = []
    web = _field(rich, 1)
    if isinstance(web, list):
        for index, item in enumerate(web):
            url = _nested_value(item, [0, 0, 0])
            if isinstance(url, str) and valid_media_url(url):
                images.append(
                    {"url": url, "name": f"web-image-{index + 1}", "alias": f"web-image:{index}"}
                )
    generated = _field(rich, 7)
    values = _nested_value(generated, [0]) if generated is not None else None
    if isinstance(values, list):
        for index, item in enumerate(values):
            url = _nested_value(item, [0, 3, 3])
            if isinstance(url, str) and valid_media_url(url):
                images.append(
                    {
                        "url": url,
                        "media_type": _nested_value(item, [0, 3, 11]),
                        "name": _nested_value(item, [0, 3, 2]) or f"image-{index + 1}.png",
                        "alias": _nested_value(item, [1, 0])
                        or f"http://googleusercontent.com/image_generation_content/{index}",
                    }
                )
    video_root = _field(rich, 59)
    media = _field(rich, 86)
    if video_root is not None or media is not None:
        candidate_id = _nested_value(candidate, [0])
        video = _nested_value(video_root, [0, 0, 0, 0, 7, 1]) if video_root is not None else None
        for kind, name, mime, url in (
            ("video", "video.mp4", "video/mp4", video),
            (
                "audio",
                "audio.mp3",
                "audio/mpeg",
                _nested_value(media, [0, 1, 7, 1]) if media is not None else None,
            ),
            (
                "music-video",
                "music-video.mp4",
                "video/mp4",
                _nested_value(media, [1, 1, 7, 1]) if media is not None else None,
            ),
        ):
            if isinstance(url, str) and valid_media_url(url):
                alias = (
                    f"gemini-media:{candidate_id}:{kind}" if isinstance(candidate_id, str) else url
                )
                images.append({"url": url, "name": name, "media_type": mime, "alias": alias})
    return images


def resolve_image_links(text, images):
    if not images:
        return text or ""
    by_alias = {image["alias"]: image for image in images}

    def replace(match):
        image = by_alias.get(match.group())
        if image is None:
            return match.group()
        if match.start() and text[match.start() - 1] == "(":
            return image["url"]
        return f"[Image]({image['url']})"

    return PLACEHOLDER.sub(replace, text or "")


def strip_immersive_chips(text: str) -> str:
    """Remove Gemini immersive_entry_chip and file-tag placeholders from visible text."""
    if not text:
        return ""
    cleaned = CHIP_PLACEHOLDER.sub("", text)
    cleaned = FILE_TAG_PLACEHOLDER.sub("", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def candidate_generated_files(candidate) -> list[dict]:
    """Extract downloadable code-execution generated files from rich[58] (field 59)."""
    rich = _nested_value(candidate, [12])
    raw_groups = _field(rich, 58) if isinstance(rich, list) else None
    if not isinstance(raw_groups, list):
        return []
    entries = (
        raw_groups[0]
        if raw_groups
        and isinstance(raw_groups[0], list)
        and raw_groups[0]
        and isinstance(raw_groups[0][0], list)
        else raw_groups
    )
    found: list[dict] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, list) or len(entry) < 3:
            continue
        tag = entry[0] if isinstance(entry[0], str) else ""
        meta = entry[2]
        if not isinstance(meta, list):
            continue
        filename = _field(meta, 2)
        urls = _field(meta, 7)
        if not isinstance(urls, list):
            continue
        download_url = next((u for u in urls if isinstance(u, str) and valid_media_url(u)), None)
        if not download_url:
            continue
        name = (filename.strip() if isinstance(filename, str) else "") or f"file-{index + 1}"
        mime_type = _field(meta, 11)
        if not isinstance(mime_type, str) or not mime_type:
            mime_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        tag_id = tag.removeprefix("[file-tag:").removesuffix("]").strip()
        found.append(
            {
                "id": tag_id or f"gemini-file-{index}",
                "kind": "file",
                "name": name,
                "media_type": mime_type,
                "url": download_url,
                "alias": tag or download_url,
            }
        )
    return found


def candidate_canvas_artifacts(candidate) -> list[dict]:
    """Extract inline Gemini Canvas / immersive_entry_chip document artifacts."""
    canvas = _field(candidate, 30)
    if not isinstance(canvas, list) or not canvas:
        return []
    rich = _nested_value(candidate, [12])
    chip = _field(rich, 56) if isinstance(rich, list) else None
    entries = canvas if isinstance(canvas[0], list) else [canvas]
    artifacts: list[dict] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, list):
            continue
        artifact_id = _field(entry, 0)
        if not isinstance(artifact_id, str) and isinstance(chip, list):
            artifact_id = _field(chip, 1)
        name = _field(entry, 2) or _field(entry, 9)
        if not isinstance(name, str) and isinstance(chip, list):
            name = _field(chip, 3)
        nested = _field(entry, 13)
        content = _field(entry, 4)
        if not isinstance(content, str) and isinstance(nested, list):
            content = _field(nested, 0)
        if not isinstance(content, str):
            continue
        filename = (name.strip() if isinstance(name, str) else "") or (f"artifact-{index + 1}.txt")
        doc_format = _field(nested, 4) if isinstance(nested, list) else None
        guessed = mimetypes.guess_type(filename)[0]
        media_type = guessed or ("text/markdown" if doc_format == "markdown" else "text/plain")
        artifacts.append(
            {
                "id": artifact_id
                if isinstance(artifact_id, str) and artifact_id
                else f"gemini-canvas-{index}",
                "kind": "document",
                "name": filename,
                "media_type": media_type,
                "content": content,
                "size_bytes": len(content.encode("utf-8")),
            }
        )
    return artifacts
