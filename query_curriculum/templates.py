from __future__ import annotations

from typing import Any, Callable


TemplatePack = Callable[[list[Any], dict[str, Any]], list[Any]]

TEMPLATE_PACKS: dict[str, TemplatePack] = {}


def register_template_pack(name: str, callback: TemplatePack) -> None:
    TEMPLATE_PACKS[name] = callback


def apply_template_packs(names: list[str], candidates: list[Any], context: dict[str, Any]) -> list[Any]:
    enriched = list(candidates)
    for name in names:
        callback = TEMPLATE_PACKS.get(name)
        if callback is None:
            raise ValueError(f"Unknown template pack: {name}")
        enriched = callback(enriched, context)
    return enriched

