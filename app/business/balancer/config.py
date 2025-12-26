from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


BALANCER_CONFIG_PATH = Path("/Users/dmitrijnazdrin/inventory_service_1/app/business/balancer/config.yaml")


@dataclass(frozen=True)
class BalancerConfig:
    """Сырым держим dict, в следующих шагах добавим строгую валидацию/модели."""
    raw: Dict[str, Any]

    @property
    def balancer(self) -> Dict[str, Any]:
        return self.raw.get("balancer", {})

    @property
    def profiles(self) -> list[Dict[str, Any]]:
        return self.balancer.get("profiles", [])


def load_config(path: Path = BALANCER_CONFIG_PATH) -> BalancerConfig:
    if not path.exists():
        raise FileNotFoundError(f"Balancer config not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return BalancerConfig(raw=data)