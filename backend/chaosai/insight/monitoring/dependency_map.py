"""Static OpenMeter dependency map used as Analyzer context.

Source: openmeter/quickstart/docker-compose.yaml (`depends_on`, ports) and quickstart/config.yaml
(sink dedupe via redis). The data-flow edges follow the documented ingestion path
API -> Kafka -> sink-worker -> ClickHouse. If the Planner teammate maintains a richer map, pass theirs to
`build_diagnostic_evidence(dependency_map=...)` instead.
"""
from __future__ import annotations

from ..schemas.analysis import DependencyEdge

_E = DependencyEdge

OPENMETER_DEPENDENCY_MAP: list[DependencyEdge] = [
    _E(source="client", target="openmeter-api", relation="sends_events_to"),
    _E(source="openmeter-api", target="kafka", relation="produces_to"),
    _E(source="sink-worker", target="kafka", relation="consumes_from"),
    _E(source="sink-worker", target="clickhouse", relation="writes_to"),
    _E(source="sink-worker", target="redis", relation="dedupes_via"),
    _E(source="openmeter-api", target="clickhouse", relation="queries_meters_from"),
    _E(source="openmeter-api", target="postgres", relation="depends_on"),
    _E(source="balance-worker", target="kafka", relation="depends_on"),
    _E(source="balance-worker", target="clickhouse", relation="depends_on"),
    _E(source="balance-worker", target="redis", relation="depends_on"),
    _E(source="billing-worker", target="kafka", relation="depends_on"),
    _E(source="billing-worker", target="clickhouse", relation="depends_on"),
    _E(source="notification-service", target="kafka", relation="depends_on"),
    _E(source="notification-service", target="clickhouse", relation="depends_on"),
]

KNOWN_SERVICES: list[str] = sorted(
    {e.source for e in OPENMETER_DEPENDENCY_MAP if e.source != "client"} | {e.target for e in OPENMETER_DEPENDENCY_MAP}
    | {"svix", "openmeter-jobs"}
)
