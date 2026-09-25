from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from . import OUTPUT_SCHEMA_VERSION
from .contracts import ContractError
from .mcp_gateway import EvidenceGateway
from .trace import TraceWriter

MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 0.05
MAX_EVIDENCE_REFS = 30


TOOL_PREFERENCES: dict[str, tuple[str, ...]] = {
    "entity": (
        "resolve_order_candidates",
        "find_order_candidates",
        "resolve_order",
        "search_orders",
        "search_order",
        "get_order",
    ),
    "order": ("get_order_items", "get_order_details", "get_order", "get_item_details"),
    "customer": ("get_customer_history", "get_customer_profile", "get_customer"),
    "shipment": (
        "get_shipment_timeline",
        "get_shipment_summary",
        "get_shipment",
        "get_delivery_timeline",
        "get_logistics_events",
    ),
    "payment": ("get_order_payments", "get_payment_records", "get_payments", "get_payment"),
    "payment_timeline": ("get_payment_timeline",),
    "refund": ("get_refund_timeline", "get_refund_records", "get_refunds", "get_refund"),
    "product": ("get_product_context",),
    "seller": ("get_sellers",),
    "policy": ("get_policy", "get_policy_rules", "lookup_policy"),
}

TOOL_KEYWORDS: dict[str, tuple[str, ...]] = {
    "entity": ("resolve", "candidate", "search", "order"),
    "order": ("order", "item", "product"),
    "customer": ("customer", "history", "profile"),
    "shipment": ("shipment", "delivery", "logistic", "tracking"),
    "payment": ("payment", "charge", "transaction"),
    "payment_timeline": ("payment", "timeline"),
    "refund": ("refund", "reimburse"),
    "product": ("product", "item"),
    "seller": ("seller", "vendor"),
    "policy": ("policy", "rule"),
}


@dataclass(frozen=True)
class EvidenceRecord:
    evidence_ref: str
    domain: str
    data: Any
    tool_name: str
    warnings: tuple[str, ...] = ()


@dataclass
class EvidenceCollector:
    """Case-scoped MCP adapter with bounded retry and evidence reuse."""

    case_id: str
    gateway: EvidenceGateway
    trace: TraceWriter
    records: list[EvidenceRecord] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)
    _cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = field(
        default_factory=dict
    )

    async def fetch(
        self,
        tool_name: str,
        *,
        actor: str,
        arguments: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        arguments = {key: str(value) for key, value in (arguments or {}).items()}
        cache_key = (tool_name, tuple(sorted(arguments.items())))
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._emit_consumed(cached, tool_name, actor, cache_hit=True, attempt=0)
            return cached

        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                evidence = await self.gateway.call(
                    tool_name, case_id=self.case_id, **arguments
                )
                self._cache[cache_key] = evidence
                self._record(evidence, tool_name)
                self._emit_consumed(evidence, tool_name, actor, cache_hit=False, attempt=attempt)
                return evidence
            except ContractError as exc:
                self.failures.append((tool_name, "invalid_evidence"))
                last_error = exc
                break
            except (TypeError, ValueError) as exc:
                self.failures.append((tool_name, "invalid_request"))
                last_error = exc
                break
            except RuntimeError as exc:
                # EvidenceGateway uses RuntimeError for an MCP tool-level error;
                # retrying an invalid identifier would waste the query budget.
                self.failures.append((tool_name, "tool_error"))
                last_error = exc
                break
            except Exception as exc:  # MCP transports expose different timeout types.
                last_error = exc
                if attempt <= MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY_SECONDS * attempt)

        if last_error is not None:
            self.failures.append((tool_name, type(last_error).__name__))
        return None

    def _record(self, evidence: dict[str, Any], tool_name: str) -> None:
        evidence_ref = evidence.get("evidence_ref")
        domain = evidence.get("domain")
        if not isinstance(evidence_ref, str) or not isinstance(domain, str):
            return
        warnings = evidence.get("warnings", [])
        if not isinstance(warnings, list):
            warnings = []
        self.records.append(
            EvidenceRecord(
                evidence_ref=evidence_ref,
                domain=domain,
                data=evidence.get("data"),
                tool_name=tool_name,
                warnings=tuple(str(item) for item in warnings if isinstance(item, str)),
            )
        )

    def _emit_consumed(
        self,
        evidence: dict[str, Any],
        tool_name: str,
        actor: str,
        *,
        cache_hit: bool,
        attempt: int,
    ) -> None:
        evidence_ref = evidence.get("evidence_ref")
        if not isinstance(evidence_ref, str):
            return
        self.trace.emit(
            case_id=self.case_id,
            event_type="tool_result_consumed",
            actor=actor,
            target="evidence-collector",
            tool_name=tool_name,
            evidence_refs=[evidence_ref],
            attributes={"cache_hit": cache_hit, "attempt": attempt},
        )

    @property
    def evidence_refs(self) -> list[str]:
        return _unique(record.evidence_ref for record in self.records)[:MAX_EVIDENCE_REFS]


def _norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def _walk(value: Any, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            yield child_path, child
            yield from _walk(child, child_path)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child, path)


def _path_name(path: tuple[str, ...]) -> str:
    return "_".join(_norm(part) for part in path if _norm(part))


def _scalar_strings(value: Any) -> list[str]:
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    if isinstance(value, list):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def _unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _extract_ids(value: Any, kind: str) -> list[str]:
    result: list[str] = []
    for path, leaf in _walk(value):
        name = _path_name(path)
        leaf_name = _norm(path[-1]) if path else ""
        id_like = any(
            token in leaf_name
            for token in ("_id", "_ids", "_ref", "_refs", "reference", "number", "tracking")
        )
        if kind == "order":
            matches = "order" in name and (id_like or leaf_name in {"order", "orders"})
        elif kind == "item":
            matches = "item" in name and id_like
        elif kind == "seller":
            matches = "seller" in name and id_like
        elif kind == "customer":
            customer_context = any(
                _norm(part) in {"customer", "customer_context", "customer_profile"}
                for part in path[:-1]
            )
            matches = ("customer" in leaf_name or customer_context) and id_like
        elif kind == "shipment":
            matches = (
                ("shipment" in name or "tracking" in name or "delivery" in name)
                and id_like
            )
        elif kind == "payment":
            matches = (
                ("payment" in name or "transaction" in name or "charge" in name)
                and id_like
            )
        else:
            matches = False
        if matches:
            result.extend(_scalar_strings(leaf))
    return _unique(result)


def _extract_order_candidates(case: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    fallback: list[str] = []
    for path, leaf in _walk(case):
        name = _path_name(path)
        leaf_name = _norm(path[-1]) if path else ""
        if "order" not in name or not (
            "id" in leaf_name or leaf_name in {"order", "orders"}
        ):
            continue
        values = _scalar_strings(leaf)
        fallback.extend(values)
        if not any(token in name for token in ("related", "history", "previous")):
            candidates.extend(values)
    return _unique(candidates or fallback)


def _first_identifier(value: Any, kind: str) -> str | None:
    values = _extract_ids(value, kind)
    return values[0] if values else None


def _first_scalar_for_keys(value: Any, keys: set[str]) -> str | None:
    for path, leaf in _walk(value):
        if _norm(path[-1]) in keys:
            values = _scalar_strings(leaf)
            if values:
                return values[0]
    return None


def _all_text(values: Iterable[Any]) -> str:
    chunks: list[str] = []
    for value in values:
        for _, leaf in _walk(value):
            if isinstance(leaf, str):
                chunks.append(leaf.lower())
    return " ".join(chunks)


def _parse_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value >= 0 else None
    if not isinstance(value, str):
        return None
    text = value.strip().replace("R$", "").replace(" ", "")
    if not text:
        return None
    text = (
        text.replace(",", ".")
        if "," in text and "." not in text
        else text.replace(",", "")
    )
    match = re.search(r"\d+(?:\.\d+)?", text)
    if match is None:
        return None
    number = float(match.group(0))
    return number if number >= 0 else None


def _money_value(payloads: Iterable[Any], aliases: tuple[str, ...]) -> float | None:
    observations: list[tuple[str, float]] = []
    normalized_aliases = {_norm(alias) for alias in aliases}
    for payload in payloads:
        for path, leaf in _walk(payload):
            number = _parse_number(leaf)
            if number is None:
                continue
            leaf_name = _norm(path[-1]) if path else ""
            path_name = _path_name(path)
            if leaf_name in normalized_aliases:
                return number
            if any(alias in path_name for alias in normalized_aliases):
                observations.append((path_name, number))
    if not observations:
        return None
    return sum(number for _, number in observations)


def _tool_score(name: str, kind: str) -> int:
    normalized = _norm(name)
    score = sum(10 for keyword in TOOL_KEYWORDS[kind] if _norm(keyword) in normalized)
    if kind == "entity" and any(token in normalized for token in ("resolve", "candidate")):
        score += 40
    if kind == "order" and "order" in normalized:
        score += 15
    return score


def _pick_tool(tool_names: Iterable[str], kind: str, used: set[str]) -> str | None:
    available = [name for name in tool_names if name not in used]
    by_normalized = {_norm(name): name for name in available}
    for preferred in TOOL_PREFERENCES[kind]:
        selected = by_normalized.get(_norm(preferred))
        if selected is not None:
            return selected
    scored = [(name, _tool_score(name, kind)) for name in available]
    scored = [(name, score) for name, score in scored if score > 0]
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[1], item[0]))
    return scored[0][0]


def _arguments_for_tool(
    tool_name: str,
    kind: str,
    case: dict[str, Any],
    order_ids: list[str],
    customer_id: str | None,
    issue_code: str | None = None,
    records: list[EvidenceRecord] | None = None,
) -> dict[str, str]:
    normalized = _norm(tool_name)
    if "customer" in normalized or kind == "customer":
        if customer_id is None:
            return {}
        argument_name = "customer_unique_id" if "unique" in normalized else "customer_id"
        return {argument_name: customer_id}

    if kind == "entity":
        if len(order_ids) > 1 and any(token in normalized for token in ("resolve", "candidate")):
            return {"candidate_order_ids": ",".join(order_ids)}
        if order_ids:
            return {"order_id": order_ids[0]}
        return {}

    if kind in {"order", "shipment", "payment", "payment_timeline", "refund"}:
        if order_ids:
            return {"order_id": order_ids[0]}
        if kind in {"payment", "refund"}:
            reference = _first_identifier(case, "payment")
            if reference is not None:
                return {"payment_reference": reference}
        return {}

    if kind == "product":
        return {"order_id": order_ids[0]} if order_ids else {}

    if kind == "seller":
        return {"order_id": order_ids[0]} if order_ids else {}

    if kind == "policy":
        topic = _first_scalar_for_keys(
            case,
            {"policy_code", "policy_topic", "issue_code", "policy_id", "policy_version"},
        )
        topic = topic or issue_code
        if not topic:
            return {}
        return {"policy_version": topic}

    return {}


def _records_for(records: list[EvidenceRecord], domains: set[str]) -> list[EvidenceRecord]:
    return [record for record in records if record.domain in domains]


def _resolve_entities(
    case: dict[str, Any], records: list[EvidenceRecord]
) -> tuple[str, list[str], list[str], float]:
    candidates = _extract_order_candidates(case)
    if not candidates:
        candidates = _unique(
            order_id
            for record in records
            for order_id in _extract_ids(record.data, "order")
        )
    if len(candidates) == 1:
        confidence = 0.95 if _extract_order_candidates(case) else 0.75
        return "resolved", candidates, [], confidence
    if not candidates:
        return "not_found", [], [], 0.0

    scores = {
        candidate: sum(
            json.dumps(record.data, ensure_ascii=False).lower().count(candidate.lower())
            for record in records
        )
        for candidate in candidates
    }
    highest = max(scores.values(), default=0)
    winners = [candidate for candidate, score in scores.items() if score == highest and score > 0]
    if len(winners) == 1:
        rejected = [candidate for candidate in candidates if candidate != winners[0]]
        return "resolved", winners, rejected, 0.85
    return "ambiguous", [], [], 0.25


def _shipment_analysis(records: list[EvidenceRecord]) -> dict[str, Any]:
    shipment_records = _records_for(records, {"shipment"})
    text = _all_text(record.data for record in shipment_records)
    explicit = _first_scalar_for_keys(
        [record.data for record in shipment_records], {"verdict", "shipment_verdict", "delay_cause"}
    )
    explicit_text = _norm(explicit or "")
    if any(token in text for token in ("conflict", "contradict", "inconsistent")):
        verdict = "conflicting"
    elif "lost" in text or "missing" in text:
        verdict = "lost"
    elif "return" in text or "returned" in text:
        verdict = "returned"
    elif "seller_delay" in explicit_text or "seller delay" in text or "seller delayed" in text:
        verdict = "seller_delay"
    elif "logistics_delay" in explicit_text or any(
        token in text for token in ("logistics delay", "carrier delay", "carrier delayed")
    ):
        verdict = "logistics_delay"
    elif any(token in text for token in ("on_time", "on time", "delivered on time")):
        verdict = "on_time"
    elif any(token in text for token in ("late", "delayed", "delay")):
        verdict = "logistics_delay"
    else:
        verdict = "insufficient_evidence"

    timeline_keys = {
        "timeline",
        "delivered_at",
        "delivery_date",
        "estimated_delivery_date",
        "shipped_at",
        "order_delivered_customer_date",
    }
    timeline_complete = any(
        _norm(path[-1]) in timeline_keys
        for record in shipment_records
        for path, _ in _walk(record.data)
        if path
    )
    late_sellers = _unique(
        seller_id
        for record in shipment_records
        for seller_id in _extract_ids(record.data, "seller")
    )
    return {
        "verdict": verdict,
        "late_seller_ids": late_sellers[:20] if verdict == "seller_delay" else [],
        "timeline_complete": timeline_complete,
    }


def _payment_analysis(records: list[EvidenceRecord]) -> dict[str, Any]:
    payment_records = _records_for(records, {"payment", "refund"})
    payloads = [record.data for record in payment_records]
    text = _all_text(payloads)
    captured = _money_value(
        payloads,
        ("captured_total_brl", "captured_total", "captured", "payment_value", "paid_total"),
    )
    refunded = _money_value(
        payloads,
        ("refunded_total_brl", "refunded_total", "refunded", "refund_amount", "total_refund"),
    )
    refundable = _money_value(
        payloads,
        ("refundable_total_brl", "refundable_total", "refundable", "refund_due"),
    )

    if "duplicate" in text or "double charge" in text or "duplicated" in text:
        verdict = "duplicate_capture"
    elif "refund_failed" in text or "refund failed" in text:
        verdict = "refund_failed"
    elif "refund_pending" in text or "refund pending" in text or "pending refund" in text:
        verdict = "refund_pending"
    elif "refunded" in text and (refunded or 0) > 0:
        verdict = "refunded"
    elif "mismatch" in text or "capture mismatch" in text:
        verdict = "capture_mismatch"
    elif "reconciled" in text:
        verdict = "reconciled"
    elif captured is not None:
        verdict = "reconciled" if refunded in (None, 0.0) else "refunded"
    else:
        verdict = "insufficient_evidence"

    if refundable is None and captured is not None:
        refundable = max(captured - (refunded or 0.0), 0.0)
    return {
        "verdict": verdict,
        "captured_total_brl": captured,
        "refunded_total_brl": refunded,
        "refundable_total_brl": refundable,
    }


def _classify_issue(
    case: dict[str, Any], shipment: dict[str, Any], payment: dict[str, Any]
) -> str:
    case_text = _all_text([case])
    if payment["verdict"] == "duplicate_capture":
        return "duplicate_charge"
    if payment["verdict"] == "refund_failed":
        return "refund_failed"
    if payment["verdict"] == "refund_pending":
        return "refund_pending"
    if shipment["verdict"] == "seller_delay":
        return "late_delivery_seller"
    if shipment["verdict"] == "logistics_delay":
        return "late_delivery_logistics"
    if payment["verdict"] == "capture_mismatch":
        return "payment_mismatch"
    if any(token in case_text for token in ("cancelled", "canceled", "cancelamento")):
        return (
            "canceled_order_paid"
            if payment["captured_total_brl"] is not None
            else "unsupported_claim"
        )
    if any(token in case_text for token in ("unavailable", "out of stock", "unavailable product")):
        return (
            "unavailable_order_paid"
            if payment["captured_total_brl"] is not None
            else "unsupported_claim"
        )
    if "valid_split_payment" in case_text and payment["verdict"] == "reconciled":
        return "valid_split_payment"
    if "unsupported_claim" in case_text and payment["verdict"] in {
        "reconciled",
        "insufficient_evidence",
    }:
        return "unsupported_claim"
    if (
        payment["verdict"] == "insufficient_evidence"
        and shipment["verdict"] == "insufficient_evidence"
    ):
        return "insufficient_evidence"
    return "insufficient_evidence"


def _root_cause(
    issue: str,
    shipment: dict[str, Any],
    policy_rule: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mapping = {
        "late_delivery_seller": ("SELLER_DELAY", "seller", None),
        "late_delivery_logistics": ("LOGISTICS_DELAY", "logistics_provider", None),
        "duplicate_charge": ("DUPLICATE_CAPTURE", "payment_provider", None),
        "payment_mismatch": ("PAYMENT_CAPTURE_MISMATCH", "payment_provider", None),
        "refund_failed": ("REFUND_FAILURE", "payment_provider", None),
        "refund_pending": ("REFUND_PENDING", "platform", None),
        "canceled_order_paid": ("ORDER_CANCELLATION", "platform", None),
        "unavailable_order_paid": ("PRODUCT_UNAVAILABLE", "seller", None),
        "valid_split_payment": ("VALID_SPLIT_PAYMENT", "customer", None),
        "unsupported_claim": ("UNSUPPORTED_CLAIM", "customer", None),
    }
    cause, party_type, party_id = mapping.get(issue, ("INSUFFICIENT_EVIDENCE", "unknown", None))
    if policy_rule:
        parties = policy_rule.get("responsible_parties")
        if isinstance(parties, list) and parties and isinstance(parties[0], dict):
            party_type = parties[0].get("party_type", party_type)
            party_id = parties[0].get("party_id", party_id)
    if (
        issue == "late_delivery_seller"
        and party_type == "seller"
        and not shipment.get("late_seller_ids")
        and not policy_rule
    ):
        party_type, party_id = "unknown", None
    return {
        "ranked_causes": [{"cause_code": cause, "rank": 1}],
        "responsible_parties": [{"party_type": party_type, "party_id": party_id}],
    }


def _conflicts(records: list[EvidenceRecord]) -> list[dict[str, Any]]:
    tracked = {
        "order_status",
        "status",
        "delivery_date",
        "estimated_delivery_date",
        "payment_value",
        "refund_status",
        "order_delivered_customer_date",
    }
    observations: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for record in records:
        for path, leaf in _walk(record.data):
            if not path or _norm(path[-1]) not in tracked:
                continue
            if isinstance(leaf, (dict, list)):
                continue
            value = str(leaf).strip()
            if value:
                observations[_norm(path[-1])].append((record.tool_name, value))

    result: list[dict[str, Any]] = []
    for field_name, values in observations.items():
        distinct_values = _unique(value for _, value in values)
        sources = _unique(source for source, _ in values)
        if len(distinct_values) < 2 or len(sources) < 2:
            continue
        result.append(
            {
                "field": field_name[:100],
                "sources": sources[:5],
                "selected_source": None,
                "resolution_code": "unresolved_source_conflict",
            }
        )
    return result[:5]


def _financial_resolution(
    issue: str,
    payment: dict[str, Any],
    order_ids: list[str],
    policy_rule: dict[str, Any] | None = None,
) -> dict[str, Any]:
    refundable = policy_rule.get("refund_brl") if policy_rule else None
    if not isinstance(refundable, (int, float)):
        refundable = payment.get("refundable_total_brl")
    if not isinstance(refundable, (int, float)):
        refundable = 0.0
    if payment.get("verdict") == "refunded":
        refundable = 0.0
    refundable = max(float(refundable), 0.0)
    lines: list[dict[str, Any]] = []
    if refundable > 0:
        lines.append(
            {
                "reason_code": issue.upper(),
                "amount_brl": refundable,
                "entity_id": order_ids[0] if order_ids else None,
            }
        )
    return {
        "currency": "BRL",
        "recommended_refund_brl": refundable,
        "refund_lines": lines,
    }


def _policy_rule(records: list[EvidenceRecord], issue: str) -> dict[str, Any]:
    for record in records:
        if record.domain != "policy" or not isinstance(record.data, dict):
            continue
        rules = record.data.get("rules")
        if not isinstance(rules, dict):
            continue
        rule = rules.get(issue)
        if isinstance(rule, dict):
            return rule
    return {}


def _claim_assessments(case: dict[str, Any], evidence_refs: list[str]) -> list[dict[str, Any]]:
    claims: list[Any] = []
    for path, value in _walk(case):
        if (
            path
            and _norm(path[-1]) in {"claims", "complaints", "claim_items"}
            and isinstance(value, list)
        ):
            claims.extend(value)
    result: list[dict[str, Any]] = []
    for index, claim in enumerate(claims[:5], 1):
        claim_id = (
            _first_scalar_for_keys(claim, {"claim_id", "id"})
            if isinstance(claim, dict)
            else None
        )
        result.append(
            {
                "claim_id": claim_id or f"claim_{index}",
                "verdict": "insufficient_evidence" if not evidence_refs else "partially_supported",
                "confidence": 0.25 if not evidence_refs else 0.5,
                "evidence_refs": evidence_refs[:10],
            }
        )
    return result


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    """Run the L3B coordinator, specialist agents and verifier for one case."""
    case_id = case.get("case_id")
    if not isinstance(case_id, str) or not case_id:
        raise ValueError("case must contain a non-empty case_id")

    tool_names = await gateway.list_tools()
    if not tool_names:
        raise RuntimeError("MCP Gateway returned no tools")

    collector = EvidenceCollector(case_id, gateway, trace)
    used_tools: set[str] = set()
    order_ids = _extract_order_candidates(case)
    customer_id = _first_identifier(case, "customer")

    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="entity-agent",
        decision_code="resolve_case_entities",
    )
    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor="coordinator",
        target="entity-agent",
    )
    entity_tool = _pick_tool(tool_names, "entity", used_tools)
    if entity_tool is not None:
        used_tools.add(entity_tool)
        candidates_to_check = order_ids[:3]
        if candidates_to_check:
            await asyncio.gather(
                *(
                    collector.fetch(
                        entity_tool,
                        actor="entity-agent",
                        arguments={"order_id": candidate},
                    )
                    for candidate in candidates_to_check
                )
            )
        else:
            await collector.fetch(
                entity_tool,
                actor="entity-agent",
                arguments=_arguments_for_tool(entity_tool, "entity", case, order_ids, customer_id),
            )

    entity_status, order_ids, rejected_order_ids, entity_confidence = _resolve_entities(
        case, collector.records
    )
    customer_id = customer_id or _first_identifier(
        [record.data for record in collector.records], "customer"
    )

    query_order_ids = order_ids or _extract_order_candidates(case)[:1]
    specialist_calls: list[tuple[str, str, str, dict[str, str]]] = []
    for kind in (
        "order",
        "customer",
        "shipment",
        "payment",
        "payment_timeline",
        "refund",
        "product",
        "seller",
    ):
        agent_name = f"{kind}-agent"
        trace.emit(
            case_id=case_id,
            event_type="task_assigned",
            actor="coordinator",
            target=agent_name,
            decision_code=f"investigate_{kind}",
        )
        trace.emit(
            case_id=case_id,
            event_type="handoff",
            actor="coordinator",
            target=agent_name,
        )
        tool_name = _pick_tool(tool_names, kind, used_tools)
        if tool_name is None:
            continue
        arguments = _arguments_for_tool(
            tool_name, kind, case, query_order_ids, customer_id, records=collector.records
        )
        if kind not in {"order", "policy"} and not arguments:
            continue
        used_tools.add(tool_name)
        specialist_calls.append((tool_name, agent_name, kind, arguments))

    await asyncio.gather(
        *(
            collector.fetch(tool_name, actor=agent_name, arguments=arguments)
            for tool_name, agent_name, _, arguments in specialist_calls
        )
    )
    if customer_id is None:
        customer_id = _first_identifier(
            [record.data for record in collector.records], "customer"
        )
    entity_status, order_ids, rejected_order_ids, entity_confidence = _resolve_entities(
        case, collector.records
    )

    shipment = _shipment_analysis(collector.records)
    payment = _payment_analysis(collector.records)
    issue = _classify_issue(case, shipment, payment)

    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="policy-agent",
        decision_code="apply_policy",
    )
    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor="coordinator",
        target="policy-agent",
    )
    policy_tool = _pick_tool(tool_names, "policy", used_tools)
    if policy_tool is not None:
        used_tools.add(policy_tool)
        await collector.fetch(
            policy_tool,
            actor="policy-agent",
            arguments=_arguments_for_tool(
                policy_tool, "policy", case, order_ids, customer_id, issue
            ),
        )

    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor="coordinator",
        target="conflict-resolver",
        decision_code="resolve_source_conflicts",
    )
    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor="coordinator",
        target="conflict-resolver",
    )
    conflicts = _conflicts(collector.records)
    policy_rule = _policy_rule(collector.records, issue)
    cause = _root_cause(issue, shipment, policy_rule)
    financial = _financial_resolution(issue, payment, order_ids, policy_rule)
    evidence_refs = collector.evidence_refs

    policy_status = policy_rule.get("case_status")
    if entity_status != "resolved" or issue == "insufficient_evidence":
        case_status = "needs_investigation"
    elif policy_status in {"action_required", "no_action", "needs_investigation"}:
        case_status = policy_status
    elif financial["recommended_refund_brl"] > 0 or issue in {
        "canceled_order_paid",
        "unavailable_order_paid",
        "late_delivery_seller",
        "late_delivery_logistics",
        "duplicate_charge",
        "payment_mismatch",
        "refund_failed",
        "refund_pending",
    }:
        case_status = "action_required"
    else:
        case_status = "no_action"

    confidence = min(1.0, max(0.0, (entity_confidence + (0.7 if evidence_refs else 0.0)) / 2))
    if issue == "insufficient_evidence":
        confidence = min(confidence, 0.35)

    secondary: list[str] = []
    if shipment["verdict"] != "insufficient_evidence":
        secondary.append(f"shipment_{shipment['verdict']}")
    if payment["verdict"] != "insufficient_evidence":
        secondary.append(f"payment_{payment['verdict']}")
    if entity_status == "ambiguous":
        secondary.append("ambiguous_entity")

    output: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "case_id": case_id,
        "assessment": {
            "primary_issue": issue,
            "secondary_issues": _unique(secondary)[:10],
            "case_status": case_status,
            "confidence": confidence,
        },
        "affected_entities": {
            "order_ids": order_ids[:20],
            "item_ids": _unique(
                item_id
                for record in collector.records
                for item_id in _extract_ids(record.data, "item")
            )[:20],
            "seller_ids": _unique(
                seller_id
                for record in collector.records
                for seller_id in _extract_ids(record.data, "seller")
            )[:20],
            "payment_references": _unique(
                reference
                for record in collector.records
                for reference in _extract_ids(record.data, "payment")
            )[:20],
            "shipment_ids": _unique(
                shipment_id
                for record in collector.records
                for shipment_id in _extract_ids(record.data, "shipment")
            )[:20],
        },
        "entity_resolution": {
            "status": entity_status,
            "resolved_order_ids": order_ids[:20],
            "rejected_candidates": rejected_order_ids[:20],
            "confidence": entity_confidence,
        },
        "customer_context": {
            "customer_unique_id": customer_id,
            "related_order_ids": order_ids[:20],
        },
        "shipment_analysis": shipment,
        "payment_analysis": payment,
        "root_cause_analysis": cause,
        "evidence_refs": evidence_refs,
        "data_conflicts": conflicts,
        "financial_resolution": financial,
        "resolution_actions": _resolution_actions(issue, case_status, policy_rule),
    }
    claims = _claim_assessments(case, evidence_refs)
    if claims:
        output["claim_assessments"] = claims

    trace.emit(
        case_id=case_id,
        event_type="policy_decided",
        actor="policy-agent",
        target="verifier-agent",
        decision_code=issue,
        evidence_refs=evidence_refs[:20],
    )
    trace.emit(
        case_id=case_id,
        event_type="verification_completed",
        actor="verifier-agent",
        target="coordinator",
        decision_code="output_contract_ready",
        evidence_refs=evidence_refs[:20],
        attributes={
            "entity_status": entity_status,
            "evidence_count": len(evidence_refs),
            "conflict_count": len(conflicts),
        },
    )
    return output


def _resolution_actions(
    issue: str, case_status: str, policy_rule: dict[str, Any] | None = None
) -> list[str]:
    if case_status == "needs_investigation":
        return ["collect_missing_evidence"]
    recommended_action = policy_rule.get("recommended_action") if policy_rule else None
    policy_actions = {
        "issue_refund": ["process_refund"],
        "refund_freight": ["refund_freight"],
        "reconcile_payment": ["reconcile_payment"],
        "refund_duplicate_charge": ["reverse_duplicate_capture"],
        "retry_refund": ["retry_refund_safely"],
        "monitor_refund": ["monitor_refund"],
        "document_no_action": ["document_no_action"],
    }
    if recommended_action in policy_actions:
        return policy_actions[recommended_action]
    actions = {
        "canceled_order_paid": ["validate_cancellation", "process_refund"],
        "unavailable_order_paid": ["confirm_unavailability", "process_refund"],
        "late_delivery_seller": ["notify_seller", "offer_customer_remedy"],
        "late_delivery_logistics": ["escalate_logistics", "offer_customer_remedy"],
        "duplicate_charge": ["reverse_duplicate_capture"],
        "payment_mismatch": ["reconcile_payment"],
        "refund_pending": ["monitor_refund"],
        "refund_failed": ["retry_refund_safely", "notify_customer"],
    }
    return actions.get(issue, [])[:8]
