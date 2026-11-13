"""Validation findings and reports."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal

from hls_metadata_validation.spec import ProductSpec

Severity = Literal["error", "warning", "info"]
FindingSurface = Literal["cog", "cmr", "stac", "golden", "spec"]


@dataclass(frozen=True)
class Finding:
    severity: Severity
    surface: FindingSurface
    check: str
    granule: str
    target: str
    message: str
    known_gap: str | None = None


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    def extend(self, findings: list[Finding]) -> None:
        self.findings.extend(findings)

    def apply_known_gaps(self, spec: ProductSpec) -> None:
        """Downgrade errors matching a known gap of ``spec`` to warnings."""
        updated = []
        for f in self.findings:
            if f.severity == "error":
                for gap in spec.known_gaps:
                    if gap.matches(f.surface, f.check, f.target):
                        f = replace(f, severity="warning", known_gap=gap.id)
                        break
            updated.append(f)
        self.findings = updated

    @property
    def ok(self) -> bool:
        return not any(f.severity == "error" for f in self.findings)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "findings": [asdict(f) for f in self.findings]}

    def to_text(self) -> str:
        counts = Counter(f.severity for f in self.findings)
        lines = []
        for f in sorted(self.findings, key=lambda f: (f.granule, f.surface, f.target)):
            gap = f" [known gap: {f.known_gap}]" if f.known_gap else ""
            lines.append(
                f"{f.severity.upper():7} {f.granule} {f.surface}:{f.check} "
                f"{f.target}: {f.message}{gap}"
            )
        status = "PASS" if self.ok else "FAIL"
        lines.append(
            f"{status}: {counts['error']} error(s), {counts['warning']} warning(s), "
            f"{counts['info']} info"
        )
        return "\n".join(lines)
