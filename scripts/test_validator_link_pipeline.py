"""End-to-end test of the validator-link rendering pipeline.

For every directory entry whose name contains characters outside the safe
[A-Za-z0-9 ._-] set, build a synthetic impact bullet, run the API matcher
to produce the link_map the frontend would receive, then drive the actual
wrapValidatorLinks() JS function (extracted verbatim from
/var/www/monadpulse/governance-mip.html) via Node.js to produce the final
HTML, and assert the name is wrapped in an <a> tag.

Pass:   exits 0, prints PASS table.
Fail:   exits 1, prints FAIL table with first un-wrapped name.

Use as a deployment gate after touching api/routes/governance.py
(_load_validator_directory_for_link_map, validator_link_map regex) or
governance-mip.html (wrapValidatorLinks).
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

# ─── 1. Mirror of the API matcher ──────────────────────────────────────
# Copied from api/routes/governance.py:372 — keep these in sync.

def api_build_link_map(directory: dict[str, dict], bullets: list[str]) -> dict[str, dict]:
    bullet_blob = " ".join(bullets)
    bullet_blob_lower = bullet_blob.lower()
    out: dict[str, dict] = {}
    for name_low, meta in directory.items():
        if len(name_low) < 4 or name_low.isdigit():
            continue
        pattern = r"(^|[^\w/])" + re.escape(name_low) + r"(?=$|[^\w/])"
        if re.search(pattern, bullet_blob_lower):
            out[meta["name"]] = {
                "val_id": meta["val_id"],
                "url": f"/validator.html?id={meta['val_id']}",
            }
    return out


def load_directory() -> dict[str, dict]:
    """Load directory + override for the network the governance pipeline
    is currently grounded against. Must stay in sync with
    collector/governance_llm.py:CONTEXT_NETWORK."""
    network = "mainnet"  # mirror of CONTEXT_NETWORK
    out: dict[str, dict] = {}
    for fname in (
        f"/opt/monadpulse/validator_directory_{network}.json",
        f"/opt/monadpulse/validator_directory_override_{network}.json",
    ):
        path = Path(fname)
        if not path.exists():
            continue
        for r in json.loads(path.read_text()):
            nm = r.get("name")
            vid = r.get("val_id")
            if nm and isinstance(vid, int):
                out[nm.lower()] = {"val_id": vid, "name": nm}
    return out


# ─── 2. Drive the actual JS linker via Node ────────────────────────────
JS_DRIVER = r"""
'use strict';
// Read JSON {linkMap, bullet} from stdin to avoid argv-quoting issues with
// names that contain emojis, brackets, or pipes.
let raw = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => { raw += chunk; });
process.stdin.on('end', () => {
  const {linkMap, bullet} = JSON.parse(raw);

  const esc = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const linkPairs = Object.entries(linkMap).sort((a,b)=>b[0].length-a[0].length);

  const wrapValidatorLinks = (htmlSafe) => {
    if (!linkPairs.length) return htmlSafe;
    const alternation = linkPairs
      .map(([name]) => esc(name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
      .join('|');
    const nameByLower = new Map(linkPairs.map(([n, m]) => [n.toLowerCase(), m]));
    const pattern = new RegExp(
      '(^|[^\\w/])(' + alternation + ')(?=$|[^\\w/])',
      'gi'
    );
    const parts = htmlSafe.split(/(<[^>]+>)/g);
    return parts.map(part => {
      if (part.startsWith('<')) return part;
      return part.replace(pattern, (full, pre, hit) => {
        const meta = nameByLower.get(hit.toLowerCase());
        if (!meta) return full;
        return pre + '<a href="' + meta.url + '" class="validator-link">' + hit + '</a>';
      });
    }).join('');
  };

  process.stdout.write(wrapValidatorLinks(esc(bullet)));
});
"""


def js_render(link_map: dict, bullet: str) -> str:
    payload = json.dumps({"linkMap": link_map, "bullet": bullet})
    p = subprocess.run(
        ["node", "-e", JS_DRIVER],
        input=payload, capture_output=True, text=True, check=False,
    )
    if p.returncode != 0:
        raise RuntimeError(f"node driver failed: {p.stderr}")
    return p.stdout


# ─── 3. Edge-case selection ────────────────────────────────────────────

_REGULAR = re.compile(r"^[A-Za-z0-9 \-._]*$")

def special_char_names(directory: dict[str, dict]) -> list[dict]:
    out = []
    for nm_low, meta in directory.items():
        nm = meta["name"]
        if not _REGULAR.match(nm):
            out.append(meta)
    return sorted(out, key=lambda m: m["name"])


# ─── 4. Run the test ───────────────────────────────────────────────────

def run() -> int:
    directory = load_directory()
    edge = special_char_names(directory)

    print(f"Loaded {len(directory)} directory entries; {len(edge)} edge cases.")
    print()
    print(f"{'name':45s} {'val_id':>7s}  api_match  js_wrap   verdict")
    print("-" * 90)

    failures: list[str] = []
    for meta in edge:
        nm = meta["name"]
        # Build a bullet that mentions this name in a realistic context.
        bullet = f"Validators like {nm} (rank 100, 11M MON staked) would be affected."
        link_map = api_build_link_map(directory, [bullet])
        api_hit = nm in link_map

        rendered = js_render(link_map, bullet) if api_hit else None
        js_hit = bool(rendered) and (
            f'<a href="/validator.html?id={meta["val_id"]}" class="validator-link">' in rendered
            and "</a>" in rendered
            and (
                # The visible text inside the anchor must equal the name (or
                # its HTML-escaped form for any < > & inside).
                f">{nm}</a>" in rendered
                or f">{nm.replace('&', '&amp;')}</a>" in rendered
            )
        )

        verdict = "PASS" if (api_hit and js_hit) else "FAIL"
        print(f"{nm:45s} {meta['val_id']:>7d}  {str(api_hit):9s} {str(js_hit):9s} {verdict}")
        if verdict == "FAIL":
            failures.append(f"{nm}: api={api_hit} js={js_hit} html={rendered!r}")

    print()
    if failures:
        print("FAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"All {len(edge)} edge cases passed.")
    return 0


if __name__ == "__main__":
    sys.exit(run())
