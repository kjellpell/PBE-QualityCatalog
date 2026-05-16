"""
Create importable Power Automate package ZIPs from the flow definitions
in the definitions/ folder.

Usage:
    python create_packages.py

Each definitions/*.json file produces a matching *.zip in the same
power-automate/ folder. Import the ZIP via:
    make.powerautomate.com → My flows → Import → Import Package (Legacy)
"""

from __future__ import annotations

import json
import uuid
import zipfile
from pathlib import Path

CONTENT_TYPES_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">\n'
    '  <Default Extension="json" ContentType="application/json"/>\n'
    '</Types>'
)


def create_package(definition_path: Path, output_dir: Path) -> Path:
    raw = json.loads(definition_path.read_text(encoding="utf-8"))
    display_name = raw.pop("_displayName", definition_path.stem)

    flow_guid = str(uuid.uuid4())

    root_manifest = {
        "schema": "1.0",
        "details": {
            "displayName": display_name,
            "description": "PBE Quality Catalog notification flow",
            "createdTime": "2025-01-01T00:00:00Z",
            "packageTelemetryId": str(uuid.uuid4()),
            "creator": "PBE Quality Catalog",
            "sourceEnvironment": "",
        },
        "resources": {
            flow_guid: {
                "type": "Microsoft.Flow/flows",
                "id": flow_guid,
                "name": flow_guid,
                "details": {"displayName": display_name},
                "actions": {"create": {}},
                "dependencies": {},
            }
        },
    }

    # Full flow object as returned by the Power Automate API — this is what
    # the import endpoint expects inside Microsoft.Flow/flows/{guid}/definition.json
    flow_obj = {
        "name": flow_guid,
        "id": f"/providers/Microsoft.ProcessSimple/environments/Default/flows/{flow_guid}",
        "type": "Microsoft.ProcessSimple/environments/flows",
        "properties": {
            "apiId": "/providers/Microsoft.PowerApps/apis/shared_logicflows",
            "displayName": display_name,
            "state": "Started",
            "definition": raw["definition"],
            "connectionReferences": raw.get("connectionReferences", {}),
            "parameters": raw.get("parameters", {}),
        },
    }

    out_path = output_dir / f"{definition_path.stem}.zip"
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", CONTENT_TYPES_XML)
        zf.writestr(
            "DefinitionManifest.json",
            json.dumps(root_manifest, indent=2, ensure_ascii=False),
        )
        zf.writestr(
            f"Microsoft.Flow/flows/{flow_guid}/definition.json",
            json.dumps(flow_obj, indent=2, ensure_ascii=False),
        )

    print(f"  Created: {out_path.name}  ({out_path.stat().st_size:,} bytes)")
    return out_path


if __name__ == "__main__":
    here = Path(__file__).parent
    definitions_dir = here / "definitions"
    created = []
    for json_file in sorted(definitions_dir.glob("*.json")):
        created.append(create_package(json_file, here))
    print(f"\nDone — {len(created)} package(s) ready to import into Power Automate.")
