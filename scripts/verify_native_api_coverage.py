#!/usr/bin/env python3
"""Read-only source/coverage audit, run on 192.168.3.88 with the frozen official inputs."""
import hashlib
import json
import pathlib
import re
import sys
import os

research = pathlib.Path(sys.argv[1])
manifest = json.loads(pathlib.Path("connector/specs/manifest.json").read_text())
methods = {"get", "post", "put", "delete", "patch", "head"}


def routes(document):
    return {(method.upper(), path) for path, item in document.get("paths", {}).items()
            for method in item if method in methods}


def component_fields(document):
    result = {}
    def walk(schema, pointer):
        if not isinstance(schema, dict):
            return
        properties = schema.get("properties")
        if isinstance(properties, dict):
            result[pointer] = set(properties)
            for name, child in properties.items():
                walk(child, pointer + "/properties/" + name)
        for key in ["items", "additionalProperties", "not"]:
            walk(schema.get(key), pointer + "/" + key)
        for key in ["allOf", "oneOf", "anyOf"]:
            for index, child in enumerate(schema.get(key, [])):
                walk(child, pointer + "/" + key + "/" + str(index))
    for name, schema in document.get("components", {}).get("schemas", {}).items():
        walk(schema, name)
    return result


html = (research / "backpack.html").read_bytes()
assert hashlib.sha256(html).hexdigest() == manifest["backpack_html_sha256"]
spec = json.JSONDecoder().raw_decode(html.decode().split("const __redoc_state = ", 1)[1])[0]["spec"]["data"]
backpack = routes(spec)
bp_catalog = json.loads(pathlib.Path("connector/specs/backpack.json").read_text())["documents"]
assert component_fields(spec) == component_fields(bp_catalog["official"])
sdk = (research / "lighter-openapi.json").read_bytes()
assert hashlib.sha256(sdk).hexdigest() == manifest["lighter_sdk_sha256"]
lighter = routes(json.loads(sdk))
lr_catalog = json.loads(pathlib.Path("connector/specs/lighter.json").read_text())["documents"]
assert component_fields(json.loads(sdk)) == component_fields(lr_catalog["sdk"])
for source in manifest["documents"]:
    file = research / ("lighter-" + source["url"].split("/api/")[1].replace("/", "_"))
    raw = file.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == source["sha256"], source["url"]
    blocks = re.findall(r"```json\s*(.*?)\s*```", raw.decode(), re.S)
    document = json.loads(blocks[source["block_index"]])
    assert "openapi" in document
    lighter |= routes(document)
    assert component_fields(document) == component_fields(lr_catalog[source["catalog_key"]]), source["url"]
for venue, expected in [("backpack", backpack), ("lighter", lighter)]:
    entries = [op for op in manifest["operations"] if op["venue"] == venue]
    actual = {(op["method"], op["path"]) for op in entries}
    assert actual == expected, (venue, expected - actual, actual - expected)
    assert len(entries) == len(actual), "duplicate native routes"
    print(json.dumps({"venue": venue, "official_operations": len(expected), "bound_operations": len(actual), "missing": []}))

signer = json.loads(pathlib.Path("connector/specs/lighter-signer.json").read_text())
header = pathlib.Path(os.environ["TITAN_LIGHTER_SIGNER_TEST_PATH"]).with_suffix(".h").read_bytes()
assert hashlib.sha256(header).hexdigest() == signer["header_sha256"]
official = set(re.findall(r"extern SignedTxResponse (Sign\w+)\(", header.decode()))
bound = {operation["name"] for operation in signer["operations"]}
rust = pathlib.Path("connector/src/lighter/signer_native_generated.rs").read_text()
symbols = set(re.findall(r'b"(Sign\w+)\\0"', rust))
assert official == bound == symbols and len(bound) == 23
print(json.dumps({"venue": "lighter-signer", "official_operations": len(official), "bound_operations": len(bound), "missing": []}))
