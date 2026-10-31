#!/usr/bin/env python3
"""Generate ABI bindings only on the build server, from its freshly generated official C header."""
import argparse
import hashlib
import json
import pathlib
import re


def snake(name):
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()
    return "r#" + name if name in {"type", "ref", "self", "match", "async", "await"} else name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("header", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    text = args.header.read_text()
    ctypes = {"int": "i32", "long long int": "i64", "long long unsigned int": "u64",
              "uint8_t": "u8", "int16_t": "i16", "uint32_t": "u32", "int64_t": "i64", "char*": "*const c_char"}
    structs = {}
    for body, name in re.findall(r"typedef struct \{([^}]+)\} (\w+);", text):
        if not name.endswith("TxReq"):
            continue
        fields = []
        for field in body.strip().split(";"):
            if not field.strip():
                continue
            ctype, fname = field.strip().rsplit(" ", 1)
            if ctype not in ctypes:
                raise ValueError("unhandled C field " + ctype)
            fields.append((ctype, fname))
        structs[name] = fields
    functions = []
    for name, declaration in re.findall(r"extern SignedTxResponse (Sign\w+)\(([^;]+)\);", text):
        params = [arg.rsplit(" ", 1) for arg in declaration.split(", ")]
        functions.append((name, params))
    lines = ["// Generated from the server-built official C header. Do not edit."]
    for name, fields in structs.items():
        native = "Native" + name.removesuffix("TxReq")
        lines += ["#[derive(Clone, serde::Serialize, serde::Deserialize)]", "#[serde(deny_unknown_fields)]", f"pub struct {native} {{"]
        lines += [f"pub {snake(fname)}: {'String' if ctype == 'char*' else ctypes[ctype]}," for ctype, fname in fields]
        lines += ["}", "#[repr(C)]", f"struct Ffi{name} {{"]
        lines += [f"{snake(fname)}: {ctypes[ctype]}," for ctype, fname in fields]
        lines += ["}"]
    lines += ["#[derive(Clone, serde::Serialize, serde::Deserialize)]", "#[serde(tag=\"type\",content=\"parameters\",rename_all=\"snake_case\",deny_unknown_fields)]", "pub enum NativeTransaction {"]
    excluded = {"cNonce", "cApiKeyIndex", "cAccountIndex", "cSkipNonce", "cLen"}
    for name, params in functions:
        fields = []
        for ctype, pname in params:
            if pname in excluded:
                continue
            field = snake(pname.removeprefix("c"))
            if ctype.endswith("*") and ctype != "char*":
                base = ctype[:-1]
                typ = "Native" + base.removesuffix("TxReq")
                if pname == "cOrders":
                    typ = f"Vec<{typ}>"
            else:
                typ = "String" if ctype == "char*" else "u32" if pname in ("cPrice", "cTriggerPrice") and ctype == "int" else ctypes[ctype]
            fields.append(f"{field}: {typ}")
        lines.append(name.removeprefix("Sign") + " {" + ",".join(fields) + "},")
    lines += ["}", "impl NativeTransaction {", "pub const OPERATIONS: &'static [&'static str] = &["]
    lines += [json.dumps(name) + "," for name, params in functions]
    lines += ["];", "}", "impl SignerAccount {", "fn native_sign(&self, transaction: NativeTransaction, options: NativeSigningOptions) -> Result<NativeSignedTransaction,ApiError> {", "let response=match transaction {"]
    for name, params in functions:
        fields = [snake(p.removeprefix("c")) for t, p in params if p not in excluded]
        lines.append(f"NativeTransaction::{name.removeprefix('Sign')} {{ {','.join(fields)} }} => {{")
        for ctype, pname in params:
            if ctype == "char*":
                field = snake(pname.removeprefix("c"))
                lines.append(f"let {field}=CString::new({field}).map_err(|_|invalid(NAME,\"native string contains NUL\"))?;")
            elif ctype.endswith("*"):
                base = ctype[:-1]
                field = snake(pname.removeprefix("c"))
                if pname == "cOrders":
                    lines.append("if orders.is_empty() || orders.len()>i32::MAX as usize {return Err(invalid(NAME,\"invalid native order group length\"));}")
                    lines.append(f"let orders: Vec<_>=orders.into_iter().map(|value| Ffi{base} {{" + ",".join(f"{snake(fn)}:value.{snake(fn)}" for ct, fn in structs[base]) + "}).collect();")
                else:
                    for ct, fn in structs[base]:
                        if ct == "char*":
                            f = snake(fn)
                            lines.append(f"let {field}_{f}=CString::new({field}.{f}).map_err(|_|invalid(NAME,\"native string contains NUL\"))?;")
                    lines.append(f"let {field}=Ffi{base} {{" + ",".join(f"{snake(fn)}:{field}_{snake(fn)}.as_ptr()" if ct == "char*" else f"{snake(fn)}:{field}.{snake(fn)}" for ct, fn in structs[base]) + "};")
        signature = []
        arguments = []
        for ctype, pname in params:
            signature.append("*const Ffi" + ctype[:-1] if ctype.endswith("*") and ctype != "char*" else ctypes[ctype])
            field = snake(pname.removeprefix("c"))
            if pname == "cNonce":
                argument = "options.nonce"
            elif pname == "cSkipNonce":
                argument = "u8::from(options.skip_nonce)"
            elif pname == "cApiKeyIndex":
                argument = "i32::from(self.key)"
            elif pname == "cAccountIndex":
                argument = "self.account"
            elif pname == "cLen":
                argument = "orders.len() as i32"
            elif ctype == "char*" or pname == "cOrders":
                argument = field + ".as_ptr()"
            elif ctype.endswith("*"):
                argument = "&" + field
            elif pname in ("cPrice", "cTriggerPrice") and ctype == "int":
                argument = field + " as i32"
            else:
                argument = field
            arguments.append(argument)
        lines += ["// SAFETY: function and repr(C) layouts are generated from this pinned library's C header.",
                  "unsafe {", "let function=*self.functions._library.get::<unsafe extern \"C\" fn(" + ",".join(signature) + ") -> SignedResponse>(b\"" + name + "\\0\").map_err(|_|invalid(NAME,\"missing native signer operation\"))?;",
                  "function(" + ",".join(arguments) + ")", "}", "},"]
    lines += ["};", "self.native_signed_response(response)", "}", "}"]
    args.output.write_text("\n".join(lines) + "\n")
    provenance = {"revision": "aefa96848a105d82a6bbb55d3d05663293a71713",
                  "header_sha256": hashlib.sha256(args.header.read_bytes()).hexdigest(),
                  "operations": [{"name": name, "parameters": params} for name, params in functions],
                  "structures": structs}
    (args.output.parents[2] / "specs/lighter-signer.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({"signing_operations": len(functions), "header_sha256": hashlib.sha256(args.header.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
