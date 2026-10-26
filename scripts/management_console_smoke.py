"""Real offline management integration. Execute only on the build server 192.168.3.88."""
from __future__ import annotations
import argparse, http.cookiejar, json, os, secrets, signal, socket, sqlite3, subprocess, time
from pathlib import Path
from urllib import request, error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "binary", "history", "data", "parameters", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--matrix", action="store_true")
    parser.add_argument("--browser", action="store_true")
    parser.add_argument("--reuse", action="store_true", help="Reuse frozen artifacts in the same private fixture and execute the matrix again")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=args.reuse)
    args.output.chmod(0o700)
    root = args.output.resolve()
    def port():
        with socket.socket() as stream:
            stream.bind(("127.0.0.1", 0)); return stream.getsockname()[1]
    node_port, manager_port = port(), port()
    origin = f"http://127.0.0.1:{manager_port}"
    token, password = secrets.token_hex(32), secrets.token_hex(24)
    env = dict(os.environ, TITAN_CONSOLE_NODE_TOKEN=token,
               TITAN_STRATEGY_SDK=str(args.repo / "python/titan-strategy-sdk"),
               TITAN_STRATEGY_PYTHON="/home/dominolu/dev/titan/.venv/bin/python",
               TITAN_REPORT_PYTHON="/home/dominolu/dev/titan/.venv/bin/python",
               TITAN_REPORTING_PATH=str(args.repo / "python/titan-reporting"))
    hashed = subprocess.check_output([str(args.binary), "console", "password-hash"], input=(password + "\n").encode(), env=env).decode().strip()
    quote = lambda value: json.dumps(str(value))
    config = ["schema_version = 1", "[manager]", f'listen = "127.0.0.1:{manager_port}"', f"public_origin = {quote(origin)}", "secure_cookie = false",
              f"database = {quote(root / 'manager.db')}", f"web_root = {quote(args.repo / 'web/console/dist')}",
              "[manager.nodes.build]", f'endpoint = "http://127.0.0.1:{node_port}"', 'token_env = "TITAN_CONSOLE_NODE_TOKEN"', 'environments = ["backtest"]']
    for name, role in (("operator", "operator"), ("reader", "reader")):
        config += ["[[manager.users]]", f"username = {quote(name)}", f"password_hash = {quote(hashed)}", f"role = {quote(role)}", 'nodes = ["build"]', 'accounts = []', 'environments = ["backtest"]']
    config += ["[node]", 'id = "build"', f'listen = "127.0.0.1:{node_port}"', 'token_env = "TITAN_CONSOLE_NODE_TOKEN"', f"work_root = {quote(root / 'node')}", f"titan_program = {quote(args.binary)}", f"history_program = {quote(args.history)}", 'environments = ["backtest"]', "task_seconds = 900"]
    days = {"sideways": "2023-09-02", "up": "2023-10-23", "down": "2023-08-17"}
    cases = [(regime, direction) for regime in days for direction in ("neutral", "long_bias", "short_bias")]
    for regime, direction in cases:
        template = f"{regime}-{direction}"
        config += [f"[node.templates.{template}]", 'research_id="grid"', 'research_name="网格策略"', f"source = {quote(args.repo / 'strategies/grid/strategy.py')}", f"parameters = {quote(args.parameters / (template + '.parameters.json'))}"]
    config += ["[node.templates.foreign-grid]", 'research_id="other-strategy"', 'research_name="隔离测试策略"', f"source = {quote(args.repo / 'strategies/grid/strategy.py')}", f"parameters = {quote(args.parameters / 'sideways-neutral.parameters.json')}"]
    for regime, day in days.items():
        config += [f"[node.datasets.{regime}]", f"regime = {quote(regime)}", f"trades = {quote(args.data / ('BTCUSDT-aggTrades-' + day + '.csv'))}", f"quotes = {quote(args.data / ('BTCUSDT-bookTicker-' + day + '.csv'))}", f"funding = {quote(args.data / ('BTCUSDT-fundingRate-' + day[:7] + '.csv'))}"]
    config_path = root / "console.toml"; config_path.write_text("\n".join(config) + "\n"); config_path.chmod(0o600)
    processes = []
    def start(kind):
        log = (root / (kind + ".log")).open("ab")
        process = subprocess.Popen([str(args.binary), "console", kind, "--config", str(config_path)], env=env, stdout=log, stderr=log, start_new_session=True)
        processes.append(process); return process
    def client(): return request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    operator, reader = client(), client()
    def call(path, data=None, *, method=None, who=operator, csrf=None):
        headers = {"Origin": origin}
        if data is not None: headers["Content-Type"] = "application/json"
        if csrf: headers["X-CSRF-Token"] = csrf
        req = request.Request(origin + path, data=None if data is None else json.dumps(data).encode(), headers=headers, method=method)
        with who.open(req, timeout=15) as response:
            content = response.read(); return json.loads(content) if content else None
    def wait(callback, timeout=120):
        end = time.monotonic() + timeout; last = None
        while time.monotonic() < end:
            try:
                result = callback()
                if result: return result
            except (error.URLError, ConnectionError, TimeoutError) as failure: last = failure
            time.sleep(0.4)
        raise RuntimeError(f"condition timed out: {last}")
    results = []
    def record(name, **facts):
        results.append({"name": name, "passed": True, **facts})
        shown=results[-1].copy()
        if "metrics" in shown:shown["metrics"]={key:shown["metrics"][key] for key in ("net_pnl_usdt","max_drawdown_pct","max_pending_bound_notional_usdt","final_position_btc")}
        print(json.dumps(shown),flush=True)
    try:
        node_process=start("node"); manager_process=start("manager")
        session = wait(lambda: call("/api/v1/session", {"username": "operator", "password": password}))
        csrf = session["csrf_token"]
        reader_session = call("/api/v1/session", {"username": "reader", "password": password}, who=reader)
        wait(lambda: len(call("/api/v1/snapshot")["resources"]) >= 12, 600)
        try:
            call("/api/v1/operations", {"idempotency_key": "denied", "node_id": "build", "action": {"kind": "prepare", "dataset_id": "sideways"}}, who=reader, csrf=reader_session["csrf_token"])
            raise AssertionError("reader could submit a write")
        except error.HTTPError as failure: assert failure.code == 403
        record("reader-api-write-denied")
        def submit(action, key=None):
            return call("/api/v1/operations", {"idempotency_key": key or secrets.token_hex(16), "node_id": "build", "action": action}, csrf=csrf)
        def completed(op, success=True):
            actual = wait(lambda: (item if item["state"] in ("succeeded", "failed", "cancelled") else None) if (item := call("/api/v1/operations/" + op["id"])) else None, 900)
            assert actual["state"] == ("succeeded" if success else "failed"), actual
            return actual
        def find_resource(operation, kind):
            return next(item for item in operation["result"]["resources"] if item["kind"] == kind)
        for dataset_id in (days if args.matrix else ("sideways",)):
            prepared=completed(submit({"kind":"prepare","dataset_id":dataset_id}))
            record("immutable-dataset-prepare-"+dataset_id,sha256=prepared["result"]["prepared_sha256"])
        selected = [] if args.reuse else (cases if args.matrix else cases[:1])
        if args.reuse and args.matrix:
            found={}
            with sqlite3.connect(root/"manager.db") as db:
                for (data,) in db.execute("SELECT data FROM resources WHERE kind='backtest' ORDER BY rowid"):
                    item=json.loads(data);body=item["body"];metrics=body["metrics"]
                    if body["business_success"] and metrics["final_position_btc"]=="0.0":
                        found.setdefault((body["dataset_id"],metrics["direction"],body["stress"]),item)
            expected={(regime,str(direction),stress) for regime in days for direction in range(3) for stress in (False,True)}
            assert expected<=found.keys(),found.keys()
            for key in sorted(expected):
                prior=found[key]
                rerun=completed(submit({"kind":"backtest","artifact_id":prior["body"]["artifact_id"],"dataset_id":key[0],"stress":key[2]}))
                item=find_resource(rerun,"backtest")
                assert item["body"]["metrics"]["final_position_btc"]=="0.0"
                record("matrix-replay-"+str(key),operation_id=rerun["id"],metrics=item["body"]["metrics"])
        for regime, direction in selected:
            name = f"{regime}-{direction}"
            parameters = json.loads((args.parameters / (name + ".parameters.json")).read_text())
            action = {"kind": "describe", "template_id": name, "parameters": parameters}
            op = submit(action, "validate-" + name)
            assert submit(action, "validate-" + name)["id"] == op["id"]
            try:
                submit({**action, "parameters": {**parameters, "unexpected": 1}}, "validate-" + name)
                raise AssertionError("changed idempotency input was accepted")
            except error.HTTPError as failure: assert failure.code == 409
            validation = completed(op)
            version = call("/api/v1/parameter-versions", {"node_id": "build", "template_id": name, "parameters": parameters, "expected_revision": None, "freeze_operation_id": op["id"]}, csrf=csrf)
            assert version["frozen"]
            try:
                call("/api/v1/parameter-versions/" + version["id"], {"node_id": "build", "template_id": name, "parameters": parameters, "expected_revision": version["revision"], "freeze_operation_id": None}, method="PATCH", csrf=csrf)
                raise AssertionError("frozen version was mutable")
            except error.HTTPError as failure: assert failure.code == 409
            building=submit({"kind": "build", "version_id": version["id"]})
            if regime=="sideways" and direction=="neutral":
                wait(lambda: call("/api/v1/operations/"+building["id"])["state"] in ("running","succeeded"))
                for process in (manager_process,node_process):
                    process.send_signal(signal.SIGINT);process.wait(timeout=10)
                node_process=start("node");manager_process=start("manager")
                wait(lambda: call("/api/v1/operations/"+building["id"]))
                record("manager-and-node-restart-preserve-task")
            built = completed(building)
            with sqlite3.connect(root/"node/runs.sqlite3") as db:
                assert db.execute("SELECT COUNT(*) FROM runs WHERE id=?",(building["id"],)).fetchone()[0]==1
            artifact = find_resource(built, "artifact")
            record("compile-" + name, artifact_id=artifact["id"], metadata_schema=validation["result"]["metadata"]["state_schema_version"])
            for stress in ((False, True) if args.matrix else (False,)):
                backtest = completed(submit({"kind": "backtest", "artifact_id": artifact["id"], "dataset_id": regime, "stress": stress}))
                result = find_resource(backtest, "backtest")
                record("backtest-" + name + ("-stress" if stress else "-base"), operation_id=backtest["id"], metrics=result["body"]["metrics"])
                if not results[-1]["metrics"]["success"]: raise AssertionError("history engine did not complete safely")
                if regime == "sideways" and direction == "neutral" and not stress:
                    report = completed(submit({"kind": "report", "backtest_id": result["id"]}))
                    with operator.open(origin + "/api/v1/files/" + report["id"] + "/report.html") as response:
                        assert b"Verified ResultBundle" in response.read()
                    record("verified-report-render-and-download")
        invalid = submit({"kind": "describe", "template_id": "sideways-neutral", "parameters": {"unexpected": 1}})
        completed(invalid, success=False); record("engine-validation-rejection")
        pending=submit({"kind":"prepare","dataset_id":"sideways"})
        call("/api/v1/operations/"+pending["id"]+"/cancel",{},csrf=csrf)
        cancelled=wait(lambda:(item if item["state"]=="cancelled" else None) if (item:=call("/api/v1/operations/"+pending["id"])) else None)
        assert cancelled["cancel_requested"];record("cancellation-confirmed-by-execution-path")
        if args.browser:
            subprocess.run(["node",str(args.repo/"scripts/management_browser_smoke.mjs"),origin,str(root/"browser")],env=dict(env,TITAN_BROWSER_PASSWORD=password),check=True,timeout=600)
            record("browser-e2e")
        (root / ("results-ui.json" if args.reuse and not args.matrix else "results.json")).write_text(json.dumps({"passed": True, "checks": results}, indent=2))
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try: process.wait(timeout=10)
                except subprocess.TimeoutExpired: process.kill(); process.wait()


if __name__ == "__main__": main()
