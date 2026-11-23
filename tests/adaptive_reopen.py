#!/usr/bin/env python3
"""验证 --mirror-reopen-duty-pct 的自适应真的生效（非空测）。

小镜像快照便宜 => 保持基础周期；大镜像快照贵 => 周期被占空比下限抬高。
用 duty=50% 把 1000 万行表的 floor 压到可观测范围（约 38 周期），否则要等 380s。
"""
import os, re, signal, subprocess, sys, time
import pymysql, requests

BIN = os.path.abspath("./target/debug/xsqld")
U, A = "http://127.0.0.1:18280", "http://127.0.0.1:18281"
LOG = "/tmp/xsqld-adaptive.log"
res = []
def rec(n, ok, ev):
    res.append((n, ok, ev)); print(f"[{'PASS' if ok else 'FAIL'}] {n}\n       {ev}", flush=True)

subprocess.run(["pkill","-TERM","-f","target/debug/xsqld"],capture_output=True); time.sleep(3)
env = {k:v for k,v in os.environ.items() if k.lower() not in ("http_proxy","https_proxy","all_proxy")}
env["RUST_LOG"]="info"
p = subprocess.Popen([BIN,"--mysql-host","127.0.0.1","--mysql-port","12981","--mysql-user","root",
    "--http-listen-addr","127.0.0.1:18280","--admin-listen-addr","127.0.0.1:18281",
    "--mirror-enabled","--mirror-dir","/tmp/xsqld-adaptive-mirrors","--mirror-interval-ms","1000",
    "--mirror-reopen-cycles","10","--mirror-reopen-duty-pct","50"],
    stdout=open(LOG,"w"),stderr=subprocess.STDOUT,env=env)
try:
    t0=time.time()
    while time.time()-t0<90:
        try:
            if requests.get(f"{U}/health",timeout=2).status_code==200: break
        except Exception: pass
        time.sleep(0.5)
    c=pymysql.connect(host="127.0.0.1",port=12981,user="root",password="",autocommit=True)
    cu=c.cursor(); cu.execute("CREATE DATABASE IF NOT EXISTS small_ad"); cu.execute("USE small_ad")
    cu.execute("SHOW TABLES")
    for (t,) in cu.fetchall(): cu.execute(f"DROP TABLE IF EXISTS `{t}`")
    cu.execute("CREATE TABLE s(id INT PRIMARY KEY, v INT)")
    cu.execute("INSERT INTO s VALUES(1,1),(2,2),(3,3)")
    for ns in ("small_ad","cg"):
        requests.post(f"{A}/v1/namespaces/{ns}/create",json={},timeout=60)
        requests.post(f"{U}/v2/pipeline",json={"baton":None,"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}},{"type":"close"}]},headers={"Host":f"{ns}.db"},timeout=60)
    # 等两个命名空间都出现重开日志
    seen={}
    t0=time.time()
    while time.time()-t0<240 and len(seen)<2:
        for ln in open(LOG,errors="replace"):
            if "mirror instance reopened" in ln:
                ns=re.search(r'namespace=(\S+)',ln); th=re.search(r'threshold=(\d+)',ln)
                cf=re.search(r'cost_floor=(\d+)',ln); sm=re.search(r'snapshot_ms=(\d+)',ln)
                if ns and th: seen[ns.group(1)]=(int(th.group(1)),int(cf.group(1)) if cf else -1,int(sm.group(1)) if sm else -1)
        time.sleep(2)
    rec("两个命名空间都触发了重开（非空测）", len(seen)==2, f"seen={seen}")
    if "small_ad" in seen:
        th,cf,sm = seen["small_ad"]
        rec("小镜像保持基础周期 10（快照便宜，不该被拉长）", th==10, f"threshold={th} cost_floor={cf} snapshot_ms={sm}")
    if "cg" in seen:
        th,cf,sm = seen["cg"]
        rec("大镜像周期被占空比抬高（>基础 10）", th>10 and cf>10, f"threshold={th} cost_floor={cf} snapshot_ms={sm}")
        rec("抬高幅度与实测快照耗时一致", sm>1000 and abs(cf - round(sm/1000/0.5))<=2,
            f"snapshot_ms={sm} => 期望 floor≈{round(sm/1000/0.5)}, 实际 {cf}")
    c.close()
finally:
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try: p.wait(timeout=40)
        except subprocess.TimeoutExpired: p.kill()
bad=[n for n,ok,_ in res if not ok]
print(f"\n{len(res)-len(bad)}/{len(res)} PASS"+(f"，失败：{bad}" if bad else ""))
sys.exit(1 if bad else 0)
