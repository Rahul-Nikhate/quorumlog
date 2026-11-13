# Deploying QuorumLog

There are three ways to run QuorumLog, from quickest to most realistic:

| Where | What runs | Persistence | Cost |
|---|---|---|---|
| [Local](#1-local) | `qlogd --demo` (3 nodes, 1 process) or 3 containers with Docker Compose | local disk / named volumes | free |
| [Render free web service](#2-render-free-tier) | `qlogd --demo` in one container | none: data resets on every restart or spin-down | free |
| [Oracle Cloud Always Free VM](#3-oracle-cloud-always-free-ampere-a1-vm) | 3 containers with Docker Compose, optional Caddy for HTTPS | named Docker volumes on the boot volume | free (within Always Free limits) |

The dashboard is served at `/` by every node, and the JSON API lives under `/v1/` (contract: the "HTTP API contract"
section of [SCOPE.md](SCOPE.md)).

---

## 1. Local

### 1a. Demo mode (one process, three Raft nodes)

```sh
go run ./cmd/qlogd --demo                # dashboard + API on http://localhost:8080
PORT=9000 go run ./cmd/qlogd --demo      # or pick another port
```

Demo mode starts three nodes inside one process. They talk to each other over localhost TCP, each node has its own
data directory, and the dashboard shows **Kill** and **Restart** buttons for every node. Pass `--data-dir DIR` to choose
where the three data directories go.

The same thing in Docker (the image's default command is `qlogd --demo --data-dir /data`):

```sh
docker build -t quorumlog .
docker run --rm -p 8080:8080 quorumlog
```

### 1b. Three real nodes with Docker Compose

[`deploy/docker-compose.yml`](../deploy/docker-compose.yml) runs one `qlogd` per container (not demo mode). Each
container has its own named volume, and they talk Raft over the compose network on port 7001.

```sh
docker compose -f deploy/docker-compose.yml up -d --build
open http://localhost:8081        # node 1 (8082 = node 2, 8083 = node 3)

# Failover by hand: find the leader on the dashboard, then stop that container
docker compose -f deploy/docker-compose.yml stop qlogd2
docker compose -f deploy/docker-compose.yml start qlogd2   # it restarts with its data and catches up

docker compose -f deploy/docker-compose.yml logs -f        # follow the logs
docker compose -f deploy/docker-compose.yml down           # stop (keeps the volumes)
docker compose -f deploy/docker-compose.yml down -v        # stop and delete all data
```

Each node runs with these flags (`docker compose ... config` prints the full file):

```
--id N  --peers 1=qlogd1:7001,2=qlogd2:7001,3=qlogd3:7001
--http-peers 1=qlogd1:8080,2=qlogd2:8080,3=qlogd3:8080
--raft-addr 0.0.0.0:7001  --http-addr 0.0.0.0:8080  --data-dir /data  --fsync always
```

`--peers` lists the Raft addresses, and `--http-peers` lists the HTTP addresses that followers use to forward requests
(writes and reads) to the leader. `--fsync` accepts `always`, `interval=10ms` or `never`. Every flag can also be set as an environment
variable, `QLOG_<FLAG>` with dashes turned into underscores (for example `QLOG_HTTP_PEERS`). See `qlogd -h`.

Any node accepts requests and forwards writes and reads to the group's leader, at most one hop (response headers
`X-QL-Leader` and `X-QL-Forwarded: true`).
Outside demo mode the dashboard has no Kill/Restart buttons. The guided demo tells you which container to stop and
then watches the election.

---

## 2. Render (free tier)

[`render.yaml`](../render.yaml) is a [Render Blueprint](https://render.com/docs/blueprint-spec). It defines one free
web service with `runtime: docker`. The service builds the repo's `Dockerfile`, runs `qlogd --demo`, and checks
`/v1/health`.

**Know the free-tier limits first** (from [Render: Deploy for Free](https://render.com/docs/free)):

- Free web services **spin down after 15 minutes without inbound traffic**. The next request wakes the service, which
  "takes about one minute", and Render shows a loading page in the meantime.
- The filesystem is **ephemeral**, and free web services **cannot attach a persistent disk**. Anything written is lost
  whenever the service redeploys, restarts or spins down. For QuorumLog this means the **demo data resets**, and the
  dashboard footer says so. Raft durability still holds *inside* one run: killing and restarting a demo node keeps that
  node's data directory.
- Each workspace gets 750 free instance hours per calendar month. A single service fits in that.
- Render routes traffic to the port in the `PORT` environment variable (default `10000`), and the app must bind
  `0.0.0.0` ([Render: Web Services](https://render.com/docs/web-services)). The Blueprint sets `PORT=8080` to match the
  image, and `qlogd --demo` listens on `0.0.0.0:$PORT`.

**Steps**

1. Push this repository to GitHub, for example `github.com/<you>/quorumlog`. Keep `render.yaml` at the repo root.
2. In the [Render Dashboard](https://dashboard.render.com), click **New → Blueprint**.
3. Connect your GitHub account if you haven't yet, then click **Connect** next to the repository. Enter a Blueprint
   name, pick the branch to deploy, and leave *Blueprint Path* empty (`render.yaml` is at the root). Render lists one
   service to create, `quorumlog` (web, Docker, Free).
4. Click **Deploy Blueprint**. The first build compiles the Go binaries inside `golang:1.26` and takes a few minutes.
   The build log is on the service page.
5. When the deploy is live, open the service URL. It looks like **`https://quorumlog.onrender.com`**. If that name is
   taken, Render adds a suffix such as `https://quorumlog-abcd.onrender.com`. The exact URL is at the top of the
   service page.
6. Check it: `curl https://<your-service>.onrender.com/v1/health` should return `{"node_id":…,"ok":true}`. Then run the
   **Guided demo** on the dashboard.

`autoDeployTrigger: commit` redeploys on every push to the linked branch. Use `checksPass` to wait for CI first, or
`off` to deploy by hand. After 15 idle minutes the first visitor waits about a minute while the service starts, and
sees an empty cluster.

---

## 3. Oracle Cloud Always Free (Ampere A1 VM)

This option gives you a VM that stays up, with real disks, so the three-container cluster keeps its data across
restarts. Facts below are from Oracle's
[Always Free Resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
page (checked 2026-11-13):

- **A1 compute:** "the first 1,500 OCPU hours and 9,000 GB hours per month for free for VM instances using the
  VM.Standard.A1.Flex shape … For Always Free tenancies, this is equivalent to 2 OCPUs and 12 GB of memory." That is
  plenty for three `qlogd` containers.
- **Storage:** 200 GB of Always Free block volume storage in total. Each boot volume is at least 47 GB, and 50 GB by
  default.
- **Capacity:** "If you receive an 'out of host capacity' error … this indicates a temporary lack of Always Free shapes
  in your home region." Retry later, or try another availability domain.
- **Idle reclamation:** "Idle Always Free compute instances may be reclaimed by Oracle." An instance counts as idle if,
  over 7 days, 95th-percentile CPU utilization is below 20%, network utilization is below 20%, and (A1 only) memory
  utilization is below 20%. An idle demo cluster can meet all three, so expect to recreate the VM now and then, or keep
  it in use. Back up anything you care about.

### 3.1 Create the instance

1. In the OCI Console, open **Compute → Instances → Create instance**.
2. **Image:** Canonical Ubuntu 24.04. **Shape:** *Change shape → Ampere → VM.Standard.A1.Flex*, with 1–2 OCPUs and
   6–12 GB of memory (must be "Always Free-eligible").
3. **Networking:** keep the default VCN and public subnet, and leave **Assign a public IPv4 address** on.
4. **SSH keys:** upload your public key, or download the generated key pair.
5. Click **Create**. When the instance is *Running*, note its public IP and connect:
   `ssh -i <key> ubuntu@<public-ip>`.

### 3.2 Open the ports (two firewalls)

The cloud **security list** and the VM's own **iptables** both block traffic by default.

1. **Security list.** On the instance page, click the subnet under *Primary VNIC*, open its **Default Security List**,
   and choose **Add Ingress Rules**: source `0.0.0.0/0`, protocol TCP, destination ports `80,443` if you use Caddy
   (recommended). Without Caddy, open `8081-8083` instead. See OCI's
   [Security Lists](https://docs.oracle.com/en-us/iaas/Content/Network/Concepts/securitylists.htm) docs.
2. **iptables on the VM.** Oracle's Ubuntu images end the INPUT chain with a REJECT rule, so new rules must be
   *inserted* before it, not appended:

   ```sh
   sudo iptables -I INPUT 1 -p tcp --dport 80  -m state --state NEW -j ACCEPT
   sudo iptables -I INPUT 1 -p tcp --dport 443 -m state --state NEW -j ACCEPT
   sudo netfilter-persistent save          # persists to /etc/iptables/rules.v4
   ```

   Ports that Docker publishes (8081–8083) are forwarded by Docker's own iptables rules and don't need INPUT rules.
   The security list still controls whether the internet can reach them.

### 3.3 Install Docker and start the cluster

```sh
# Docker Engine + Compose plugin (https://docs.docker.com/engine/install/ubuntu/)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER" && newgrp docker

git clone https://github.com/<you>/quorumlog.git
cd quorumlog
docker compose -f deploy/docker-compose.yml up -d --build    # builds natively for arm64
docker compose -f deploy/docker-compose.yml ps
curl -s localhost:8081/v1/health
```

`restart: unless-stopped` brings the containers back after a reboot. The data lives in the named volumes `qlog1..3`
(`docker volume ls`).

### 3.4 Optional: HTTPS with Caddy and a free hostname

Let's Encrypt needs a hostname, not a bare IP. Two free options:

- **sslip.io:** `203-0-113-10.sslip.io` resolves to `203.0.113.10`, so you only need to replace the dots in your IP
  with dashes ([sslip.io](https://sslip.io)).
- **DuckDNS:** register `yourname.duckdns.org` and point it at the public IP ([duckdns.org](https://www.duckdns.org)).

Install Caddy from its apt repository ([Caddy install docs](https://caddyserver.com/docs/install#debian-ubuntu-raspbian)),
then replace `/etc/caddy/Caddyfile` with:

```caddyfile
203-0-113-10.sslip.io {
	# Any node can serve the dashboard and accept writes (followers forward
	# to the leader). Use node 1 first and fail over if it stops answering.
	reverse_proxy 127.0.0.1:8081 127.0.0.1:8082 127.0.0.1:8083 {
		lb_policy first
		health_uri /v1/health
		health_interval 2s
	}
}
```

```sh
sudo systemctl reload caddy
curl -s https://203-0-113-10.sslip.io/v1/health
```

Caddy gets and renews the certificate automatically, which is why ports 80 and 443 must be open. Long-poll fetches
(`wait_ms`) need no extra proxy settings. With Caddy in front, you can remove `8081-8083` from the security list so
only HTTPS is public.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Render page loads for about a minute, then shows an empty cluster | The free instance spun down and restarted, which resets its data. This is expected. |
| Render deploy fails its health check | `qlogd` is not listening on `0.0.0.0:$PORT`, or `/v1/health` is failing. Check the service logs. |
| Dashboard shows "API unreachable" | The node that served the page is down. Open another node's port (8081/8082/8083), or use Caddy's failover. |
| Writes fail with `UNAVAILABLE` or `TIMEOUT` | Fewer than 2 of the 3 nodes are up, so there is no majority. Start a stopped node. |
| OCI: connection times out | A port is missing from the security list or from iptables (see 3.2). |
| OCI: "out of host capacity" | Always Free A1 capacity is temporarily exhausted in that region or AD. Retry later. |
