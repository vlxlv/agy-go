# Architecture

`agy-go` is a suite designed to provide three independent binaries:

### Components

1. **`agy-pool`**
   - Stable multi-account proxy and runtime component.
   - Current implementation originates from the verified `agy-pool-go` baseline.

2. **`agy-tokei`**
   - Future AGY usage analytics and terminal dashboard.
   - *Not implemented yet.*

3. **`agy-db`**
   - Future AGY conversation database inventory and retention tool.
   - *Not implemented yet.*

---

### Invariants

1. **Critical Path:** `agy-pool` is the critical runtime path.
2. **Path Isolation:** `agy-tokei` and `agy-db` must remain strictly outside the `agy-pool` proxy/SSE request path.
3. **Failure Containment:** Failure of `agy-tokei` or `agy-db` must never affect:
   - Proxy availability
   - Authentication
   - Account scheduling
   - Quota refresh
   - SSE streaming
   - Failover
   - No-replay guarantees
4. **Accounting Truth:** Future AGY token accounting uses AGY's own conversation databases as the usage source of truth, not proxy/SSE interception.
5. **Infrastructure Independence:** The currently installed `agy-pool` used to develop this project is external development infrastructure and is not to be replaced during development.
