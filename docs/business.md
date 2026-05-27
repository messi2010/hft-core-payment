# Business / domain model

tbjava is a **double-entry ledger** for payment platforms. It answers one
question reliably and fast: *what is the balance of every account, given an
ordered stream of value movements?* It does not price, match, or route — it
records money moving between accounts with the guarantees an accounting system
needs.

## 1. Core concepts

| Concept | Meaning |
|---|---|
| **Account** | A bucket that money moves into and out of. Identified by a 128-bit `id` (`UInt128`, UUID/ULID-friendly), scoped to a `ledger`, typed by a `code`. |
| **Ledger** | A currency/unit boundary. A transfer may only move value between two accounts on the **same** ledger. (e.g. `ledger=840` = USD-cents.) |
| **Code** | A user-defined classification of the account or transfer (e.g. "customer wallet", "fee", "settlement"). Must be non-zero. |
| **Transfer** | A single, balanced movement: it debits one account and credits another by the same `amount`. |
| **Amount** | u64 minor units (e.g. cents). Treated as unsigned; max 9.2e18. |

### Double-entry

Every transfer has **exactly one debit leg and one credit leg of equal amount**.
The engine cannot represent an unbalanced movement — there is no API to "just
change a balance". This means the sum of all debits always equals the sum of all
credits across a ledger, by construction.

### Balances

Each account tracks four counters:

| Counter | Meaning |
|---|---|
| `debitsPosted` | settled value debited |
| `creditsPosted` | settled value credited |
| `debitsPending` | value reserved by not-yet-settled pending transfers (debit side) |
| `creditsPending` | value reserved by pending transfers (credit side) |

Net position depends on the account's accounting nature: use
`AccountSnapshot.creditBalance()` (`creditsPosted − debitsPosted`) for
credit-normal accounts (liability/income/equity) and `debitBalance()` for
debit-normal accounts (asset/expense). `balance()` is kept as an alias of
`creditBalance()`. Which one applies is decided by the chart of accounts (the
integrator's `code` + balance-limit flags), not by the engine.

## 2. Account semantics

**Creation** (`POST /v1/accounts`) validates: `id`, `ledger`, `code` all
non-zero; balance counters must be zero; the two balance-limit flags are mutually
exclusive. Re-creating the same `id`:

- identical fields → `EXISTS` (idempotent, safe to retry);
- different fields → `EXISTS_WITH_DIFFERENT_FIELDS` (rejected).

**Balance-limit flags** enforce the account's sign:

- `DEBITS_MUST_NOT_EXCEED_CREDITS` — for asset/expense accounts that must not go
  "negative": a posting is refused (`EXCEEDS_DEBITS_LIMIT`) if
  `debitsPosted + debitsPending + amount > creditsPosted`.
- `CREDITS_MUST_NOT_EXCEED_DEBITS` — the mirror, for liability/income accounts
  (`EXCEEDS_CREDITS_LIMIT`).

Accounts without either flag may hold any balance (e.g. an equity/clearing
account).

### Mapping a chart of accounts

The engine has no first-class "account type" (this is the TigerBeetle model).
You encode the type yourself via `code` (your classification) and pick the
balance-limit flag that matches the account's normal side. Use this mapping:

| Account type | Normal side | Flag to set | Read net balance via |
|---|---|---|---|
| Asset | debit | `CREDITS_MUST_NOT_EXCEED_DEBITS` | `debitBalance()` |
| Expense | debit | `CREDITS_MUST_NOT_EXCEED_DEBITS` | `debitBalance()` |
| Liability | credit | `DEBITS_MUST_NOT_EXCEED_CREDITS` | `creditBalance()` |
| Income / Revenue | credit | `DEBITS_MUST_NOT_EXCEED_CREDITS` | `creditBalance()` |
| Equity | credit | `DEBITS_MUST_NOT_EXCEED_CREDITS` | `creditBalance()` |
| Clearing / suspense | either | *(none — may swing both ways)* | either |

The flag is **optional**: omit it for accounts that legitimately swing both ways
(clearing, some equity). But if you set the *wrong* flag for an account's normal
side, valid postings will be rejected — so the chart-of-accounts mapping above is
the integrator's responsibility, not the engine's.

## 3. Transfer types

A transfer's behaviour is selected by its flags. The three two-phase flags are
mutually exclusive.

### Standard transfer

No special flag. Immediately moves `amount` from debit to credit as **posted**
value. Subject to balance limits and the closed/ledger checks.

### Two-phase (pending) transfers

For workflows that reserve funds now and settle later (authorizations, escrow):

1. **PENDING** — reserves `amount`: increments `debitsPending`/`creditsPending`.
   Funds are held but not settled. An optional `timeoutSeconds` sets an expiry.
2. **POST_PENDING_TRANSFER** — settles a pending transfer referenced by
   `pendingId`. Releases the full pending reservation and posts the settled
   `amount` (which may be ≤ the original; the remainder is simply released). The
   amount defaults to the original if left 0; posting *more* than the original is
   refused (`EXCEEDS_PENDING_TRANSFER_AMOUNT`).
3. **VOID_PENDING_TRANSFER** — cancels a pending transfer: releases the
   reservation, posts nothing.

A pending transfer can be posted/voided once. Re-posting or posting a
voided/expired/non-pending one is refused
(`PENDING_TRANSFER_NOT_PENDING` / `PENDING_TRANSFER_EXPIRED` /
`PENDING_TRANSFER_NOT_FOUND`).

> **Expiry.** Expiry is enforced two ways: *lazily* (a post/void of an expired
> pending is refused with `PENDING_TRANSFER_EXPIRED`), and *actively* by a
> periodic sweep (`tbjava.expiry-sweep-seconds`, default 30s) that auto-voids
> timed-out pendings and releases their reserved funds. The sweep is journaled as
> an `EXPIRE` command tagged with an as-of timestamp, so recovery reproduces the
> exact same voids deterministically.

### Linked atomic batches

A transfer with the `LINKED` flag is chained to the **next** transfer in the
batch. A chain is a maximal run of linked transfers ending at the first transfer
*without* the flag. The chain is **all-or-nothing**: if any member fails, the
whole chain rolls back and every member returns `LINKED_EVENT_FAILED` (the actual
failure code is reported on the member that caused it). This is how you express
"move A→B and B→C, or neither".

`EXISTS` (idempotent duplicate) counts as success within a chain.

## 4. Idempotency

Both accounts and transfers are idempotent by `id`. A retried create with
identical fields returns `EXISTS` and changes nothing; with different fields it
returns `EXISTS_WITH_DIFFERENT_FIELDS` and is rejected. Clients can therefore
safely retry on timeout without double-applying — the foundation for
at-least-once delivery from upstream systems.

## 5. Account history

`GET /v1/accounts/{id}/transfers?limit=N` returns the most recent `N` transfers
that touched the account (as either debit or credit), newest first. Backed by an
in-memory secondary index; `limit` defaults to 100, capped at 1000.

## 6. HTTP API summary

| Method & path | Purpose |
|---|---|
| `POST /v1/accounts` | Create a batch of accounts. Returns a result code per account. |
| `POST /v1/transfers` | Create a batch of transfers (may include linked chains). Returns a result code per transfer. |
| `GET /v1/accounts/{id}` | Fetch an account snapshot (balances + fields), or 404. |
| `GET /v1/transfers/{id}` | Fetch a transfer, or 404. |
| `GET /v1/accounts/{id}/transfers?limit=N` | Account transfer history, newest first. |
| `GET /v1/trial-balance` | Per-ledger summed counters; `balanced` flags whether posted debits == posted credits. |

Batch endpoints return the result-code **name** per element, positionally aligned
with the request. A batch is *not* atomic as a whole — only `LINKED` chains
within it are atomic.

## 7. Worked examples

**Create two accounts (USD-cents ledger), then transfer 1000:**

```json
POST /v1/accounts
{"accounts":[
  {"id":1001,"ledger":840,"code":10,"flags":0,"userData64":0,"userData32":0},
  {"id":1002,"ledger":840,"code":10,"flags":0,"userData64":0,"userData32":0}
]}                                  → ["OK","OK"]

POST /v1/transfers
{"transfers":[
  {"id":5001,"debitAccountId":1001,"creditAccountId":1002,"amount":1000,
   "pendingId":0,"userData64":0,"userData32":0,"timeoutSeconds":0,
   "ledger":840,"code":720,"flags":0}
]}                                  → ["OK"]
# 1001: debitsPosted=1000   1002: creditsPosted=1000
```

**Two-phase: authorize then capture:**

```
PENDING   id=6001 1001→1002 amount=1000 flags=PENDING  → holds 1000
POST      id=6002 pendingId=6001 amount=800            → settles 800, releases 200
# 1001: debitsPending=0, debitsPosted=800
```

**Linked, all-or-nothing (A→B and B→C):**

```
id=7001 1001→1002 amount=500 flags=LINKED   ┐ atomic
id=7002 1002→1003 amount=500 flags=0        ┘
# if 7002 fails → both roll back; 7001 returns LINKED_EVENT_FAILED
```

## 8. Result codes

`CreateAccountResult` / `CreateTransferResult` mirror TigerBeetle's codes so a TB
client can switch with minimal change. The transfer codes group as:

- **Outcome:** `OK`, `EXISTS`, `EXISTS_WITH_DIFFERENT_FIELDS`, `LINKED_EVENT_FAILED`
- **Field validation:** `*_MUST_NOT_BE_ZERO`, `ACCOUNTS_MUST_BE_DIFFERENT`,
  `FLAGS_ARE_MUTUALLY_EXCLUSIVE`
- **References:** `DEBIT/CREDIT_ACCOUNT_NOT_FOUND`,
  `ACCOUNTS_MUST_HAVE_SAME_LEDGER`, `TRANSFER_MUST_HAVE_SAME_LEDGER_AS_ACCOUNTS`
- **Account state:** `DEBIT/CREDIT_ACCOUNT_CLOSED`
- **Balance:** `EXCEEDS_DEBITS_LIMIT`, `EXCEEDS_CREDITS_LIMIT`, `OVERFLOWS_*`
- **Two-phase:** `PENDING_TRANSFER_NOT_FOUND`, `PENDING_TRANSFER_NOT_PENDING`,
  `PENDING_TRANSFER_EXPIRED`, `EXCEEDS_PENDING_TRANSFER_AMOUNT`,
  `PENDING_TRANSFER_HAS_DIFFERENT_*`

## 9. Flag & result-code coverage

For protocol compatibility the enums declare the full TigerBeetle set, but not
all are enforced yet. Honest current state:

**Account flags**

| Flag | Status |
|---|---|
| `DEBITS_MUST_NOT_EXCEED_CREDITS` | ✅ enforced |
| `CREDITS_MUST_NOT_EXCEED_DEBITS` | ✅ enforced |
| `CLOSED` | ✅ enforced on read (transfers to/from a closed account are refused); can only be set at creation |
| `LINKED` | reserved/ignored on accounts |
| `HISTORY` | declared; CDC stream not implemented |

**Transfer flags**

| Flag | Status |
|---|---|
| `LINKED` | ✅ enforced (atomic chains) |
| `PENDING` | ✅ enforced |
| `POST_PENDING_TRANSFER` | ✅ enforced |
| `VOID_PENDING_TRANSFER` | ✅ enforced |
| `BALANCING_DEBIT` / `BALANCING_CREDIT` | ❌ declared, not yet enforced |
| `CLOSING_DEBIT` / `CLOSING_CREDIT` | ❌ declared, not yet enforced (so `CLOSED` is set only at creation) |
| `HISTORY` | ❌ declared; CDC not implemented |

**Result codes now enforced:** `OVERFLOWS_DEBITS_POSTED` / `OVERFLOWS_CREDITS_POSTED`
/ `OVERFLOWS_DEBITS_PENDING` / `OVERFLOWS_CREDITS_PENDING` — a transfer that would
wrap a u64 counter is refused, leaving balances intact.

**Result codes not yet produced:** `PENDING_TRANSFER_HAS_DIFFERENT_*` — post/void
uses the original transfer's accounts/ledger directly rather than validating that
the values supplied on the post/void match the original.

## 10. Guarantees & invariants

- **Atomicity:** a single transfer is atomic; a `LINKED` chain is atomic; an
  unlinked batch is not atomic as a whole.
- **Durability:** a result is returned only after the command is fsynced to the
  journal. A crash loses at most the un-acknowledged tail.
- **Idempotency:** safe retry by `id`.
- **Determinism:** the same command stream always yields the same state — the
  basis for recovery and for future replica/audit verification.
- **Double-entry:** debits and credits are always equal and opposite, and the
  per-ledger trial balance (`GET /v1/trial-balance`) makes this observable.
- **Overflow-safe:** a transfer that would wrap a u64 balance counter is refused
  rather than silently corrupting the balance.
- **Pending expiry:** timed-out pending reservations are auto-voided and the
  funds released, deterministically (replayable).

What it does **not** guarantee today: high availability (single node; wrap with
external consensus for HA) or multi-currency atomic transfers (single `ledger`
per transfer). See [design.md](design.md#deliberately-out-of-scope).
