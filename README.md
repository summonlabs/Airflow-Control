# Airflow Control

Vendor-neutral airflow-oriented control semantics for rooms, rows, racks,
containment, and pressure relationships, for the Data Center Control Plane.

**The core question.** Given current airflow and pressure evidence, containment
state, thermal obligations, fan policy, authority, and generation, what
airflow-oriented control transition or setpoint request may be attempted safely,
and how is commanded state kept separate from observed and verified physical
effect?

Airflow Control answers that question and nothing else. It adjudicates airflow
control. It does not decide how the cooling plant is arranged, how much cooling
capacity exists, what a thermal zone's authority is, or where a workload should
run.

## Systems boundary

**This runtime owns**

* stable identities for airflow devices, containment elements, pressure
  relationships, obligations, interlocks, grants, and overrides, with device
  generation separated from state revision;
* airflow device lifecycle (provisioned, active, maintenance, degraded,
  isolated, faulted, retired) with an explicit declared transition table;
* room, row, and rack airflow obligations, with the binding that says how each
  one is evaluated against evidence;
* pressure relationships modelled as an explicit positive, negative, or neutral
  polarity plus a validated band — never as a free-form string;
* containment state relevant to airflow control, with a documented safety
  precedence that is deliberately not the numeric order of the enumeration;
* fan and setpoint policy at the abstraction boundary, and the safe operating
  envelope that policy supplies;
* commanded state, observed state, and verified state as three separate values;
* control attempts: intent, preconditions, adapter command, acknowledgement,
  observed effect, verified effect, failure, cancellation, explicit
  supersession, and the idempotency that makes a retry safe;
* interlocks, permission grants, maintenance overrides, and safety permits
  issued by the systems that own them;
* durable, integrity-checked persistence of all of the above, with crash and
  rollback semantics;
* one narrow vendor adapter interface for plant integration.

**This runtime does not own** cooling topology, cooling capacity accounting,
liquid-cooling loops, thermal-zone authority, global cooling failover,
thermal-emergency coordination, low-level fan motor firmware or PID behaviour,
BMS device protocols as the product boundary, or facility placement and power
policy. It consumes limits, obligations, spaces, and external conditions from
those systems as typed, generation-stamped references and opaque identifiers,
and it never reproduces their policy engines. It never derives a limit; it
enforces the limit it was given, and reports that the limit is unknown when it
was not given one.

Rooms, rows, racks, and measurement points are **opaque references**. This
runtime never resolves, walks, or re-derives topology; it stores the reference
the owning layer published and refuses a request whose reference does not match.

## The rule this library exists to enforce

An acknowledgement is not an effect. A command object is not physical actuation.
Telemetry is not authority. A value that was recovered from disk is not current
evidence. Zero is not unknown, and "unsupported" is not "unavailable".

Every one of those statements is a separate, testable state in this library:

| concept | where it lives | what it means |
|---|---|---|
| permission | `PermissionGrant` | an epoch-scoped, generation-scoped, expiring reference from the owning authority |
| authorisation | `ActuationAuthorization` | proof, constructed only by the engine, that every precondition was validated |
| command | `AdapterCommand` | what was handed to an adapter; not an effect |
| acknowledgement | `AdapterOutcome` | what the adapter said; not an effect |
| observation | `Observation` | what a source reported, with quality, generation, and freshness |
| verified effect | `VerifiedEffect` | what a *fresh, bound* observation established after a command |

## Architecture

    include/airflow_control/  the public API (14 headers)
    src/                      the implementation
    src/detail/               the codec, CRC, file, path, container, and
                              serialization layers
    tools/airflow_cli.cpp     the airflow-control administration tool
    examples/                 five runnable scenarios
    bench/                    the completed-operation benchmark
    tests/                    the proof-obligation suite
    downstream/consumer/      an out-of-tree find_package consumer
    docs/artifact-format.md   the durable byte format

The public surface is strongly typed: identities, generations, epochs,
incarnations, revisions, ticks, sequences, and attempt ordinals are distinct
types, and none of them is a generic integer. Physical quantities are exact
integers in fixed units — airflow in whole cubic metres per hour, differential
pressure in whole millipascals, temperature in whole millidegrees Celsius, and
fan setpoints in basis points of design airflow. Floating point is never used
for an authoritative value. The engine is a pimpl, so the header stays stable and
the mutable state stays private.

### Lifecycle

Seven states, twenty-four declared transitions, no others. `active` and
`degraded` permit control; `maintenance` permits it only with an explicit,
scoped, expiring, in-epoch maintenance override; `provisioned`, `isolated`,
`faulted`, and `retired` refuse it. `retired` is terminal. Every transition
names a class (commission, service, recovery, administrative, fault), and the
class decides which permission action the caller must hold — the action is never
inferred from the state names.

### Deterministic validation precedence

The primary code returned for an invalid request is a function of the request and
the current state, in this order:

1. request shape — `invalid_argument`, `out_of_range`
2. idempotent replay — the retained result, or `idempotency_conflict`
3. identity resolution — `not_found`
4. identity binding — `identity_mismatch`
5. lifecycle gate — `lifecycle_forbidden`
6. unresolved attempt — `attempt_unresolved`, `supersession_unsupported`
7. containment precedence — `containment_breached`, `containment_unknown`,
   `containment_open`
8. device generation — `generation_mismatch`
9. state revision — `revision_mismatch`
10. authority epoch — `epoch_stale`
11. permission — `permission_missing`, `permission_stale`, `permission_denied`
12. safety permit — `safety_permit_missing`, `safety_permit_stale`
13. interlock — `interlock_open`, `interlock_unknown`
14. evidence currency — `pressure_unknown`, `pressure_conflicted`,
    `pressure_violated`
15. obligations — `obligation_unknown`, `obligation_unsatisfied`
16. envelope and slew — `limit_unknown`, `limit_exceeded`

Step 2 deliberately precedes steps 8 and 9: a retry of an already accepted
attempt returns the prior accepted result rather than being refused because the
device moved on in the meantime. A retry that repeats the same request under the
same key replays; the same key with a different request is a conflict.

The trace of every check is returned with the decision, so a refusal always names
the precondition that produced it, and repeating an identical request against an
unchanged state returns an identical code and an identical message.

### Request classes, containment, and pressure

Every intent has exactly one class, decided by a pure function:

| class | intents | what it must prove |
|---|---|---|
| optimization | `lower_airflow`, `rebalance_rows`, `trim_for_efficiency` | containment intact everywhere in scope, every pressure relationship in the room satisfied, every protected obligation satisfied |
| protective | `hold_setpoint`, `raise_airflow`, `restore_pressure_relationship`, `release_to_policy` | nothing about containment; a named pressure relationship must be *known* for a restore |
| safety_directed | `emergency_purge` | an explicit, in-epoch, unexpired safety permit |

**Containment breach has deterministic precedence over an optimization request.**
The engine reduces every element that governs the device — same room, and either
room-scoped or the device's own row — to the worst state by an explicit safety
rank: breached outranks unknown, which outranks a deliberate service opening,
which outranks intact. That rank is a policy decision and is deliberately **not**
the numeric order of the enumeration. An optimization request against anything
other than intact is refused with the code for the worst state present, and the
choice is deterministic because ties break on the element identity.

A room with no declared containment element is **unknown**, not intact: absence
of a declaration is not evidence of containment integrity. A protective request
is not blocked by containment, because raising airflow may be exactly the remedy
for a breach and blocking it on unknown evidence could itself be unsafe.

**Unknown or stale pressure evidence fails closed for a transition that requires
proof.** A `restore_pressure_relationship` request names its relationship, and
that relationship must be known — satisfied or violated. Unknown is refused with
`pressure_unknown`; contradictory sensor evidence is refused with
`pressure_conflicted`. A protective request may act on a *violated*
relationship, because that is the reason to act.

Contradictory evidence is defined precisely: two fresh, good readings contradict
each other when they disagree about whether the band holds, or when they differ
by more than the band's declared tolerance. When fresh good readings agree, the
one closest to a band edge decides — the best case when the band holds, the
least-bad case when it does not.

### Authority, generations, and fencing

Every mutation states the authority it was planned against and refuses stale
authority rather than merging it:

* **device generation** — required on every control request; a reading, a grant,
  an override, or a permit bound to another generation does not apply;
* **evidence generation** — pressure evidence is stamped with the *relationship's*
  generation, metered airflow with the *obligation's*, and device readings with
  the *policy envelope's*; the three are published by different authorities and
  are never interchangeable;
* **state revision** — bumped by every accepted mutation; required for lifecycle
  and policy changes, optional but honoured for control commands;
* **authority epoch** — adopted explicitly, monotonic; adopting an older epoch is
  refused, because that would silently re-authorize withdrawn grants;
* **store generation** — the monotonic fence of the durable publication;
* **incarnation** — allocated and published on every successful open, so a writer
  that was thought to be dead can be told apart from its successor;
* **attempt id, command id, observation id, effect sequence, audit sequence** —
  five independent ordinals, never interchangeable.

Interlocks fail closed. A protected obligation that is open blocks control; one
that cannot be established at all blocks control as unknown; one that was
declared but never reported blocks control as unknown. A report from another
epoch is not current, and an out-of-order report is refused rather than allowed
to walk the obligation backwards. A safety-directed request is the one thing that
passes a protected interlock, and only with its own permit.

### Acknowledgement, effect, and idempotency

An attempt is a durable record. It is written, and published, **before** the
command leaves for the adapter. That record is the durable command-attempt
boundary: a process that dies immediately after dispatch leaves behind a record
that says a command may have reached the device, and recovery adopts it as
`recovery_required` without ever re-sending it. A device whose attempt has no
established effect refuses new control until the effect is established from fresh
evidence or the attempt is explicitly resolved.

The attempt record carries the desired setpoint, so the unresolved attempt *is*
the desired state; there is no second copy to drift. When an adapter states that
it refused, was unavailable, or faulted, the attempt is resolved immediately,
because the adapter has said something definite about the outcome.

Verification needs evidence that is **fresh** — age within
`options.evidence.max_age_ticks`, correct device generation, correct evidence
generation, quality good, not recovered from disk — and **bound** to the attempt:
measured at or after the instant the command was dispatched, and accepted strictly
after the attempt was accepted. A reading that was already in hand when the
command was issued is fresh but is not evidence *about* the command, and yields
`evidence_stale` rather than a verdict.

The delivery tolerance is explicit and defaults to zero, which is the strictest
answer; a deployment that knows its device settles sets
`setpoint_tolerance_basis_points` or `airflow_tolerance_cubic_metres_per_hour`
to a value it can defend.

### Supersession

An unresolved attempt is never silently overwritten. A newer command may take
over an unresolved attempt on the same device only when the caller names that
exact attempt, the device generation matches, and the new command either delivers
at least as much airflow as the incumbent setpoint or is a safety-directed
request carrying a valid permit. Anything else is `supersession_unsupported`.

A percent request and an airflow request are not ordered against each other,
because relating them needs the device's fan curve, which this runtime does not
own. That is a refusal, never an assumption.

### Idempotency retention

The idempotency window retains the most recently accepted attempts, bounded by
`EngineOptions::idempotency_window` (default 256, hard bound
`ModelBounds::max_idempotency_window`). A replay is a read of a retained result:
it re-actuates nothing, changes nothing, and publishes nothing. Refused requests
are journalled but not retained: a refused request is not an accepted attempt,
and re-sending it re-evaluates against current state.

The fingerprint covers everything the request says except its key. Eviction is
observable — a retry whose key has been evicted is a new request and is evaluated
as such. The attempt journal bound is raised to at least the window size, because
a retained key must always name an attempt the journal still holds, and evicting
an attempt evicts its retained key with it. The window is persisted, so a retry
after a restart replays the prior result instead of actuating again.

## Persistence and recovery

The store is a dual-slot, head-committed container with CRC-32C over every head
record and every payload, an explicit format version, and a documented byte
layout. The full description is in [docs/artifact-format.md](docs/artifact-format.md).

Publication: write the staging payload into the slot the adopted generation does
not occupy, flush, **then** write the head record that makes it reachable, and
flush. The commit point is that head write. Nothing before it is authoritative,
and the writer re-reads and re-verifies what it committed before claiming
durability.

Recovery adopts the valid head with the largest generation and verifies its
payload in full; generations are never stitched together. A head that verifies
but whose payload does not, or a file whose heads are both unreadable while the
head area holds bytes, is refused rather than repaired. If exactly one head is
readable and the other head area holds unverified bytes — which is what a torn
publication looks like — the previous whole generation is adopted and the store
reports `rollback_observed`; a caller that requires monotonic authority arms
`fence_min_generation` and gets `rollback_detected`.

Every structure in the file is bounded before it is allocated, counts are checked
against the bound that applies to them, text is re-validated, and a decode that
does not consume the payload exactly is refused. The decoder performs referential
integrity checks, so an internally inconsistent store is refused rather than
loaded and repaired.

Cross-process write authority is a real operating-system lock on a sidecar file,
held for the store's lifetime. The operating system releases it when the process
dies. Every publication re-reads both heads first and refuses to overwrite a
generation it never saw.

### What survives a restart, and what does not

Recovery never reissues a command, never promotes recovered telemetry to fresh
evidence, and never invents a verified state. The desired setpoint, the verified
effect, the attempt journal, the idempotency window, permissions, interlocks,
overrides, permits, containment state, pressure bands, obligations, lifecycle,
the authority epoch, the logical clock, and the audit ring all survive. Telemetry
comes back marked `recovered` and is stale until a fresh reading revalidates it.

## Concurrency and lock order

An engine instance is not internally synchronized: one instance is used by one
thread at a time. There is exactly one lock in this library — the operating-system
file lock of a durable store — and:

* it is acquired once, at open, and released at close;
* it is never reacquired on any call path, so there is no read-to-write upgrade,
  no re-entry through a callback or helper, and no nested acquisition;
* the engine holds no mutex at all, so no internal lock can be held across an
  adapter call, a filesystem operation, or a clock;
* it is deliberately held across adapter calls, because a second process must not
  interleave commands with this one, and no code reachable from an adapter can
  acquire it;
* a second opener is refused with `busy`, and the operating system releases the
  lock when the holder dies.

The library never reads the system clock. Time is a logical tick supplied by the
caller and control decisions are taken against logical ticks only. Every instant
supplied to the engine must not be later than the engine's logical clock, so the
sequence of an operation is always explicit; adopting an authority epoch is the
one exception, because a control plane cannot adopt its authority in the future —
adopting an epoch at an instant advances the clock to that instant.

## Canonical state and determinism

`AirflowControlEngine::canonical_state()` renders the authoritative model as
deterministic text and `state_digest()` digests it with FNV-1a 64. Two engines
driven through the same logical event sequence produce byte-identical text
regardless of when they ran, which process ran them, or how many times they
published. Excluded by construction: the engine incarnation, the store
generation, the audit ring, and the audit drop counter. Included: every identity,
generation, revision, lifecycle state, envelope, obligation, containment report,
pressure band and reading, permission, interlock, observation fact, attempt, and
idempotency entry.

The `recovered` flag on an observation *is* included, because it is a real fact
about where the reading came from. A restart therefore changes the canonical
state: the same logical event sequence replayed from scratch in a fresh store
produces identical text, while reopening an existing store marks its readings
recovered and says so.

## Safety posture

This library models decisions that would be safety-critical if connected to real
air-handling equipment, and it keeps that boundary explicit. It never claims
physical actuation because a command object was created or an adapter
acknowledged receipt. It contains no vendor credential, no network endpoint, and
no secret. It performs no autonomous external control outside the documented
adapter boundary. Containment, pressure, and interlocks fail closed. An adapter
cannot bypass core precondition validation: the command type is constructible only
from an engine-issued authorisation token, an outcome that does not echo the
command it answers is fenced, and an adapter is never invoked at all when a
precondition fails.

Every control result produced in this repository comes from a deterministic
synthetic adapter and is labeled **SYNTHETIC**. No data-center air-handling
equipment was used or available. The examples, the tool, the benchmark, and the
tests all say so in their output.

## Command line

    airflow-control init --store site.airflowstore
    airflow-control epoch adopt --store site.airflowstore --epoch 1 --actor operator-1 --tick 1
    airflow-control device add --store site.airflowstore --device dev-1 --generation 1 \
        --room room-1 --row row-1 --actor operator-1 --tick 1
    airflow-control grant add --store site.airflowstore --grant g1 --issuer airflow-authority \
        --epoch 1 --device dev-1 --device-generation 1 --room room-1 \
        --actions apply_setpoint,raise_airflow,lower_airflow,change_pressure_target,emergency_purge \
        --issued 1
    airflow-control lifecycle set --store site.airflowstore --device dev-1 --generation 1 \
        --revision 1 --target active --epoch 1 --actor operator-1 --tick 1
    airflow-control policy set --store site.airflowstore --device dev-1 --generation 1 --revision 2 \
        --policy pol-1 --policy-generation 1 --min-percent 2000 --max-percent 9000 \
        --default-percent 5000 --min-airflow 2000 --max-airflow 9000 --max-step 10000 \
        --source thermal-policy --evidence-generation 1 --actor operator-1 --tick 1
    airflow-control containment define --store site.airflowstore --containment cont-1 \
        --room room-1 --row row-1 --kind aisle_containment --actor operator-1 --tick 1
    airflow-control containment report --store site.airflowstore --containment cont-1 \
        --state intact --quality good --source dcim --sequence 1 --evidence-generation 1 --tick 1
    airflow-control tick advance --store site.airflowstore --tick 2
    airflow-control evaluate --store site.airflowstore --device dev-1 --device-generation 1 \
        --epoch 1 --intent raise_airflow --setpoint-percent 6000 --key k1 --actor operator-1 --tick 2
    airflow-control issue --store site.airflowstore --device dev-1 --device-generation 1 --epoch 1 \
        --intent raise_airflow --setpoint-percent 6000 --key k1 --actor operator-1 --tick 2 \
        --adapter synthetic
    airflow-control tick advance --store site.airflowstore --tick 3
    airflow-control verify --store site.airflowstore --attempt 1 --adapter synthetic --source field \
        --fan-sequence 10 --fan-point row-1-fan --tick 3
    airflow-control attempts --store site.airflowstore
    airflow-control state --store site.airflowstore
    airflow-control store-audit --store site.airflowstore

Verification is a separate invocation at a later instant, because the evidence
that establishes an effect must be accepted strictly after the command: an
`issue --verify` in one process would verify at the command's own instant and
honestly report the effect as indeterminate. Every invocation opens the store,
performs one verb, and closes, so the recovery path runs on every command. Exit code 0 is success, 1 is a refusal (with
`error: <token>: <message>`), and 2 is a usage error — a caller can tell "ask
again with a well-formed command" from "the request was understood and refused".
`--json` prints one compact JSON object instead; `--store` may be omitted to run
against an in-memory engine with no durability.

## Library use

```cpp
#include <airflow_control/engine.hpp>
#include <airflow_control/synthetic_adapter.hpp>

using namespace airflow_control;

EngineOptions options;                       // every bound is explicit
options.evidence.max_age_ticks = LogicalTick::from(1000);

auto opened = AirflowControlEngine::open("site.airflowstore", OpenMode::open_or_create, options);
if (!opened.ok()) { /* the status names the reason */ }
AirflowControlEngine& engine = opened.value();

ControlRequest request{
    .key = IdempotencyKey::parse("k1").value(),
    .device = AirflowDeviceId::parse("dev-1").value(),
    .device_generation = DeviceGeneration::from(1),
    .epoch = AuthorityEpoch::from(1),
    .intent = ControlIntent::raise_airflow,
    .setpoint = SetpointRequest{SetpointPercent{SetpointBasisPoints::create(6000).value()}},
    .actor = ActorId::parse("operator-1").value(),
    .requested_at = LogicalTick::from(2),
};

const Decision decision = engine.evaluate(request).value();
if (decision.eligible) {
  const AttemptRecord attempt = engine.issue(request, adapter).value();
  // The acknowledgement is an acknowledgement; the effect is whatever fresh,
  // bound evidence later establishes.
  const VerificationRequest verification{.attempt = attempt.id, /* ... */};
  const VerifiedEffect verified = engine.verify(verification).value();
  // verified.state is effective, contradicted, indeterminate, or unverified.
}
engine.close();
```

### The vendor adapter interface

```cpp
class AirflowAdapter {
 public:
  virtual AdapterDescriptor describe() const = 0;
  virtual AdapterOutcome execute(const AdapterCommand& command) = 0;
  virtual Result<ObservationDraft> read(const AdapterReadRequest& request) = 0;
};
```

That is the whole boundary. No vendor type, header, error code, protocol, or
transport appears in this library. An adapter receives an already-authorised
command and answers with a disposition plus, optionally, a draft reading. It
cannot fabricate a command, cannot widen the preconditions the engine validated,
cannot invent an observation identity or an acceptance instant, and cannot have
its answer attributed to a command it was not given.

## Package consumption

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
cmake --install build --config Release --prefix /some/prefix
```

The install exports `AirflowControl::airflow_control`, an
`AirflowControlConfig.cmake` package, the public headers, and the license files.
An out-of-tree consumer configures against it with
`find_package(AirflowControl 1.0 REQUIRED)`; the consumer in
`downstream/consumer` is exactly that, and it is built and run by the
`airflow_test_downstream_install` check.

## Examples

Five runnable programs, each an executable check rather than a demonstration;
each exits non-zero if its expectation does not hold.

| example | what it proves |
|---|---|
| `pressure_recovery` | a violated negative-pressure relationship is restored by a protective command, and a second attempt whose adapter acknowledges without moving the plant is contradicted and then blocks the device |
| `ack_without_effect` | an adapter that acknowledges and changes nothing: the acknowledgement is recorded as an acknowledgement, and fresh evidence then establishes the opposite condition |
| `containment_precedence` | a containment breach deterministically refuses an optimization request while a protective raise stays eligible and a safety-directed purge needs its permit |
| `stale_evidence` | a reading taken before the command cannot verify it, a fresh one can, and evidence that ages out fails closed |
| `crash_reopen` | a real process death inside the adapter call, after the durable command-attempt boundary: recovery adopts the attempt as `recovery_required`, a retry of the same key replays the durable record without reaching an adapter, and a new key is refused until the effect is established |

## Validation

Everything below was run in this repository, on this host. Nothing is aspirational,
and no number here was estimated.

**Build matrix (Windows 11, MSVC 19.44.35207, CMake 4.3.2, x64).**

| configuration | result |
|---|---|
| Release, `/W4 /WX /permissive- /utf-8 /Zc:__cplusplus` | clean, no warnings, library, CLI, five examples and the benchmark |
| Debug, same warnings | clean, no warnings |
| Release with AddressSanitizer (`/fsanitize=address /Zi`) | clean build |

**Test suite.** Nineteen CTest tests by default — fourteen library suites plus the
five example programs, which fail their exit status when an expectation does not
hold — and a twentieth when `AIRFLOW_CONTROL_DOWNSTREAM_PREFIX` points at an install
prefix. The fourteen library suites perform **7,090 checks across 89 cases**, and a
suite that performs no check at all is itself reported as a failure, so a suite
cannot pass vacuously. All of them pass in Release, in Debug, and in Release built
with AddressSanitizer. The suites cover identity and checked arithmetic; the lifecycle
transition table; the pressure band, containment precedence, obligation and
envelope rules; evidence freshness and binding; permission, interlock, override and
permit currency; control semantics including the deterministic refusal precedence,
acknowledgement versus effect, and no mutation on refusal; idempotency and its
bounded window; adapter fencing; persistence and corrupted-store refusal; crash
recovery; real multiprocess exclusion, process-death lock release and stale-writer
fencing; a seeded property drive with an independent reference model of the control
gate; adversarial input; and the command line tool end to end.

**Real process evidence.** The suites and examples start real child processes. A
child dies inside the adapter call, strictly after the durable command-attempt
boundary, and the parent proves that recovery adopts the attempt as
`recovery_required`, that a retry of the same key replays the durable record
without reaching an adapter, and that a new key is refused with
`attempt_unresolved`. Another child holds the store, proves the second opener is
refused with `busy`, and is terminated with the operating system's own call, after
which the lock is released and the store is still whole. A third forges a head
record out of band and proves the live writer is fenced with `store_fenced` rather
than allowed to overwrite a generation it never saw.

**What each configuration ran.** `ctest` reports `100% tests passed, 0 tests
failed out of 19` for Release, for Debug and for Release with AddressSanitizer, and
`out of 20` once the downstream consumer test is enabled.

```
suite                     checks  cases      suite                     checks  cases
airflow_test_identity       2223      8      airflow_test_persistence      301      7
airflow_test_property       1790      1      airflow_test_lifecycle        295      5
airflow_test_control         641      9      airflow_test_adapter          269     11
airflow_test_model           379     12      airflow_test_adversarial      190      5
airflow_test_idempotency     343      5      airflow_test_authority        187      6
airflow_test_multiprocess    155      5      airflow_test_evidence         139      7
airflow_test_recovery        137      5      airflow_test_cli               41      3
```

**Determinism, measured across processes.** Two independent processes were driven
through the same logical event sequence against two different durable stores. Their
`canonical_state()` output was byte-identical and their `state_digest()` values
matched exactly (`3e5c02cb8d53aba0`).

**Durable format, inspected byte by byte.** A store produced by the tool was read
back with a hex dump and checked against [docs/artifact-format.md](docs/artifact-format.md):
the file magic, format version, header size, both slot capacities, the all-zero
reserved regions, both markers, both head records with their generations, payload
lengths and incarnation, and the payload magic were all exactly as documented.
Adopting a generation below a caller-supplied minimum is refused with
`rollback_detected`.

**Install, export, downstream.** The package installs to a clean prefix and an
independent out-of-tree consumer configures against it with `find_package`, builds,
and runs a full lifecycle: register, authorise, observe, evaluate, issue, verify,
close, reopen. It reports `effect=effective` and confirms after reopening that the
attempt journal and the verified effect survived.

**Benchmark.** `airflow_bench_control` measures completed operations only. The timed
operation is one whole accepted control command through a durable store: request
validation, precondition evaluation, durable publication of the attempt record, the
adapter call, the durable outcome publication, verification against a fresh
synthetic observation, and the durable verification publication. The workload and
the confirmed retention configuration are printed with the result, and the state is
verified before the result is reported.

```
workload operations=200 devices=4 repetitions=5
configuration audit-capacity=256 idempotency-window=256 attempt-journal=512 slot-capacity-bytes=262144
label store=REAL adapter=SYNTHETIC (no hardware is driven)
run=0 completed=200 seconds=3.16824 operations-per-second=63.1266 publications=1000 state-verified=true
run=1 completed=200 seconds=2.61298 operations-per-second=76.5409 publications=1000 state-verified=true
run=2 completed=200 seconds=2.65026 operations-per-second=75.4642 publications=1000 state-verified=true
run=3 completed=200 seconds=2.99037 operations-per-second=66.8813 publications=1000 state-verified=true
run=4 completed=200 seconds=3.4579 operations-per-second=57.8386 publications=1000 state-verified=true
summary statistic=median-and-mean median-operations-per-second=66.8813 mean-operations-per-second=67.9703
```

The durable store path measured here is **REAL** file and device I/O. The adapter is
**SYNTHETIC**: it is a deterministic simulator that drives no hardware. Each
completed operation includes five publications — the attempt record at the
command-attempt boundary (which now also carries the idempotency key), the adapter
outcome, the read-back observation, the verification, and the verification's
observation — because each of those is a separate durable fact and none of them
belongs outside the measurement. No before/after comparison is published: the
methodology does not isolate a changed variable, so only the current measured path
is reported.

## Remaining limitations

* **No hardware validation.** No data-center air-handling unit, CRAH, CRAC,
  in-row fan, containment panel, or differential-pressure sensor was available,
  and none was used. Everything in this repository is verified against
  deterministic synthetic adapters. The vendor boundary, the fencing, and the
  persistence semantics are designed for real integrations, but no real
  integration has been exercised here. Nothing in this repository is hardware
  evidence.
* **The POSIX branch is unverified.** `src/detail/file_io.cpp` and
  `src/detail/path.cpp` contain a POSIX implementation of the file, lock, and
  path primitives, and the build selects it on non-Windows hosts. It is written to
  the same contract, but it could not be compiled or run in this environment, so
  it is documented as unverified rather than claimed as portable.
* **An engine instance is single-threaded.** There is no internal locking. Two
  threads sharing one engine instance is a programming error, not a supported
  configuration. Cross-process safety is real, via the store lock.
* **The store does not grow in place.** Slot capacity is fixed when the store is
  created (default 8 MiB, `StoreOptions::slot_capacity_bytes`). A model that no
  longer fits is refused with `store_capacity_exceeded` rather than silently
  truncated; the capacity is recorded in the file and reported by
  `store-audit`.
* **Rollback after head destruction is possible, and is the caller's to fence.**
  A head record that is present but unreadable is indistinguishable from a torn
  head write, so the store adopts the previous whole generation. A caller that
  needs monotonic authority must arm `min_generation`.
* **CRC-32C is corruption detection, not authentication.** An attacker with write
  access to a store can rewrite its contents and its checksums. Nothing in the
  file is secret, and the format is not a defence against that adversary.
* **A store whose header verifies but whose two head areas are entirely zero is
  adopted as an empty store.** That is the state of a store created but never
  published to, and it is also, indistinguishably, the state left by something that
  destroyed both head records while sparing the header. A single torn head is
  detected and rolled back; wholesale destruction of both heads is outside the
  corruption model, because an adversary who can rewrite two head records can also
  rewrite the header.
* **The slew bound applies to fan-percent setpoints only.** Relating an airflow
  request to a fan position needs the device's fan curve, which this runtime does
  not own; an airflow setpoint is bounded by the envelope alone.
* **The idempotency window is bounded and evicts.** A retry whose key has been
  evicted is a new request. The bound is configurable and its retention semantics
  are documented above; there is no unbounded history.
* **The audit ring is bounded and drops.** The oldest entries are dropped when the
  ring is full, and the drop count is reported rather than hidden.
* **A policy engine is not present, by design.** This runtime cannot tell you
  whether a rack *should* receive more air. It can only tell you whether the
  authority, evidence, containment, obligations, envelope, and interlocks you
  supplied permit the transition, and whether the effect was established.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.
