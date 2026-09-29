# Airflow Control durable artifact format

This document describes the byte layout of the one durable artifact this runtime
writes: the **store**. It is a complete description. A reader that implements
this document and the payload section below can read a store without this
library, and a writer that implements it can produce one this library accepts.

Everything is little-endian. Every integer field is unsigned unless stated
otherwise. No field is length-prefixed unless it is described as variable.

## Container

    offset 0                      512-byte file header
    offset 512                    slot 0     (slot_capacity_bytes long)
    offset 512 + slot_capacity    slot 1     (slot_capacity_bytes long)

A slot begins with a 128-byte head record; the generation's payload follows it
immediately. `slot_capacity_bytes` is fixed when the store is created and is
recorded in the file header. The file is exactly `512 + 2 * slot_capacity_bytes`
bytes; it never grows in place.

### File header (512 bytes)

| offset | size | field | value |
|---|---|---|---|
| 0 | 8 | magic | the ASCII bytes `AFCTLST1` |
| 8 | 4 | format_version | `1` |
| 12 | 4 | header_bytes | `512` |
| 16 | 8 | slot_capacity_bytes | bytes reserved per slot |
| 24 | 8 | payload_capacity_bytes | `slot_capacity_bytes - 128` |
| 32 | 8 | creation_store_serial | diagnostic; written as `0` |
| 40 | 464 | reserved | all zero |
| 504 | 4 | header_crc32c | CRC-32C over bytes `[0, 504)` |
| 508 | 4 | header_marker | `0x41464331` |

A header whose magic, marker, CRC, version, reserved region, or stated
capacities do not hold exactly is refused. The file is **not** repaired.

### Head record (128 bytes, at the start of each slot)

| offset | size | field | value |
|---|---|---|---|
| 0 | 8 | magic | the ASCII bytes `AFCLHEAD` |
| 8 | 8 | generation | the monotonic publication counter, starting at 1 |
| 16 | 8 | payload_length | bytes of payload in this slot |
| 24 | 4 | payload_crc32c | CRC-32C over the payload |
| 28 | 4 | format_version | `1` |
| 32 | 8 | writer_incarnation | the engine incarnation that published it |
| 40 | 8 | writer_process_id | the operating-system process that published it |
| 48 | 72 | reserved | all zero |
| 120 | 4 | head_crc32c | CRC-32C over bytes `[0, 120)` |
| 124 | 4 | head_marker | `0x41464831` |

The payload is exactly `payload_length` bytes at `slot_offset + 128`. Bytes
beyond `payload_length` inside the slot are not part of the generation, are
never read, and are not required to be zero.

## Publication and the commit point

Publishing generation *N+1* into the slot the adopted generation does **not**
occupy proceeds in this order:

1. write the payload bytes;
2. flush to stable storage;
3. write the 128-byte head record, which is what makes the payload reachable;
4. flush to stable storage.

**The commit point is the head write.** Nothing before it is authoritative: a
process that dies during steps 1–2 leaves the previous generation reachable and
the staging slot holding bytes no head describes.

A head that is half-written fails its CRC, so a reader treats it as absent. A
head is valid only when its magic, marker, version, both CRCs, the reserved
region, the capacity bound, and the payload verification all hold.

The writer then re-reads the head it just wrote and re-verifies the payload
against the publication buffer. Durability is claimed only after that read-back.

## Adoption and recovery

At open, the store reads both heads, discards any head that does not verify,
verifies the payload of every head that does, and **adopts the valid head with
the largest generation**. Generations are never stitched together: a generation
is adopted whole or not at all. A head that verifies but whose payload does not
is corruption, and the store is refused rather than repaired.

* Both heads unreadable and the head area non-zero → `store_corrupt`.
* One head readable and the other head area non-zero but unverified → the store
  adopts the previous whole generation and reports `rollback_observed`. This is
  what a torn publication looks like.
* A valid file header with **both** head areas entirely zero is adopted as an
  empty store at generation 0. That is both the state of a store that was created
  and never published to, and the state left by something that destroyed both head
  records while leaving the header intact. The format cannot tell those apart, and
  it does not try: an adversary who can rewrite two head records can also rewrite
  the header, and CRC-32C is corruption detection rather than authentication. This
  is a real and documented limitation, not a silent one.
* A store whose adopted generation is below a caller-supplied
  `StoreOptions::min_generation` with `fence_min_generation` set is refused with
  `rollback_detected`.

A newly created store has no valid head; its generation is 0 until the first
publication. A crash during creation leaves a 512-byte file with no valid head,
which still opens as an empty store rather than as a missing one.

## Write authority

Cross-process exclusion is a real operating-system lock on the sidecar file
`<canonical-store-path>.lock`, taken before the store file is opened or created
and held for the store's lifetime. The operating system releases it when the
holding process dies. A second opener is refused with `busy`.

Every publication re-reads both heads first. A head that advanced beyond the
generation this writer adopted means another writer touched the file, and the
publication is refused with `store_fenced` rather than allowed to overwrite a
generation this writer never saw.

The path recorded in the lock name is the canonical path: absolute, with every
existing component resolved through reparse points, and with a final component
that is not a directory. Two processes naming the same logical store through
different spellings — mixed separators, embedded relative components, different
letter case — therefore derive the same lock path.

## Payload

The payload is the canonical encoding of the model. Its first two fields are a
32-bit magic `0x41464D31` and a 32-bit format version `1`; the remainder is a
fixed sequence of fields in this order:

    epoch                     u64
    tick                      u64
    next_incarnation          u64
    next_command              u64
    next_attempt              u64
    next_observation          u64
    next_effect               u64
    next_audit                u64
    idempotency_window        u64
    attempt_journal_capacity  u64
    audit_capacity            u64
    audit_dropped             u64
    devices                   collection
    relationships             collection
    containment               collection
    obligations               collection
    interlocks                collection
    grants                    collection
    overrides                 collection
    permits                   collection
    attempts                  collection
    idempotency               collection
    audit                     collection

A collection is a 32-bit element count followed by that many elements. Every
collection is written in a canonical order — ascending by the element's identity
— so the same model always produces the same bytes regardless of the order the
engine happened to learn about its objects. A decoder requires that order and
refuses a payload whose elements are out of order (`store_corrupt`) or repeated
(`duplicate_identity`).

Text and identifiers are written as a 32-bit byte length followed by that many
bytes with no terminator. Identifiers are re-validated on decode against the same
character rules the API enforces; text is re-validated as strict UTF-8 with no
control characters.

An optional field is a presence byte — `0` or `1`, and nothing else — followed by
the value when present.

Enum values are written as 32-bit integers. **The numeric values of the
enumerations are part of format version 1.** A value outside the declared set is
refused as `store_corrupt` rather than mapped to a default.

### Bounds and consistency

Every count is checked against the structural bound that applies to it before
anything is allocated, and the decoder refuses a payload that declares more
elements than the bound permits. The decoder also verifies:

* identity counters are ahead of every identity the payload uses;
* every observation belongs to the device and device generation it is stored
  under, and a device slot holds the observation kind it is named for;
* every relationship's evidence names that relationship and is a pressure
  reading;
* every attempt names a device the payload holds and was accepted at or before
  the stored clock;
* every idempotency slot names an attempt the journal still holds;
* every grant, override, permit, and interlock names a device that exists;
* every obligation's bound devices exist, a metered obligation names its point,
  and a device-sum obligation binds at least one device;
* retention capacities are consistent with the records actually carried;
* the payload is consumed exactly — trailing bytes are refused.

A payload that fails any check is refused. It is never partially adopted, never
repaired, and never rewritten.

## Integrity, not authentication

CRC-32C detects accidental corruption. It is not a message authentication code.
An attacker with write access to a store can rewrite its contents and its
checksums. Nothing in the file is secret, and this format is not a defence
against that adversary.
