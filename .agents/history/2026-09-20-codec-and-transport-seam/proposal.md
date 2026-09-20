# `snapshot.Codec` — design note

Status: proposal, not implemented. Working file, not committed.

## The problem

`RepositoryConfig` asks for `Marshal`, `Unmarshal` and `Upcasters` as three
separate fields, and the store calls `Marshal` on write and `Unmarshal` on read
with the row's bytes passed straight through (`snapshot.go:442`, `snapshot.go:308`).

That is fine while the payload is JSON, but it leaves **no seam between "the bytes
in the row" and "the state"**. Anything that has to happen *to the bytes* before
they are parsed has nowhere to live.

The important such thing is encryption of the payload, which this project's own
guidance tells people to do as early as possible. Today the only place to put it is
inside `Marshal`/`Unmarshal` — and that collides with upcasting, because
`upcastPayload` (`snapshot.go:331`) receives the row's bytes and would therefore
receive ciphertext.

## Proposal

Replace the two encoding fields with one interface:

```go
// Codec converts a state value to and from its payload bytes.
type Codec[T any] interface {
	// Encode returns the payload for state.
	Encode(state T) ([]byte, error)

	// Decode returns the state described by payload. streamID is the stream the
	// caller asked for, so an implementation can reject a payload that belongs to
	// a different one.
	Decode(streamID string, payload []byte) (T, error)
}

// JSON returns the default codec, backed by encoding/json.
func JSON[T any]() Codec[T]
```

`RepositoryConfig` loses `Marshal` and `Unmarshal` and gains `Codec Codec[T]`.
`Upcasters` keeps its current shape and its current position in the read path.

Why this shape:

- `Marshal` and `Unmarshal` are two halves of one concern. They are always
  supplied together, they are validated together (the nil checks at
  `snapshot.go:174-183`), and neither is ever used without the other.
- `JSON[T]()` reproduces today's behaviour exactly, including passing the
  `streamID` through to the decoder, which is what lets a caller reject a foreign
  payload.
- It shrinks the configuration surface, which is the main cost of adopting this
  component — and it shrinks it by removing a field rather than adding one.
- It gives a team one obvious place to put a codec that is not JSON, instead of
  two function literals at every wiring site.

## Where `Migrate` must not go

Do **not** put upcasting on the codec. An upcaster transforms *plaintext at a known
schema version*; a codec converts a state value. If migration moved onto the codec,
then an encrypting codec would have to decrypt inside `Migrate`, upcast, and
re-encrypt the result — feasible, but it buries the ordering inside every codec
implementation and makes correctness the codec author's problem.

Keeping them apart yields one pipeline with a stated order:

```
write:  T  --Codec.Encode-->  plaintext @ current schema
           --Transport.Apply-->  stored bytes

read:   stored bytes  --Transport.Restore-->  plaintext @ row schema
                      --Upcasters(row .. current)-->  plaintext @ current schema
                      --Codec.Decode-->  T
```

Two properties fall out of that and both are worth writing down as contract:

1. **Upcasters always run on plaintext.** They never see ciphertext or a
   compressed frame.
2. **The transport is outermost.** Both the codec and the upcasters live inside
   the encryption boundary.

## The adjacent decision: a transport seam

The codec change alone does not finish the encryption story, because there is still
no seam for step two of each pipeline. `Put` writes whatever `Encode` returned and
`Decode` receives the row's bytes, so a crypto step still has to be smuggled into
`Encode`/`Decode` — the very collision described above.

So there is a second, smaller question: where does the transport transform live.

- **(a) A transformer in the config**, applied to row bytes on both paths, with the
  codec and the upcasters inside it. One ordered list, so encryption and
  compression compose: `[]Transformer{Gzip(), AESGCM(key)}`.
- **(b) The codec owns its transport**, e.g. `snapshot.Encrypted(snapshot.JSON[T](), key)`.
  Smaller config, but the codec's payload is then no longer plaintext, which
  contradicts rule 1 above unless the ordering is documented per implementation.

Preference: **(a)**, for the ordering reason. It keeps every concern single-purpose
and it makes the encrypt-then-upcast question unanswerable wrongly, because the
pipeline has one place to be written down.

Whether to do this in the same change is a judgement call. The argument for doing it
together is that the ordering is a *contract*: shipping `Codec` without a transport
seam invites someone to put crypto in `Encode`, which is exactly what this note
exists to prevent.

## What this does not solve

The state shape itself. Whether the domain exposes a memento, a set of accessors, or
marshals itself is untouched by this note. The `Codec` seam only decides who owns the
bytes, not who owns the shape.

## Compatibility and implementation

- `JSON[T]()` must reproduce today's behaviour exactly. Existing callers migrate by
  deleting two fields and adding one.
- Construction validation becomes one nil check on `Codec` instead of two.
- `Upcasters` is unchanged: `map[int]Upcaster`, applied in `upcastPayload`.
- The `streamID` argument on `Decode` stays. `Unmarshal(streamID, data)` was added
  deliberately so a caller can reject a payload belonging to a different stream, and
  implementations rely on it. If more context is ever needed — the stream type, the
  row's schema version, a payload header — the escape hatch is a request struct
  rather than another parameter, so that decision does not have to be made now.
- Snapshot rows are derived data, so a codec change needs no data migration. At worst
  an old payload becomes unreadable, which falls back to a replay.

## Open questions

1. Should `Decode` also receive the row's `SchemaVersion`, so a codec can branch on
   it, or should that stay the upcasters' exclusive concern?
2. Transport seam in the same change, or as a follow-up? (See above; I lean toward
   the same change, for the ordering.)
3. Is `JSON[T]()` sufficient as the default, or is a common case worth special-casing
   — for instance a state type that implements its own `json.Marshaler`?
