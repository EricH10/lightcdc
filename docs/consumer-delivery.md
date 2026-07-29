# Consumer Delivery Contract

LightCDC currently gives each `(stream, consumer)` pair one ordered, durable
cursor. The consumer name is chosen by the downstream application or its
operator and should remain stable across restarts.

## Current Model

- Only one active subscription may use a `(stream, consumer)` pair at a time.
- Events are delivered in increasing local sequence order.
- `Ack N` is cumulative: it means every event from that stream through sequence
  `N` has finished successfully.
- An acknowledgement is accepted only when `N` is no greater than the highest
  sequence LightCDC has made available to that stream and consumer.
- A consumer cannot acknowledge before LightCDC has delivered at least one
  event to it.
- A consumer should process one event completely, acknowledge it, and then move
  on. It must not fan events out for out-of-order processing under one consumer
  identity.
- Disconnecting before an acknowledgement leaves the durable cursor unchanged,
  so reconnecting with the same consumer name redelivers the event.
- Delivery is at least once. A crash can therefore cause duplicates, and
  downstream processing should be idempotent.
- A consumer that intentionally does not need missed events can seek to
  `latest` before subscribing.
- Retention is a hard boundary and is not pinned by consumer offsets. A
  consumer that falls behind receives an expired-offset error and must
  explicitly seek to `earliest` or `latest`.
- `earliest` means the oldest event payload that is still retained, not
  necessarily sequence 1.
- Delivery history is retained across ordinary reconnects, so a delayed
  acknowledgement from an earlier connection remains valid. An explicit seek
  clears that history and requires a new delivery before another
  acknowledgement.

This deliberately simple model resembles a classic Kafka partition cursor. It
keeps acknowledgement meaning unambiguous while LightCDC's capture and recovery
path is still being hardened.

## Future Parallel Worker Model

Parallel workers sharing one logical consumer require a different protocol.
The planned direction follows Sequin's approach: every delivered message gets
an opaque acknowledgement ID and a time-limited lease, with explicit
acknowledge, negative-acknowledge or release, a visibility timeout, and a
maximum in-flight count.

That protocol lets one worker fail without moving a cumulative cursor past
unfinished work. It will be added as a separate shared-worker mode rather than
quietly changing the meaning of the current `Ack` request.
