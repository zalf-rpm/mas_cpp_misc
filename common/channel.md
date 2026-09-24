# The FBP channel

`channel.cpp` implements the buffered channel that carries IPs between FBP components:
writers put messages in, readers take them out, the buffer bounds how far ahead the writers
may run. This file records what went wrong in it, and which properties have to keep holding.

## Two bugs that were expensive to find

### A waiter was identified by the address of its fulfiller

Blocked readers and writers wait in `blockingReadFulfillers` / `blockingWriteFulfillers`. Each
had a `kj::defer` guard to take it back out of the queue when its call was canceled, and the
guard looked the entry up **by the address of its `PromiseFulfiller`**.

The trap: `kj::Promise::attach()` destroys its attachment when the promise **resolves**, not
only when it is canceled, so the guard also ran on the success path - looking for an address
whose fulfiller had already been fulfilled, popped and freed. With three or more writers on one
channel the allocator handed that heap block straight back to the next writer that blocked, so
the stale guard erased a *different*, still waiting writer. Its promise failed with
`PromiseFulfiller was destroyed without fulfilling the promise` and its message was gone, while
the reader downstream waited for it forever.

**The rule:** never use a pointer as an identity for something that outlives one turn of the
event loop. Waiters now carry a monotonically increasing `WaiterId` that is never reused.

### A read is a destructive take

`read()` moves the message out of the channel into the read call and forgets it. From then on
the message exists *only* inside that call, so a caller which cancels it or drops its response
destroys the message - silently, because the writer was told long ago that its write succeeded
and the channel cannot tell a response that was used from one that was thrown away.

This bit in `WrapIntoSubstream`: it raced a read on `in` against one on `brackets`, and when the
close-bracket won it threw the pending `in` read away. One IP short per substream, no error
anywhere, and the component then waited forever for an IP that no longer existed.

`readLeased()` is the answer on this side: the message stays owed to the channel until the lease
is acknowledged, and releasing the lease capability without an ack puts it back. Cap'n Proto's
reference counting reports that release whether the call was canceled, the process died or the
connection broke, which is exactly the signal the plain protocol lacks. Clients should still not
throw reads away - the lease costs a redelivery, avoiding the cancellation costs nothing.

## Properties that have to keep holding

- **Once `write()` returns, the message belongs to the channel.** `deliverOrBuffer` copies it out
  of the writer's request for that reason. Handing a waiting reader a view into the live request
  saves the copy and does work today (kj arms a fulfilled promise depth-first, so the reader
  copies it out before the write call completes), but it fails as a use-after-free rather than as
  an error the moment anything asynchronous slips in between. See the comment there before
  changing it.
- **A blocked reader implies an empty buffer, a blocked writer implies no free slot.** Readers
  only ever block when there is nothing to take; writers only when there is nowhere to put. If
  both were possible at once, a message could sit in the buffer while a reader waits for it.
- **An unblocked writer holds its buffer slot until it has written.** `reservedBufferSlots`
  counts those slots; without it the same slot is handed out twice. The slot is released by the
  writer's cancellation guard, which is also where the next writer gets woken - so that path runs
  on the success case too, by design.
- **`done` means no message will ever come.** It therefore waits for the buffer to be empty *and*
  for outstanding leases, because a leased message may still return. A reader told `done` never
  asks again.
- **The buffer limit is soft in two places:** a message returning from an unacknowledged lease,
  and one held back by a gating observer, may put the buffer one over its size. Losing a message
  to a bookkeeping limit would be worse.

## Observation, pause and step

Two distinct hook points, worth keeping apart:

- **accept** - the channel takes a message from a writer. Observation happens here, so every
  message is reported exactly once no matter how often it is handed out afterwards; a message
  returning from a lease is not reported twice.
- **deliver** - the channel hands a message to a reader. `pause`, `resume`, `step` and gating
  observers act here.

A best effort observer is never waited for and is skipped while a previous call is still on its
way. A gating observer has to answer before delivery, which is how a debugger holds a flow: keep
the call open and the message stays put. Gating is currently **global** rather than per message -
while any gate is open nothing is delivered, so a message whose own observer already answered
still waits behind a later one. Stricter than necessary, but it can never deliver a message its
observer has not seen.

Pausing gates delivery only. Writers keep filling the buffer and block once it is full, so a
paused channel back-pressures its upstream without any extra machinery.

## Tests

`channel-test.cpp` drives the real channel through in-process capnp clients, no networking. Local
calls make the interleavings reproducible instead of timing dependent: the server method runs when
the event loop turns, and `waitScope.poll()` lets queued continuations run, so a test can place a
cancellation or a second read exactly where it wants it.

```
ninja channel-test && ./common/channel-test          # all
./common/channel-test concurrentWritersLoseNoMessages # one
CHANNEL_TEST_VERBOSE=1 ./common/channel-test <name>   # with the channel's own INFO trace
```

Each test has a watchdog: a test that waits forever is reported as a failure rather than hanging,
because waiting forever is precisely the failure mode this file is about.

## The other half lives in the Python runtime

`zalfmas_fbp` (repo `mas_python_fbp`) reads input ports through `readLeased` and acknowledges as
soon as the IP reaches the component, and its runtime never cancels a read it has started. Both
matter: the channel can only give a message back if the reader does not first throw it away.
