# Debugging Rust in VS Code

## How It Works

Cargo's development profile compiles native machine code with debug information.
That information maps machine instructions back to Rust files, line numbers,
functions, and many local variables.

The tools have separate jobs:

- Rust Analyzer understands Rust source and provides editor actions.
- Cargo compiles the selected binary, example, or test with debug information.
- LLDB controls the compiled process.
- CodeLLDB connects VS Code's debug controls to LLDB and formats Rust values.

A breakpoint asks LLDB to pause when execution reaches a source location. While
paused, VS Code can show local variables, evaluate expressions, display the call
stack, and step through instructions associated with source lines.

Debug builds are intentionally preferred. Optimized release builds may inline
functions, reorder instructions, and remove variables, which makes source-level
stepping confusing.

## Starting a Session

Open the `lightcdc` directory itself as the VS Code workspace. Open **Run and
Debug**, choose a configuration, and press `F5`.

Available project configurations:

- `Debug lightcdc: CDC + consumer` starts PostgreSQL, capture, the gRPC server,
  and the example consumer as one compound debugging session. The consumer runs
  the demo orders SQL after subscribing so both sides receive activity.
- `Debug lightcdc: inspect redb` opens the existing redb store.
- `Debug lightcdc: capture` starts PostgreSQL capture.
- `Debug lightcdc: run capture + gRPC` starts the combined runtime.
- `Debug lightcdc: example consumer + demo events` waits for port 50051,
  connects the example consumer, and runs the demo orders SQL.
- `Debug storage inspection test` runs one storage unit test.
- `Attach to a running Rust process` attaches to a process started elsewhere.

The compound configuration starts PostgreSQL automatically. For the individual
capture configurations, start it with **Terminal > Run Task > lightcdc: start
PostgreSQL** first. Stop other LightCDC processes before debugging `inspect`
because redb permits only one process to open the database file.

Rust Analyzer also places **Run | Debug** links above unit tests. Selecting
**Debug** there is the easiest way to debug whichever test is beside the cursor.

## Basic Controls

- Click beside a line number to set or remove a breakpoint.
- `F5` continues to the next breakpoint.
- `F10` steps over the current line.
- `F11` steps into a called function.
- `Shift+F11` steps out of the current function.
- `Shift+F5` stops the process.

Use the **Variables** panel for locals, **Watch** for expressions that should be
reevaluated each time execution pauses, and **Call Stack** to see how execution
reached the current function.

Right-clicking a breakpoint allows a condition or log message. A logpoint prints
information without stopping the process, which is useful inside event loops.

## Useful LightCDC Breakpoints

- `capture_with_store` shows the capture and durability loop.
- `ReplicationReader::next_transaction` shows transaction buffering and commit boundaries.
- `PgOutputDecoder::decode` shows raw pgoutput message handling.
- `RedbEventStore::persist_transaction` shows the atomic event and source-LSN write.
- `LightCdcService::subscribe` shows creation of a consumer stream.
- The spawned loop inside `subscribe` shows replay, filtering, and delivery.

## Async Rust

An async function becomes a state machine that Tokio polls. Many async tasks can
share a small number of operating-system threads, so a Tokio worker thread is
not the same thing as one logical request or consumer.

Stepping into `.await` may enter Tokio, Future, or generated state-machine code.
When that happens, continue to a breakpoint in the next LightCDC function
instead of stepping through runtime internals. Breakpoints and logpoints around
state changes are usually clearer than instruction-by-instruction stepping in an
async loop.

The debugger shows runtime values. Ownership, borrowing, and lifetime failures
are compile-time checks, so those are normally investigated through compiler
errors and Rust Analyzer rather than LLDB.
