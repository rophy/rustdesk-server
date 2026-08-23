# Repository Instructions

## Rust Rules

- In Rust code, do not introduce `unwrap()` or `expect()`.
- Allowed exceptions:
- Tests may use `unwrap()` or `expect()` when it keeps the test focused and readable.
- Lock acquisition may use `unwrap()` only when the locking API makes that the practical option and the failure mode is poison handling rather than normal control flow.
- Outside those exceptions, propagate errors, handle them explicitly, or use safer fallbacks instead of `unwrap()` and `expect()`.

## Testing

- `cargo test` runs all tests.
- `cargo test --lib` runs unit tests only (in `src/`).
- `cargo test --tests` runs integration tests only (in `tests/`).
- Integration tests spawn local hbbs/hbbr processes. On Unix, cleanup uses
  `libc::atexit` (SIGTERM on exit) and `PR_SET_PDEATHSIG` (SIGTERM if parent dies).

## Editing Hygiene

- Do not introduce formatting-only changes.
- Do not run repository-wide formatters or reflow unrelated code unless the
  user explicitly asks for formatting.
- Keep diffs limited to semantic changes required for the task.

