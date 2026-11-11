# Collie Whistle agent notes

- `docs/SPEC.md` is the scope. The plan for the whole suite is `docs/SPEC.md` in `0cwa/collie-pack`.
- No dependency on CollieSocket, Signal, Tauri or Collie Pack. Payloads are opaque: never parse or decrypt them.
- Protocol behaviour is defined by recorded calls from real clients. Run the same fixtures against stock ntfy as well as this server, so they can't drift from the real thing.
- Keep the scope small. Features ntfy has but UnifiedPush clients don't call stay out.
- A test run that collected 0 tests failed.
