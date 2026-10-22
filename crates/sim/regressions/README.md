# Regression seeds

When `rkv-sim --save-regressions crates/sim/regressions` finds a failing seed on the REAL (unmutated) code it writes
`<profile>-<seed>.json` here. `cargo test -p sim --test regressions` replays every file and requires zero violations,
so a fixed bug stays fixed. Files are never deleted. Empty so far: no unmutated-code failure has been found by the simulator
(see docs/BUGS_FOUND.md for the bugs found by other means).
