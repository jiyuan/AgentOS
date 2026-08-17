# Token calibration

What `agentos_core::prompt::tokens` estimated, beside what the provider actually counted. Regenerate with `agentos-gateway calibrate` (live call) or re-check the estimator against these same numbers offline with `agentos-gateway calibrate --check`.

<!-- BEGIN GENERATED CALIBRATION -->
Recorded 2026-08-17 against `gpt-5.4-mini` on `openai`.

**2 of 8 cases within ±15%.** Median absolute error 20.2%, mean signed error +8.5%. Worst over-estimate +38.0%, worst under-estimate -28.6%.

| Case | Chars | Estimated | Actual | Error |
|---|---:|---:|---:|---:|
| `minimal` | 4 | 5 | 7 | -28.6% |
| `english_prose` | 636 | 163 | 134 | +21.6% |
| `chinese_prose` | 201 | 185 | 140 | +32.1% |
| `mixed_scripts` | 175 | 95 | 82 | +15.9% |
| `code_block` | 428 | 112 | 138 | -18.8% |
| `multi_turn` | 197 | 81 | 76 | +6.6% |
| `tool_schemas` | 66 | 236 | 171 | +38.0% |
| `tool_round_trip` | 315 | 238 | 236 | +0.8% |

## What each case is

- **`minimal`** — One short user turn. Measures the provider's fixed per-request overhead, which every other case's error should be read against.
- **`english_prose`** — English, the register the 4:1 rule was derived from.
- **`chinese_prose`** — Chinese. A 4:1 divisor under-counts this by about four times, which is why the estimator counts wide characters one for one.
- **`mixed_scripts`** — Chinese with English identifiers embedded. This deployment's normal case.
- **`code_block`** — Symbol-dense ASCII. Code tokenizes worse than 4:1, so this is where the estimate is most likely to fall below the truth.
- **`multi_turn`** — A system turn plus four conversational turns. Isolates the per-message overhead constant, which a single-message case cannot separate from the provider's fixed cost.
- **`tool_schemas`** — A short turn plus two tool schemas. Schemas carry no messages but occupy the same window, and on a small request they outweigh the conversation.
- **`tool_round_trip`** — An assistant tool-call turn and its result. The shape that grows a transcript fastest, and the one whose cost is easiest to miss because the assistant message has almost no text.
<!-- END GENERATED CALIBRATION -->
