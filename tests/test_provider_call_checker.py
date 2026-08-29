from pathlib import Path
import importlib.util


SCRIPT = Path(__file__).parents[1] / "scripts" / "check_provider_calls.py"
SPEC = importlib.util.spec_from_file_location("provider_checker", SCRIPT)
CHECKER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECKER)


# AF-036: syntactic aliases and split call expressions remain rejected.
def test_syntactic_bypasses_are_rejected():
    source = r'''
        // llm.complete_messages(&messages, tools)
        let example = ".complete_messages(";
        llm
            . complete_messages
            (&messages, tools)
            .await?;
        <dyn Llm as Llm>
            ::complete_messages_stream
            (llm, messages, tools);
    '''
    found = CHECKER.direct_calls(source)
    assert [name for _, name in found] == [
        "complete_messages",
        "complete_messages_stream",
    ]


def main() -> int:
    """Run every test in this module without requiring pytest.

    `scripts/check-provider-calls.sh` calls this, so the AF-036 regression runs
    on every required CI job. Discovery is by prefix rather than a hand-written
    call list: the previous revision named one function here and nothing ran the
    file at all, so a second test would have been collected by a pytest nobody
    installs and executed by nothing.
    """
    tests = sorted(
        (name, value)
        for name, value in globals().items()
        if name.startswith("test_") and callable(value)
    )
    if not tests:
        print("FAIL: no tests were discovered in this module")
        return 1

    failures = 0
    for name, test in tests:
        try:
            test()
        except Exception as error:  # noqa: BLE001 - report, do not mask
            failures += 1
            print(f"FAIL: {name}: {error!r}")
        else:
            print(f"ok: {name}")

    if failures:
        print(f"provider-call checker regressions FAILED ({failures} of {len(tests)})")
        return 1
    print(f"provider-call checker regressions ok: {len(tests)} test(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
