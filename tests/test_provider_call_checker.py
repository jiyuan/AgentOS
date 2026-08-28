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


if __name__ == "__main__":
    test_syntactic_bypasses_are_rejected()
