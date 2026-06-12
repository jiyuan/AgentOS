//! Hand-written parser for the policy YAML subset used by `agent.toml`
//! policy files. Split out of `approve/mod.rs` (roadmap Phase 4.1) as pure
//! code motion.
//!
//! Deliberately hand-written rather than a `serde_yaml` dependency: the
//! grammar is a small fixed subset (rule list + `default:` + per-rule
//! fields + `args:` matchers) and keeping the parser local avoids a
//! supply-chain dependency in the authorization layer.

use super::{Policy, PolicyAction, PolicyError, PolicyRule, PolicyVerb};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn parse_policy_yaml(input: &str) -> Result<Policy, PolicyError> {
    let mut policy = Policy::default();
    let mut current_rule: Option<PolicyRule> = None;
    let mut in_args = false;

    for (index, raw_line) in input.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = strip_comment(raw_line).trim();
        if trimmed.is_empty() || trimmed == "rules:" {
            continue;
        }

        if let Some(value) = trimmed.strip_prefix("default:") {
            policy.default_decision = parse_verb(value.trim(), line_number)?;
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("- ") {
            if let Some(rule) = current_rule.take() {
                policy.rules.push(rule);
            }
            let mut rule = PolicyRule {
                action: PolicyAction::Any,
                decision: PolicyVerb::Deny,
                reason: None,
                arg_equals: BTreeMap::new(),
            };
            in_args = false;
            apply_rule_field(&mut rule, rest, line_number, &mut in_args)?;
            current_rule = Some(rule);
            continue;
        }

        let Some(rule) = current_rule.as_mut() else {
            return Err(invalid_yaml(line_number, "field is outside of a rule"));
        };

        if in_args && !trimmed.contains(':') {
            return Err(invalid_yaml(line_number, "argument matcher is missing ':'"));
        }
        apply_rule_field(rule, trimmed, line_number, &mut in_args)?;
    }

    if let Some(rule) = current_rule.take() {
        policy.rules.push(rule);
    }

    Ok(policy)
}

fn apply_rule_field(
    rule: &mut PolicyRule,
    field: &str,
    line_number: usize,
    in_args: &mut bool,
) -> Result<(), PolicyError> {
    if field == "args:" {
        *in_args = true;
        return Ok(());
    }

    let (key, value) = field
        .split_once(':')
        .ok_or_else(|| invalid_yaml(line_number, "rule field is missing ':'"))?;
    let key = key.trim();
    let value = value.trim();

    if *in_args && !matches!(key, "tool" | "action" | "decision" | "reason") {
        rule.arg_equals.insert(Arc::from(key), parse_scalar(value));
        return Ok(());
    }
    *in_args = false;

    match key {
        "tool" => {
            rule.action = PolicyAction::Tool(unquote(value));
            Ok(())
        }
        "action" => {
            rule.action = match unquote(value).as_ref() {
                "any" => PolicyAction::Any,
                "handoff" => PolicyAction::Handoff,
                "delegate" => PolicyAction::Delegate,
                "escalate" => PolicyAction::Escalate,
                other => PolicyAction::Tool(Arc::from(other)),
            };
            Ok(())
        }
        "decision" => {
            rule.decision = parse_verb(value, line_number)?;
            Ok(())
        }
        "reason" => {
            rule.reason = Some(unquote(value));
            Ok(())
        }
        other => Err(invalid_yaml(
            line_number,
            format!("unknown rule field '{other}'"),
        )),
    }
}

fn parse_verb(value: &str, line: usize) -> Result<PolicyVerb, PolicyError> {
    match unquote(value).as_ref() {
        "allow" => Ok(PolicyVerb::Allow),
        "deny" => Ok(PolicyVerb::Deny),
        "ask_user" => Ok(PolicyVerb::AskUser),
        other => Err(invalid_yaml(line, format!("unknown policy verb '{other}'"))),
    }
}

fn parse_scalar(value: &str) -> Value {
    let value = unquote(value);
    match value.as_ref() {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        "null" => Value::Null,
        _ => value
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(value.to_string())),
    }
}

fn strip_comment(line: &str) -> &str {
    line.split_once('#').map_or(line, |(before, _)| before)
}

fn unquote(value: &str) -> Arc<str> {
    let value = value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        Arc::from(&value[1..value.len() - 1])
    } else {
        Arc::from(value)
    }
}

fn invalid_yaml(line: usize, message: impl Into<Arc<str>>) -> PolicyError {
    PolicyError::InvalidYaml {
        line,
        message: message.into(),
    }
}
