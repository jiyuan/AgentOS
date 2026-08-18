use super::{SubAgentError, SubAgentInvocation, SubAgentRegistry};
use crate::approve::Policy;
use crate::r#loop::ApprovalAuthorization;
use agentos_interfaces::orchestrator::SubAgentSpec;
use agentos_proto::{
    DelegationGrant, Envelope, RunId, SchemaVersion, DELEGATION_GRANT_SCOPES_KEY,
    DELEGATION_GRANT_TTL_KEY,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl SubAgentRegistry {
    pub fn prepare(
        &self,
        spec: &SubAgentSpec,
        parent_policy: &Policy,
        input: Envelope,
        run_id: RunId,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        self.prepare_inner(spec, parent_policy, input, run_id, None, Vec::new())
    }

    pub fn prepare_authorized(
        &self,
        spec: &SubAgentSpec,
        parent_policy: &Policy,
        input: Envelope,
        run_id: RunId,
        parent_run_id: &RunId,
        authorization: &ApprovalAuthorization,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        self.prepare_inner(
            spec,
            parent_policy,
            input,
            run_id,
            Some((parent_run_id, Some(authorization))),
            Vec::new(),
        )
    }

    pub fn prepare_existing_grants(
        &self,
        spec: &SubAgentSpec,
        parent_policy: &Policy,
        input: Envelope,
        run_id: RunId,
        parent_run_id: &RunId,
        grants: Vec<DelegationGrant>,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        self.prepare_inner(
            spec,
            parent_policy,
            input,
            run_id,
            Some((parent_run_id, None)),
            grants,
        )
    }

    fn prepare_inner(
        &self,
        spec: &SubAgentSpec,
        parent_policy: &Policy,
        input: Envelope,
        run_id: RunId,
        authorization: Option<(&RunId, Option<&ApprovalAuthorization>)>,
        existing_grants: Vec<DelegationGrant>,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        let definition = self
            .definitions
            .get(&(spec.agent_id.clone(), Arc::clone(&spec.policy_id)))
            .cloned()
            .ok_or_else(|| SubAgentError::Unknown {
                agent_id: spec.agent_id.clone(),
                policy_id: Arc::clone(&spec.policy_id),
            })?;
        let child_policy = Policy::narrow(parent_policy, &definition.policy)?;
        #[cfg(debug_assertions)]
        crate::invariants::delegation_narrows(parent_policy, &child_policy);

        let requested_scopes = spec.metadata.get(DELEGATION_GRANT_SCOPES_KEY);
        let requested_ttl = spec
            .metadata
            .get(DELEGATION_GRANT_TTL_KEY)
            .and_then(serde_json::Value::as_u64);
        let expected_scopes = (!definition.delegation_grant_scopes.is_empty()).then(|| {
            serde_json::to_value(&definition.delegation_grant_scopes)
                .expect("delegation grant scopes are serializable")
        });
        if requested_scopes != expected_scopes.as_ref()
            || requested_ttl
                != (!definition.delegation_grant_scopes.is_empty())
                    .then_some(definition.delegation_grant_ttl_secs)
        {
            return Err(SubAgentError::GrantRequestMismatch {
                agent_id: definition.agent_id.clone(),
            });
        }

        let delegation_grants = if !existing_grants.is_empty() {
            let parent_run_id = authorization
                .and_then(|(parent_run_id, authorization)| {
                    authorization.is_none().then_some(parent_run_id)
                })
                .ok_or_else(|| SubAgentError::InvalidGrant {
                    grant_id: existing_grants
                        .first()
                        .map(|grant| Arc::clone(&grant.grant_id))
                        .unwrap_or_else(|| Arc::from("missing")),
                    agent_id: definition.agent_id.clone(),
                })?;
            for grant in &existing_grants {
                if grant.delegatee != definition.agent_id
                    || grant.policy_id != definition.policy_id
                    || grant.parent_run_id != *parent_run_id
                    || grant.transitive
                    || grant.version != SchemaVersion::default()
                    || grant.scopes != definition.delegation_grant_scopes
                    || grant.expires_at_unix
                        != grant
                            .issued_at_unix
                            .saturating_add(definition.delegation_grant_ttl_secs)
                {
                    return Err(SubAgentError::InvalidGrant {
                        grant_id: Arc::clone(&grant.grant_id),
                        agent_id: definition.agent_id.clone(),
                    });
                }
            }
            existing_grants
        } else if definition.delegation_grant_scopes.is_empty() {
            Vec::new()
        } else {
            let Some((parent_run_id, Some(authorization))) = authorization else {
                return Err(SubAgentError::GrantRequiresApproval {
                    agent_id: definition.agent_id.clone(),
                });
            };
            vec![DelegationGrant {
                version: SchemaVersion::default(),
                grant_id: Arc::from(format!(
                    "grant-{}-{}",
                    parent_run_id.as_str(),
                    authorization.approval_id.as_str()
                )),
                authorized_by: authorization.authorized_by.clone(),
                parent_run_id: parent_run_id.clone(),
                delegatee: definition.agent_id.clone(),
                policy_id: Arc::clone(&definition.policy_id),
                issued_at_unix: authorization.approved_at_unix,
                expires_at_unix: authorization
                    .approved_at_unix
                    .saturating_add(definition.delegation_grant_ttl_secs),
                transitive: false,
                scopes: definition.delegation_grant_scopes.clone(),
            }]
        };
        Ok(SubAgentInvocation {
            definition,
            policy: child_policy,
            input,
            run_id,
            channel_capacity: self.channel_capacity,
            trace_sink: self.trace_sink.clone(),
            task_workspace: self.task_workspace.clone(),
            session: self.session.clone(),
            spill: self.spill.clone(),
            tool_result_inline_bytes: self.tool_result_inline_bytes,
            summarizer: self.summarizer.clone(),
            compaction_config: self.compaction_config,
            cancel: CancellationToken::new(),
            parent_seed: None,
            delegation_grants,
        })
    }
}
