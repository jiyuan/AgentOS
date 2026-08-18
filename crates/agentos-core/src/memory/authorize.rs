use super::accounting::MemoryOperation;
use super::{
    HydrationRequest, MemoryCaller, MemoryError, MemoryOwner, MemoryScope, MemoryStore,
    MemoryVisibility,
};
use std::sync::Arc;

pub(super) fn authorize_scope(
    caller: &MemoryCaller,
    scope: &MemoryScope,
    operation: MemoryOperation,
) -> Result<(), MemoryError> {
    if scope.store == MemoryStore::Audit {
        if caller.audit_read_access && matches!(operation, MemoryOperation::Read) {
            return Ok(());
        }
        return Err(unauthorized("audit memory requires an administrative path"));
    }

    match &scope.owner {
        MemoryOwner::User(user_id) => {
            if caller.principal_key.is_some() {
                return Err(unauthorized(
                    "legacy user memory requires ID-002 migration to a typed principal",
                ));
            }
            let caller_user = caller
                .user_id
                .as_deref()
                .unwrap_or_else(|| caller.conversation_id.as_str());
            if caller_user == user_id.as_ref() {
                Ok(())
            } else {
                Err(unauthorized("user memory belongs to a different caller"))
            }
        }
        MemoryOwner::Conversation(conversation_id) => {
            if caller.principal_key.is_none() && conversation_id == &caller.conversation_id {
                Ok(())
            } else {
                Err(unauthorized(
                    "conversation memory belongs to a different conversation",
                ))
            }
        }
        MemoryOwner::Principal(principal_key) => {
            if caller.principal_key.as_ref() == Some(principal_key) {
                Ok(())
            } else {
                Err(unauthorized(
                    "principal memory belongs to a different caller",
                ))
            }
        }
        MemoryOwner::Agent(agent_id) => {
            if scope.visibility == MemoryVisibility::Private && agent_id == &caller.agent_id {
                Ok(())
            } else {
                Err(unauthorized("agent memory belongs to a different agent"))
            }
        }
        MemoryOwner::Task(task_id) => {
            if task_id == &caller.task_id {
                Ok(())
            } else {
                Err(unauthorized("task memory belongs to a different task"))
            }
        }
        MemoryOwner::Shared => {
            let shared_read = matches!(operation, MemoryOperation::Read)
                && scope.visibility != MemoryVisibility::Private;
            if shared_read && shared_domain_allowed(caller, scope) {
                Ok(())
            } else {
                Err(unauthorized(
                    "shared memory is outside the caller's allowed domains",
                ))
            }
        }
    }
}

pub(super) fn hydration_scopes(
    caller: &MemoryCaller,
    request: &HydrationRequest,
) -> Vec<MemoryScope> {
    let stores = if request.stores.is_empty() {
        vec![MemoryStore::Semantic]
    } else {
        request.stores.clone()
    };
    let domain = request.domain.clone();
    let mut scopes = Vec::new();
    for store in stores {
        if caller.principal_key.is_none() {
            if let Some(user_id) = &caller.user_id {
                scopes.push(MemoryScope::new(
                    store,
                    MemoryOwner::User(Arc::clone(user_id)),
                    MemoryVisibility::Private,
                    domain.clone(),
                ));
            }
        }
        scopes.push(MemoryScope::new(
            store,
            caller
                .principal_key
                .clone()
                .map(MemoryOwner::Principal)
                .unwrap_or_else(|| MemoryOwner::Conversation(caller.conversation_id.clone())),
            MemoryVisibility::Private,
            domain.clone(),
        ));
        scopes.push(MemoryScope::new(
            store,
            MemoryOwner::Agent(caller.agent_id.clone()),
            MemoryVisibility::Private,
            domain.clone(),
        ));
        scopes.push(MemoryScope::new(
            store,
            MemoryOwner::Task(caller.task_id.clone()),
            MemoryVisibility::Private,
            domain.clone(),
        ));
        for shared_domain in &caller.allowed_shared_domains {
            scopes.push(MemoryScope::new(
                store,
                MemoryOwner::Shared,
                MemoryVisibility::Shared,
                Some(Arc::clone(shared_domain)),
            ));
        }
    }
    scopes
}

fn shared_domain_allowed(caller: &MemoryCaller, scope: &MemoryScope) -> bool {
    let domain = scope.domain.as_deref().unwrap_or("general");
    caller
        .allowed_shared_domains
        .iter()
        .any(|allowed| allowed.as_ref() == domain)
}

pub(super) fn unauthorized(message: &'static str) -> MemoryError {
    MemoryError::Backend(Arc::from(format!("memory scope unauthorized: {message}")))
}
