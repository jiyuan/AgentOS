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
        MemoryOwner::Conversation(principal) => {
            // The whole principal, not just the conversation id: the same id
            // on another channel, or under another agent, is another
            // conversation's memory.
            if principal == &caller.conversation_principal() {
                Ok(())
            } else {
                Err(unauthorized(
                    "conversation memory belongs to a different conversation",
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
            if scope.visibility == MemoryVisibility::Private {
                // A private scope owned by nobody in particular is not a
                // thing; refusing is the only honest reading.
                return Err(unauthorized("shared memory cannot be private"));
            }
            let domain = scope.domain.as_deref().unwrap_or("general");
            let permitted = match operation {
                MemoryOperation::Read => caller
                    .allowed_shared_domains
                    .iter()
                    .any(|allowed| allowed.as_ref() == domain),
                // Everything that is not a read is a mutation, and a shared
                // mutation needs the writable list — which the runtime built
                // by intersecting the deployment's global switch, the
                // domain's own `write`, and the caller's grant
                // (M7 / `MEM-001`).
                MemoryOperation::Write | MemoryOperation::Forget => caller
                    .writable_shared_domains
                    .iter()
                    .any(|allowed| allowed.as_ref() == domain),
            };
            if permitted {
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
        if let Some(user_id) = &caller.user_id {
            scopes.push(MemoryScope::new(
                store,
                MemoryOwner::User(Arc::clone(user_id)),
                MemoryVisibility::Private,
                domain.clone(),
            ));
        }
        scopes.push(MemoryScope::new(
            store,
            MemoryOwner::Conversation(caller.conversation_principal()),
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

pub(super) fn unauthorized(message: &'static str) -> MemoryError {
    MemoryError::Backend(Arc::from(format!("memory scope unauthorized: {message}")))
}
