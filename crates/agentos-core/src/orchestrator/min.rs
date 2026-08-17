use crate::prompt;
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::tool::ToolSpec;
use agentos_llm::Llm;
use agentos_proto::{Message, MessageRole};
use async_trait::async_trait;
use std::sync::Arc;

pub struct EchoOrchestrator;

#[async_trait]
impl Orchestrator for EchoOrchestrator {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        let content = ctx
            .state
            .transcript
            .items
            .last()
            .map(|item| Arc::clone(&item.message.content))
            .unwrap_or_else(|| Arc::from(""));
        Ok(Plan::Reply(Message::text(MessageRole::Assistant, content)))
    }
}

pub struct MinOrchestrator {
    llm: Arc<dyn Llm>,
    tool_specs: Vec<ToolSpec>,
}

impl MinOrchestrator {
    pub fn new(llm: Arc<dyn Llm>) -> Self {
        Self {
            llm,
            tool_specs: Vec::new(),
        }
    }

    /// Expose a set of tool schemas to the LLM. When the model returns a
    /// `tool_calls` array, `plan()` translates the first call into
    /// `Plan::CallTool`; otherwise it returns `Plan::Reply`.
    pub fn with_tools(mut self, tool_specs: Vec<ToolSpec>) -> Self {
        self.tool_specs = tool_specs;
        self
    }
}

#[async_trait]
impl Orchestrator for MinOrchestrator {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        // `Min` carries no skill catalog, but it still assembles through the
        // one authority — so hydrated memory reaches it too, and a section
        // added later needs no change here.
        let prompt = prompt::assemble(
            ctx,
            &prompt::Assembly {
                tools: &self.tool_specs,
                context_budget_tokens: self.llm.context_budget_tokens(),
                ..Default::default()
            },
        );
        let response =
            super::streaming::complete_message(&*self.llm, ctx, &prompt.messages, &self.tool_specs)
                .await
                .map_err(super::planning_error)?;
        ctx.push_llm_usage_from_message(&response);
        Ok(super::plan_from_response(response))
    }
}
