from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def plan_step(
    step_description: str = None,
    step_number: Optional[str] = None,
    depends_on: Optional[str] = None,
    rationale: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Plan the next sub-query step. Called iteratively throughout execution.
    
    This tool records what the LLM intends to do before generating SQL,
    providing traceability and encouraging structured thinking.
    
    Args:
        step_description: What this step will accomplish (natural language)
        step_number: Optional step number for tracking
        depends_on: Which previous steps/temp tables this depends on
        rationale: Why this step is needed based on previous results
    
    Returns:
        Confirmation to proceed with generating the sub-SQL
    """
    if not step_description:
        return {
            "content": "ERROR: 'step_description' is a required parameter for [plan_step]. Please provide it."
        }

    logger.info(f"Planning step: {step_description}")
    
    # Format the step information
    step_num_str = f"Step {step_number}" if step_number else "Next Step"
    deps_str = depends_on if depends_on else "None"
    rationale_str = rationale if rationale else "Initial step"
    
    content = f"""PLAN RECORDED:
Step {step_number or 'N'}: {step_description}
Dependencies: {deps_str}
Rationale: {rationale_str}

Now generate the SQL for this step using execute_sql_step.
If you need to check schema details first, use execute_bash."""
    
    return {
        "content": f"EXECUTION RESULT of [plan_step]:\n{content}"
    }


def register_tools(registry):
    """Register planner tools with the tool registry."""
    registry.register_tool("plan_step", plan_step)
