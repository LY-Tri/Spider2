from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

def terminate(answer: str, execution_summary: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """
    Submit final SQL answer.
    
    Args:
        answer: The final SQL query that answers the question.
        execution_summary: Optional summary of the decomposition steps taken.
    """
    output = f"EXECUTION RESULT of [terminate]:\n{answer}"
    if execution_summary:
        output += f"\n\nExecution Summary:\n{execution_summary}"

    return {
        "content": output
    }


def register_tools(registry):
    registry.register_tool("terminate", terminate)
    registry.register_tool("finish", terminate)  # Register finish as an alias for terminate
