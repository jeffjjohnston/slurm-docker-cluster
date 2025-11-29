import argparse
import asyncio
import json
import pandas as pd
from pathlib import Path
from agents import Runner, trace, Agent, ModelSettings, function_tool
from dotenv import load_dotenv, find_dotenv
from loki_utils import LokiClient

load_dotenv(find_dotenv())


AGENT_INSTRUCTIONS = (Path(__file__).parent / "instructions.md").read_text()

WORKFLOWS_BASE_PATH = Path("/nextflow/workflows")

WORKFLOW_DEFINITIONS = {
    "unreliable-exome": (WORKFLOWS_BASE_PATH / "unreliable-exome.nf").read_text()
}


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Observability Agent")
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="The query for the agent to answer.",
    )
    return parser.parse_args()


@function_tool
async def retrieve_logs_for_run(run_name: str, lookback_hours: int = 72) -> str:
    """Query Loki for logs associated with a given Nextflow run name."""
    print(f"retrieve_logs_for_run({run_name}, {lookback_hours})")
    client = LokiClient(base_url="http://loki:3100")
    query = f'{{source="nextflow"}} | json | nextflow_run="{run_name}"'
    raw_records = client.query_range(
        query=query,
        hours_ago=lookback_hours,
        limit=5000,
        direction="FORWARD",
    )

    records = []
    for record in raw_records:
        entry = json.loads(str(record["record"]))
        records.append(
            {
                "timestamp": pd.to_datetime(int(record["timestamp"]), unit="ns"),
                "stream": record["labels"]["stream"],
                **entry,
            }
        )

    records_df = pd.DataFrame(records)
    # arrange by timestamp ascending
    records_df = records_df.sort_values(by="timestamp", ascending=True).reset_index(
        drop=True
    )
    print(f"  - Retrieved {len(records_df)} log records for run {run_name}")
    return json.dumps(records_df.to_dict(orient="records"), default=str)


@function_tool
async def retrieve_workflow_definition(workflow_file: str) -> str:
    """Retrieve the Nextflow workflow definition for a given workflow file name."""
    print(f"retrieve_workflow_definition({workflow_file})")
    # remove .nf extension if present
    if workflow_file.endswith(".nf"):
        workflow_file = workflow_file[:-3]
    if workflow_file in WORKFLOW_DEFINITIONS:
        return WORKFLOW_DEFINITIONS[workflow_file]
    return f"Workflow definition for {workflow_file} not found."


async def main():
    """Main function to run the agent."""
    print("Starting agent...")

    args = parse_args()
    log_agent = Agent(
        name="Workflow Observability Agent",
        model_settings=ModelSettings(tool_choice="required"),
        tools=[
            retrieve_logs_for_run,
            retrieve_workflow_definition,
        ],
        model="gpt-5.1",
        instructions=AGENT_INSTRUCTIONS,
    )

    with trace("Workflow Observability Agent"):
        response = await Runner.run(
            starting_agent=log_agent,
            input=args.query,
        )
    print("Agent Response:")
    print(response.final_output)


if __name__ == "__main__":
    asyncio.run(main())
