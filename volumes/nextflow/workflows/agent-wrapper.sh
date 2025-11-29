#!/bin/bash

# Wrapper script to run a Nextflow workflow and, if it errors, invoke the
# agent-slack.py script to analyze the error and post to Slack.

SAMPLE_NAME=$1

touch "r1_${SAMPLE_NAME}.fastq.gz"
touch "r2_${SAMPLE_NAME}.fastq.gz"

nextflow run \
    -config /nextflow/workflows/nextflow.config \
    -profile slurm \
    -name exome_$SAMPLE_NAME \
    /nextflow/workflows/unreliable-exome.nf \
        --sample_id $SAMPLE_NAME \
        --reads1 r1_${SAMPLE_NAME}.fastq.gz \
        --reads2 r2_${SAMPLE_NAME}.fastq.gz \
        --ref_fa /nextflow/runs/common/genome.fa
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "Workflow failed with exit code $EXIT_CODE. Invoking agent-slack.py..."
    source /nextflow/agent/.venv/bin/activate
    python /nextflow/agent/agent-slack.py --run-name exome_$SAMPLE_NAME
else
    echo "Workflow completed successfully."
fi
