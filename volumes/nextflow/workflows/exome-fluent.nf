nextflow.enable.dsl = 2

def jsonHeader(task, sample_id) {
    def process = task.process
    def attempt = task.attempt
    
    """\
    printf '{"nf_meta":true,"workflow":"%s","nextflow_run":"%s","process":"%s","sample":"%s","attempt":"%s","user":"%s","slurm_job_id":"%s","slurm_node":"%s"}\\n' \
        "\${WORKFLOW_NAME:-unknown}" \
        "\${RUN_NAME:-unknown}" \
        "${process}" \
        "${sample_id}" \
        "${attempt}" \
        "\${USER:-unknown}" \
        "\${SLURM_JOB_ID:-none}" \
        "\${SLURM_NODELIST:-none}" | tee /dev/stderr
    """.stripIndent()
}

params.sample_id = params.sample_id ?: 'SAMPLE1'
params.reads1    = params.reads1    ?: 'data/SAMPLE1_R1.fastq.gz'
params.reads2    = params.reads2    ?: 'data/SAMPLE1_R2.fastq.gz'
params.ref_fa    = params.ref_fa    ?: 'ref/genome.fa'

process ALIGN {

    tag "${sample_id}"

    input:
        val(sample_id)
        path(reads1)
        path(reads2)
        path(ref_fa)

    output:
        path("${sample_id}.aligned.bam")

    script:
    """
    ${jsonHeader(task, sample_id)}
    echo "Starting alignment for sample: ${sample_id}"
    echo "R1: ${reads1}"
    echo "R2: ${reads2}"
    echo "Ref: ${ref_fa}"

    for step in \$(seq 0 100); do
        echo "Aligning... \${step}% complete"
        if (( RANDOM % 100 < 0 )); then
            echo "Out of memory" > /dev/stderr
            exit 1
        fi
        sleep 0.1
    done
 
    # Demo: just create a dummy BAM file
    echo "DUMMY_BAM_CONTENT" > ${sample_id}.aligned.bam
    """
}

process CALL_VARIANTS {

    tag "${sample_id}"

    input:
        val(sample_id)
        path(bam)
        path(ref_fa)

    output:
        path("${sample_id}.variants.vcf.gz")

    script:
    """
    ${jsonHeader(task, sample_id)}
    echo "Sample: ${sample_id}"
    echo "BAM: ${bam}"

    printf "##fileformat=VCFv4.2\n#CHROM\\tPOS\\tID\\tREF\\tALT\\tQUAL\\tFILTER\\tINFO\n" \
      > ${sample_id}.variants.vcf
    gzip -c ${sample_id}.variants.vcf > ${sample_id}.variants.vcf.gz
    """
}

process COVERAGE_QC {

    tag "${sample_id}"

    input:
        val(sample_id)
        path(bam)

    output:
        path("${sample_id}.coverage_qc.txt")

    script:
    """
    ${jsonHeader(task, sample_id)}
    echo "Sample: ${sample_id}"
    echo "BAM: ${bam}"
    echo "metric\tvalue"            >  ${sample_id}.coverage_qc.txt
    echo "mean_coverage\t42"       >> ${sample_id}.coverage_qc.txt
    echo "pct_10x\t0.98"           >> ${sample_id}.coverage_qc.txt
    """
}

process ANNOTATE_VCF {

    tag "${sample_id}"

    input:
        val(sample_id)
        path(vcf_gz)

    output:
        path("${sample_id}.annotated.vcf.gz")

    script:
    """
    ${jsonHeader(task, sample_id)}
    echo "Sample: ${sample_id}"
    echo "VCF: ${vcf_gz}"
    echo "Annotated VCF" > ${sample_id}.annotated.vcf.gz
    """
}

process SUBMIT_TO_DB {

    tag "${sample_id}"

    input:
        val(sample_id)
        path(bam)
        path(vcf_gz)
        path(annotated_vcf)

    output:
        path("${sample_id}.db_payload.json")

    script:
    """
    ${jsonHeader(task, sample_id)}
    echo "Sample: ${sample_id}"
    echo "BAM: ${bam}"
    echo "VCF: ${vcf_gz}"
    echo "Annotated VCF: ${annotated_vcf}"

    cat <<EOF > ${sample_id}.db_payload.json
{
  "sample_id": "${sample_id}",
  "bam_path":  "${bam}",
  "vcf_path":  "${vcf_gz}",
  "annotated_vcf_path": "${annotated_vcf}",
  "status":    "READY_FOR_SUBMISSION"
}
EOF
    """
}

workflow {

    ch_aligned_bam = ALIGN(
        params.sample_id,
        file(params.reads1),
        file(params.reads2),
        file(params.ref_fa)
    )

    ch_vcf = CALL_VARIANTS(
        params.sample_id,
        ch_aligned_bam,
        file(params.ref_fa)
    )

    COVERAGE_QC(
        params.sample_id,
        ch_aligned_bam
    )

    ch_annotated_vcf = ANNOTATE_VCF(
        params.sample_id,
        ch_vcf
    )

    SUBMIT_TO_DB(
        params.sample_id,
        ch_aligned_bam,
        ch_vcf,
        ch_annotated_vcf
    )
}