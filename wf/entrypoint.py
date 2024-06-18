import os
import shutil
import subprocess
import typing
from pathlib import Path

import requests
import typing_extensions
from flytekit.core.annotation import FlyteAnnotation
from latch.ldata.path import LPath
from latch.resources.tasks import custom_task, nextflow_runtime_task
from latch.resources.workflow import workflow
from latch.types import metadata
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.nextflow.workflow import get_flag
from latch_cli.services.register.utils import import_module_by_path
from latch_cli.utils import urljoins

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)


@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        },
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]


@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(
    pvc_name: str,
    input: LatchFile,
    viral_fasta: LatchFile,
    outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({"output": True})],
    email: typing.Optional[str],
    multiqc_title: typing.Optional[str],
    genome: typing.Optional[str],
    fasta: typing.Optional[LatchFile],
    gtf: typing.Optional[LatchFile],
    multiqc_methods_description: typing.Optional[str],
    min_reads: int,
    max_hits: int,
    remove_duplicates: bool,
) -> None:
    try:
        shared_dir = Path("/nf-workdir")

        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
            *get_flag("input", input),
            *get_flag("viral_fasta", viral_fasta),
            *get_flag("outdir", outdir),
            *get_flag("email", email),
            *get_flag("multiqc_title", multiqc_title),
            *get_flag("min_reads", min_reads),
            *get_flag("max_hits", max_hits),
            *get_flag("remove_duplicates", remove_duplicates),
            *get_flag("genome", genome),
            *get_flag("fasta", fasta),
            *get_flag("gtf", gtf),
            *get_flag("multiqc_methods_description", multiqc_methods_description),
        ]

        print("Launching Nextflow Runtime")
        print(" ".join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(
                    urljoins(
                        "latch:///your_log_dir/nf_nf_core_viralintegration",
                        name,
                        "nextflow.log",
                    )
                )
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)


@workflow(metadata._nextflow_metadata)
def nf_nf_core_viralintegration(
    input: LatchFile,
    viral_fasta: LatchFile,
    outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({"output": True})],
    email: typing.Optional[str],
    multiqc_title: typing.Optional[str],
    genome: typing.Optional[str],
    fasta: typing.Optional[LatchFile],
    gtf: typing.Optional[LatchFile],
    multiqc_methods_description: typing.Optional[str],
    min_reads: int = 5,
    max_hits: int = 50,
    remove_duplicates: bool = True,
) -> None:
    """
    nf-core/viralintegration

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(
        pvc_name=pvc_name,
        input=input,
        viral_fasta=viral_fasta,
        outdir=outdir,
        email=email,
        multiqc_title=multiqc_title,
        min_reads=min_reads,
        max_hits=max_hits,
        remove_duplicates=remove_duplicates,
        genome=genome,
        fasta=fasta,
        gtf=gtf,
        multiqc_methods_description=multiqc_methods_description,
    )
