"""
Deployment for a flow.
"""
from pathlib import Path

import click
from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_aws import ECSTask
from prefect_github import GitHubRepository


from ...utilities.misc import list_packages


@click.group()
def cli():
    ...


# NOTE I do nothing, remove me
@flow
def my_flow():
    """Does something."""
    return []


@cli.command
@click.argument("reference", type=str, default="main")
@click.option(
    "--apply",
    is_flag=True,
    default=False,
    help="Apply the deployment to Orion.",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path),
    help="YAML file to output the deployment to.",
)
@click.option(
    "-s",
    "--storage-block",
    type=str,
    default="github-repository-my-flow",
    help="Name of the GitHubRepository Block the deployment should use."
)
@click.option(
    "-i",
    "--infra-block",
    type=str,
    default="ecs-task-my-flow",
    help="Name of the ECSTask Block the deployment should use."
)
def production(
    reference: str,
    apply: bool,
    output: Path,
    storage_block: str,
    infra_block: str,
):
    """
    Create the production deployment and have it pull data from the repository
    reference.
    """
    # Create storage block to pull production code from
    storage: GitHubRepository = GitHubRepository.load(storage_block)
    storage = storage.copy(update=dict(reference=reference))
    click.echo(
        f"Fetched GitHubRepository Block '{storage._block_document_name}'"
    )

    # Create infrastructure block to run production code in
    infra: ECSTask = ECSTask.load(infra_block)
    click.echo(f"Fetched ECSTask Block '{infra._block_document_name}'")
    infra.env.update(dict(EXTRA_PIP_PACKAGES=" ".join(list_packages(True, False))))

    prod_deployment: Deployment = Deployment.build_from_flow(
        flow=my_flow,
        name="My Flow ETL",
        storage=storage,
        infrastructure=infra,
        schedule=CronSchedule(cron="0 1 * * *"),
        work_queue_name="default",
        tags=["Production"],
    )

    if apply:
        storage.save(storage._block_document_name, overwrite=True)
        click.echo(
            f"Created GitHubRepository Block '{storage._block_document_name}'"
        )
        infra.save(infra._block_document_name, overwrite=True)
        click.echo(f"Created ECSTask Block '{infra._block_document_name}'")
        click.echo(
            f"  EXTRA_PIP_PACKAGES=\"{infra.env['EXTRA_PIP_PACKAGES']}\""
        )
        prod_deployment.apply()
        click.echo(f"Applied Deployment '{prod_deployment.name}'")
    if output:
        prod_deployment.to_yaml(output)
        click.echo(str(output))


@cli.command
@click.argument("reference", type=str)
@click.option(
    "--apply",
    is_flag=True,
    default=False,
    help="Apply the deployment to Orion.",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path),
    help="YAML file to output the deployment to.",
)
@click.option(
    "-s",
    "--storage-block",
    type=str,
    default="github-repository-my-flow",
    help="Name of the GitHubRepository Block the deployment should use."
)
@click.option(
    "-i",
    "--infra-block",
    type=str,
    default="ecs-task-my-flow",
    help="Name of the ECSTask Block the deployment should use."
)
def development(
    reference: str,
    apply: bool,
    output: Path,
    storage_block: str,
    infra_block: str,
):
    """
    Create a development deployment identifiable via its repository
    reference.
    """
    # Create storage block to pull development code from
    storage: GitHubRepository = GitHubRepository.load(storage_block)
    click.echo(
        f"Fetched GitHubRepository Block '{storage._block_document_name}'"
    )
    storage = storage.copy(
        exclude={"_block_document_id"},
        update=dict(
            _block_document_name=(
                storage._block_document_name + "-dev-" + reference
            ),
            reference=reference,
        ),
    )

    # Create infrastructure block to run development code in
    infra: ECSTask = ECSTask.load(infra_block)
    click.echo(f"Fetched ECSTask Block '{infra._block_document_name}'")
    env = infra.env.copy()
    env["EXTRA_PIP_PACKAGES"] = " ".join(list_packages())
    infra = infra.copy(
        exclude={"_block_document_id"},
        update=dict(
            _block_document_name=(
                infra._block_document_name + "-dev-" + reference
            ),
            env=env,
        ),
    )

    dev_deployment: Deployment = Deployment.build_from_flow(
        flow=my_flow,
        name=f"My Flow ETL - Dev[{reference}]",
        storage=storage,
        infrastructure=infra,
        work_queue_name="default",
        tags=["Development"],
    )

    if apply:
        storage.save(storage._block_document_name, overwrite=True)
        click.echo(
            f"Created GitHubRepository Block '{storage._block_document_name}'"
        )

        infra.save(infra._block_document_name, overwrite=True)
        click.echo(f"Created ECSTask Block '{infra._block_document_name}'")
        click.echo(
            f"  EXTRA_PIP_PACKAGES=\"{infra.env['EXTRA_PIP_PACKAGES']}\""
        )

        dev_deployment.apply()
        click.echo(f"Applied Deployment '{dev_deployment.name}'")
    if output:
        dev_deployment.to_yaml(output)
        click.echo(str(output))


if __name__ == "__main__":
    cli()
