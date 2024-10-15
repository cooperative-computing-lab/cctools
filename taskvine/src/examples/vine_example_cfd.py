#!/usr/bin/env python

# This example runs multiple computational fluid dynamics (CFD)
# simulations using Ansys Fluent on a matrix of angle of attack
# and Mach number input parameters. Each simulation outputs the
# converged aerodynamic force and torque values.

# The program demonstrates how users can handle multiple input
# and output files from each task and create an interactive
# script to launch many computationally intensive scientific tasks.

from datetime import datetime
import logging
from math import cos, radians, sin
import os

import click
import ndcctools.taskvine as vine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


@click.command(context_settings={"show_default": True})
@click.argument("case")
@click.option("--attacks",
              "-a",
              multiple=True,
              default=(0.0, 5.0, 10.0, 15.0, 20.0),
              help="The angles of attack to parameterize.")
@click.option("--machs",
              "-m",
              multiple=True,
              default=(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6),
              help="The mach numbers to parameterize.")
@click.option("--iterations",
              "-i",
              default=500,
              help="The number of iterations to execute the CFD run.")
@click.option("--port",
              "-p",
              default=9340,
              help="The port on which the TaskVine manager listens.")
def cfd(case: str, attacks: tuple[int, ...], machs: tuple[int, ...],
        iterations: int, port: int):
    """Launch parameterized Ansys Fluent CFD jobs via TaskVine.

    The case file must contain the vehicle mesh and four boundaries: farfield,
    inlet, outlet, and launchvehicle. The vehicle's roll axis must coincide
    with the x axis such that the drag force acts in the positive x direction.
    """
    # Create the TaskVine manager.
    manager = vine.Manager(port)
    case_vine_file = manager.declare_file(case)
    logging.info(f"TaskVine manager listening on port {port}")

    # Retrieve the basename of the case file.
    case_name = os.path.basename(case)
    case_name = case_name.split(".", 2)[0]

    # Load the journal template.
    journal_file = f"data/journal.jou"
    journal_template: str
    with open(journal_file, "r") as file:
        journal_template = file.read()

    # Make the output directory.
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    output_directory = f"data/cfd/{now}"
    os.makedirs(output_directory, exist_ok=True)
    logger.info(f"Created output directory {output_directory}")

    for attack in attacks:
        for mach in machs:
            name = f"{case_name}_{attack}_{mach}_{iterations}"

            # To induce an angle of attack, split the x and y components of the
            # flow velocity vector. Note that we assume the vehicle's roll axis
            # coincides with the x axis.
            angle_of_attack = radians(attack)
            flow_vector_x = cos(angle_of_attack)
            flow_vector_y = sin(angle_of_attack)

            # Paramterize the journal template.
            journal_paramaterized = journal_template.format(
                mach=mach,
                flow_vector_x=flow_vector_x,
                flow_vector_y=flow_vector_y,
                iterations=iterations)

            # Create the task with inputs and outputs.
            task = vine.Task(
                f"module load ansys/2024R1; fluent 3ddp -t1 -g < journal.jou > log 2>&1"
            )
            journal_vine_buffer = manager.declare_buffer(journal_paramaterized)
            axial_vine_file = manager.declare_file(
                f"{output_directory}/{name}.axial")
            normal_vine_file = manager.declare_file(
                f"{output_directory}/{name}.normal")
            log_vine_file = manager.declare_file(
                f"{output_directory}/{name}.log")
            task.add_input(case_vine_file, "case.cas.h5")
            task.add_input(journal_vine_buffer, "journal.jou")
            task.add_output(axial_vine_file, "axial.out")
            task.add_output(normal_vine_file, "normal.out")
            task.add_output(log_vine_file, "log")

            # Submit the task to TaskVine.
            manager.submit(task)
            logger.info(
                f"Submitted computational fluid dynamics task {task.id} with {case_name=} {attack=} {mach=} {iterations=}"
            )

    while not manager.empty():
        task = manager.wait(10)
        if task is not None:
            logger.info(
                f"Completed task {task.id} with exit code {task.exit_code}")


if __name__ == "__main__":
    cfd()
