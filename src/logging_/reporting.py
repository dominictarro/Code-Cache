"""
Code for enhanced logging outputs.
"""
import logging

import psutil

from ..utilities.misc import convert_size


def log_system_states(
    logger: logging.Logger, level: logging._Level = logging.INFO
):
    """Logs memory and disk usage of the system.

    :param logger:  Logger to log with
    :param level:   Level to log at, defaults to logging.INFO
    """
    # Memory measurements
    m = psutil.virtual_memory()
    logger.log(
        level,
        "Memory state: total='%s' free='%s' used='%s'",
        convert_size(m.total),
        convert_size(m.free),
        convert_size(m.used),
    )

    # Disks
    partitions = psutil.disk_partitions()
    for prtn in partitions:
        if prtn.mountpoint:
            d = psutil.disk_usage(prtn.mountpoint)
            logger.log(
                level,
                "Disk state: disk='%s' mountpoint='%s' total='%s' free='%s' used='%s'",
                prtn.device,
                prtn.mountpoint,
                convert_size(d.total),
                convert_size(d.free),
                convert_size(d.used),
            )
