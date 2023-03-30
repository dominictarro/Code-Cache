"""
Utility methods for uncategorized activities.
"""
import math

import toml


def convert_size(size_bytes: int, rounding: int = 2):
    """Converts bytes to the best size unit.

    :param size_bytes:  Size in bytes
    :param rounding:    Number of decimals to round new unit to, defaults to 2
    :return:            Size in best unit
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, rounding)
    return "%s %s" % (s, size_name[i])


def list_packages(
    packages: bool = True, dev_packages: bool = False
) -> list[str]:
    """Lists packages in a Pipfile.

    :param packages:        Include packages in the `packages` set, defaults\
        to True
    :param dev_packages:    Include packages in the `dev-packages` set,\
        defaults to False
    :return:                
    """
    data = []
    with open("Pipfile", "r") as fo:
        pipfile: dict = toml.load(fo)
        if packages:
            data.extend(pipfile["packages"])
        if dev_packages:
            data.extend(pipfile["dev-packages"])
    return data
