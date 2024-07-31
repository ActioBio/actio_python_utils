import _io
import bz2
import csv
import gzip
import inspect
import logging
import lzma
import math
import operator
import os
import pickle
import random
import re
import signal
import subprocess
import sys
import time
import traceback
import yaml
from collections.abc import Callable, Hashable, Iterable, Mapping, MutableMapping
from functools import cache, partial, reduce, wraps
from ipdb import post_mortem
from numbers import Real
from pathlib import Path
from typing import Any, BinaryIO, Collection, Optional


# load YAML config file
cfg_fn = os.path.join(os.path.expanduser("~"), ".actiorc")
if not os.path.isfile(cfg_fn):
    cfg_fn = os.path.join(os.path.dirname(__file__), "conf", "config.yaml")
with open(cfg_fn) as c:
    cfg = yaml.safe_load(c)

logger = logging.getLogger(__name__)


def intersect_dict_with_set(
    d: dict[Hashable, Any], s: Collection[Hashable]
) -> dict[Hashable, Any]:
    """
    Restricts ``d`` to those ``(key, value)`` pairs such that ``key in s``

    :param d: the dictionary
    :param s: the set
    :return: the restricted dict
    """
    return {key: d[key] for key in d if key in s}


class Debug(object):
    def __init__(self, debug: bool = False):
        self.debug = debug

    def __enter__(self):
        if self.debug:
            logger.debug(
                "Will enter ipdb debugging mode if an exception is encountered."
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if self.debug:
                logger.error("Exception occurred", exc_info=(exc_type, exc_val, exc_tb))
                self.exc_type = exc_type
                self.exc_val = exc_val
                self.exc_tb = exc_tb
                # traceback.print_exc()
                # e, m, tb = sys.exc_info()
                # disable unhelpful logging messages
                for log_type in ["asyncio", "parso.cache", "parso.python.diff"]:
                    logging.getLogger(log_type).setLevel(logging.WARNING)
                post_mortem(exc_tb)
            return False  # re-reaise the exception


def set_seed_with_logging(seed: int | None) -> None:
    """
    Sets a seed and logs it along with the calling function's name.

    :param seed: The seed to set
    """
    if seed is not None:
        logger.debug(f"Setting seed={seed} from {inspect.stack()[1].function}.")
        random.seed(seed)


def coalesce(*args) -> Any:
    """
    Return the first argument that is not ``None``

    :return: The first defined value
    """
    for item in args:
        if item is not None:
            return item
    return None


def flatten_cfg(
    key: Hashable, d: MutableMapping[Hashable, dict | list] = cfg, sep: str = "."
) -> None:
    """
    Flatten a nested value in a dict

    :param key: The key corresponding to the value to flatten
    :param d: The dict to use
    :param sep: The value to join key with its nested values
    :raises TypeError: If ``d[key]`` is not either a :class:`dict` or
        :class:`list`
    """
    if key not in d:
        raise KeyError(f"{key} isn't a valid key in {d}.")
    key_type = type(d[key])
    if key_type is dict:
        keys_to_delete = []
        pairs_to_add = []
        for nested_key, value in d[key].items():
            if type(value) is dict:
                keys_to_delete.append(nested_key)
                for subkey, subvalue in value.items():
                    pairs_to_add.append((sep.join([nested_key, subkey]), subvalue))
        for nested_key in keys_to_delete:
            del d[key][nested_key]
        for nested_key, value in pairs_to_add:
            d[key][nested_key] = value
    elif key_type is list:
        # this will retain the ordering which can be important for materialized views
        new_list = []
        for item in d[key]:
            if type(item) is dict:
                for nested_key, values in item.items():
                    for value in values:
                        new_list.append(sep.join([nested_key, str(value)]))
            else:
                new_list.append(item)
        d[key] = new_list
    else:
        raise TypeError(f"Unexpected key type: {key_type} for {key} in {d}.")


def rename_dict_keys(
    original_dict: Mapping[Hashable, Any], renames: Iterable[tuple[Hashable, Hashable]]
) -> dict:
    """
    Returns a new dict that's a copy of the supplied dict but with an arbitrary
    number of keys renamed

    :param original_dict: The dict to rename keys in
    :param renames: A list of pairs of key names, ``[old, new]``
    :return: A dict with relabelled keys
    """
    d = original_dict.copy()
    for old, new in renames:
        if old in d:
            if new is None:
                del d[old]
            else:
                d[new] = d.pop(old)
    return d


def cast_chromosome_to_int(chromosome: str) -> int:
    """
    Cast a chromosome string, optionally prefixed with chr, to an integer.
    X -> 23, Y -> 24, M/MT -> 25.  Failures will be returned as None

    :param chromosome: The chromosome to cast
    :return: The integer version of the chromosome
    """
    chromosome = chromosome.removeprefix("chr")
    if chromosome == "X":
        return 23
    elif chromosome == "Y":
        return 24
    elif chromosome in ("M", "MT"):
        return 25
    else:
        try:
            return int(chromosome)
        except:
            return None


class DictToFunc(object):
    """
    Class that after initializing with a dict can be called to map keys to
    values, e.g.::

        > D = DictToFunc({"a": 42, "b": "apple"})
        > D("a")
        42
        > D("b")
        apple
        > D("c")
        KeyError("c")

    :param choices: The dict to map keys to values
    """

    def __init__(self, choices: dict[Hashable, Any]):
        self.choices = choices

    def __call__(self, key: Hashable) -> Any:
        """
        Returns the value in ``self.choices`` for  ``key``

        :param key: The dict key
        :return: ``self.choices[key]``
        """
        return self.choices[key]


class NumericValue(object):
    """
    Creates a class that can be used as a function to verify that a passed
    argument is a numeric value of the correct type and in the expected range

    :param min_value: Minimum value to compare to
    :param max_value: Maximum value to compare to
    :param left_op: Compare with  ``min_value left_op value``
    :param right_op: Compare with ``value right_op max_value``
    :param var_type: The type of value to cast to
    """

    def __init__(
        self,
        min_value: Real = -math.inf,
        max_value: Real = math.inf,
        left_op: Callable[[Real, Real], bool] = operator.le,
        right_op: Callable[[Real, Real], bool] = operator.lt,
        var_type: Real = int,
    ) -> None:
        self.min_value = min_value
        self.max_value = max_value
        self.left_op = left_op
        self.right_op = right_op
        self.var_type = var_type

    def __call__(self, arg: Real) -> Real:
        """
        Returns ``self.var_type(arg)`` if this is valid,
        ``self.left_op(self.min_value, self.var_type(arg))`` &
        ``self.right_op(self.var_type(arg), self.max_value)``

        :param arg: the value to test
        :raises ValueError: If arg cannot be cast to the desired type or if it
            is not in the expected range
        :return: ``self.var_type(arg)`` if possible and in the proper range
        """
        try:
            value = self.var_type(arg)
        except:
            raise ValueError(f"{arg} is not the expected type ({self.var_type}).")
        if self.left_op(self.min_value, value) and self.right_op(value, self.max_value):
            return value
        else:
            raise ValueError(f"{value} is not in the expected range.")


def timer(func: Callable[[...], Any]) -> Callable[[...], Any]:
    """
    Wraps a function to output the running time of function calls.

    :param func: The function to wrap
    :return: The wrapped function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        logger.info(f"Finished {func.__name__!r} in {elapsed_time:.4f} seconds.")
        return value

    return wrapper


def debug(func: Callable[[...], Any]) -> Callable[[...], Any]:
    """
    Wraps a function to output the function signature, run the function,
    output the return value, and return the return value.

    :param func: The function to wrap
    :return: The wrapped function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        logger.debug(f"{func.__name__!r} returned {value!r}")
        return value

    return wrapper


def which(program: str) -> Optional[str]:
    """
    :param program: The program to find
    :return: The path to the program if location, None otherwise
    """

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def open_pipe(command: str, mode: str = "r", buff: int = 1024 * 1024) -> None:
    """
    Runs a :class:`subprocess.Popen` and either retains input or output

    :param command: The command to execute
    :param mode: The mode with which to handle process, "r" = read, "w" = write
    :param buff: Buffer size
    """
    text = "b" not in mode
    if "r" in mode:
        return subprocess.Popen(
            command,
            shell=True,
            bufsize=buff,
            stdout=subprocess.PIPE,
            text=text,
            preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL),
        ).stdout
    elif "w" in mode:
        return subprocess.Popen(
            command, shell=True, bufsize=buff, stdin=subprocess.PIPE, text=text
        ).stdin
    return None


NORMAL = 0
PROCESS = 1
PARALLEL = 2
WHICH_BZIP2 = which("bzip2")
WHICH_GZIP = which("gzip")
WHICH_PBZIP2 = which("pbzip2")
WHICH_PIGZ = which("pigz")
WHICH_XZ = which("xz")


def open_bz2(
    filename: str, mode: str = "r", buff: int = 1024 * 1024, external: int = PARALLEL
) -> Optional[_io.TextIOWrapper]:
    """
    Return a file handle to filename using ``pbzip2``, ``bzip2``, or ``b2``
    module

    :param filename: The filename to open
    :param mode: The mode with which to open
    :param buff: Buffer size
    :param external: External code
    """
    if external is None or external == NORMAL:
        return bz2.open(filename, mode, buff)
    elif external == PROCESS:
        if not WHICH_BZIP2:
            return open_bz2(filename, mode, buff, NORMAL)
        if "r" in mode:
            return open_pipe("bzip2 -dc " + filename, mode, buff)
        elif "w" in mode:
            return open_pipe("bzip2 >" + filename, mode, buff)
    elif external == PARALLEL:
        if not WHICH_PBZIP2:
            return open_bz2(filename, mode, buff, PROCESS)
        if "r" in mode:
            return open_pipe("pbzip2 -dc " + filename, mode, buff)
        elif "w" in mode:
            return open_pipe("pbzip2 >" + filename, mode, buff)
    return None


def open_gz(
    filename: str, mode: str = "r", buff: int = 1024 * 1024, external: int = PARALLEL
) -> Optional[_io.TextIOWrapper]:
    """
    Return a file handle to filename using ``pigz``, ``gzip``, or ``gzip`` module

    :param filename: The filename to open
    :param mode: The mode with which to open
    :param buff: Buffer size
    :param external: External code
    """
    if external is None or external == NORMAL:
        return gzip.open(filename, mode)
    elif external == PROCESS:
        if not WHICH_GZIP:
            return open_gz(filename, mode, buff, NORMAL)
        if "r" in mode:
            return open_pipe("gzip -dc " + filename, mode, buff)
        elif "w" in mode:
            return open_pipe("gzip -9 >" + filename, mode, buff)
    elif external == PARALLEL:
        if not WHICH_PIGZ:
            return open_gz(filename, mode, buff, PROCESS)
        if "r" in mode:
            return open_pipe("pigz -dc " + filename, mode, buff)
        elif "w" in mode:
            return open_pipe("pigz >" + filename, mode, buff)
    return None


def open_xz(
    filename: str, mode: str = "r", buff: int = 1024 * 1024, external: int = PARALLEL
) -> Optional[_io.TextIOWrapper]:
    """
    Return a file handle to filename using either ``xz`` or ``lzma`` module

    :param filename: The filename to open
    :param mode: The mode with which to open
    :param buff: Buffer size
    :param external: External code
    """
    if external is None or external == NORMAL:
        return lzma.open(filename, mode)
    elif external == PROCESS:
        if not WHICH_XZ:
            return open_xz(filename, mode, buff, NORMAL)
        if "r" in mode:
            return open_pipe("xz -dc " + filename, mode, buff)
        else:
            return open_pipe("xz >" + filename, mode, buff)
    elif external == PARALLEL:
        return open_xz(filename, mode, buff, PROCESS)
    return None


def zopen(
    filename: str, mode: str = "r", buff: int = 1024 * 1024, external: int = PARALLEL
) -> _io.TextIOWrapper:
    """
    Open pipe, zipped, or unzipped file automagically

    |  ``external == 0``: normal zip libraries
    |  ``external == 1``: (zcat, gzip, xz) or (bzcat, bzip2)
    |  ``external == 2``: (pigz -dc, pigz) or (pbzip2 -dc, pbzip2)

    :param filename: The filename to open
    :param mode: The mode with which to open the file handle
    :param buff: Buffer size
    :param external: External process code usage
    :raises ValueError: If "r" and "w" in mode or neither in mode
    :return: The opened file handle
    """
    if "r" in mode == "w" in mode:
        raise ValueError("r or w must be in mode and not both")
    if filename.startswith("!"):
        return open_pipe(filename[1:], mode, buff)
    elif filename.endswith(".bz2"):
        return open_bz2(filename, mode, buff, external)
    elif filename.endswith(".gz"):
        return open_gz(filename, mode, buff, external)
    elif filename.endswith(".xz"):
        return open_xz(filename, mode, buff, external)
    else:
        return open(filename, mode, buff)


def extract_excel_sheet(
    fn: str,
    output_fn: str,
    sheet: str = "Sheet1",
    skip_nlines: int = 0,
    comment_prefix: Optional[str] = None,
    replacement_patterns: Optional[Mapping[str, str]] = {},
    **kwargs,
) -> None:
    r"""
    Extract a sheet from an Excel spreadsheet.

    :param fn: The spreadsheet filename
    :param output_fn: The output filename
    :param sheet: The sheet name to extract
    :param skip_nlines: Skip this many lines
    :param comment_prefix: Skip each line that begins with this string
    :param replacement_patterns: A mapping of patterns to replace; each key will
        be replaced by its value
    :param \**kwargs: Any keyword arguments to pass to csv.writer
    """
    import openpyxl

    if replacement_patterns:
        replacement_func = lambda x: (
            ""
            if x is None
            else reduce(
                lambda y, pattern_repl: re.sub(pattern_repl[0], pattern_repl[1], y),
                replacement_patterns.items(),
                str(x),
            )
        )
    else:
        replacement_func = lambda x: x
    wb = openpyxl.load_workbook(fn, read_only=True)
    ws = wb[sheet]
    with zopen(output_fn, "w") as csv_fh:
        w = csv.writer(csv_fh, **kwargs)
        for x, row in enumerate(ws.rows):
            if x < skip_nlines:
                continue
            if comment_prefix and row[0].value.startswith(comment_prefix):
                continue
            w.writerow([replacement_func(cell.value) for cell in row])


class CustomCSVDialect(csv.Dialect):
    delimiter = ","
    escapechar = "\\"
    lineterminator = "\n"
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False
    strict = False


def check_valid_output_directory(
    output_directory: str, overwrite: bool = False, create_directory: bool = False
) -> None:
    """
    Check if the given directory is valid for outputting.

    :param output_directory: The output directory to check
    :param overwrite: Overwrite the directory if it exists already
    :param create_directory: Create the directory
    :raises NotImplementedError: If ``output_directory`` exists,
        ``overwrite = True``, and ``output_directory`` is not a file or directory
    :raises OSError: If ``output_directory`` exists and ``overwrite = False``
    """
    if os.path.exists(output_directory):
        if overwrite:
            logger.info(
                f"{output_directory} already exists; deleting as overwrite was specified."
            )
            if os.path.isfile(output_directory):
                os.remove(output_directory)
            elif os.path.isdir(output_directory):
                rmtree(output_directory)
            else:
                raise NotImplementedError(
                    f"{output_directory} exists and isn't a file or directory...?"
                )
        else:
            raise OSError(f"{output_directory} already exists.")
    else:
        parent_dir = os.path.dirname(output_directory)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
    if create_directory:
        os.mkdir(output_directory)


def get_csv_fields(
    fn: str,
    sep: str = ",",
    sanitize: bool = False,
    sanitize_with: Optional[Iterable[tuple[str, str]]] = ((".", "_"), ("-", "_")),
) -> list[str]:
    """
    Get the column names from the first line of the specified file and
    optionally replaces an arbitrary sequence of strings to others

    :param fn: The path to the CSV to get column names from
    :param sep: The field separator
    :param sanitize: Whether to apply the string replacements in
        ``sanitize_with``
    :param sanitize_with: For each pair, replace the first string
        with the second
    """
    with zopen(fn, "rt") as fh:
        fields = next(fh).rstrip("\n").split(sep)
    if sanitize:
        return [
            reduce(lambda s, old_new: s.replace(*old_new), sanitize_with, field)
            for field in fields
        ]
    else:
        return fields


def sync_to_s3(dir_name: str, s3_path: str) -> None:
    """
    Syncs a directory to specific S3 bucket/path.

    :param dir_name: The directory to sync
    :param s3_path: The S3 path to sync to
    """
    subprocess.run(["aws", "s3", "sync", dir_name, s3_path], check=True)


def save_to_pickle(
    item: Any, fn: str | Path | BinaryIO, overwrite: bool = False
) -> None:
    """
    Persist the given object.

    :param item: Object to persist
    :param fn: File path, Path object, or binary file handle to save to
    :param overwrite: Whether to overwrite if the path exists
    """
    if isinstance(fn, _io.BufferedWriter):
        pickle.dump(item, fn)
    else:
        if Path(fn).exists():
            if overwrite:
                logger.debug(f"Path {fn} exists, but overwrite=True; will overwrite.")
            else:
                logger.warning(f"Path {fn} exists and overwrite=False.")
                return
        logger.info(f"Saving pickle to {fn}.")
        with zopen(fn, "wb") as fh:
            pickle.dump(item, fh)


@cache
def load_pickle(fn: str | Path, data_type: type | None = None) -> Any:
    """
    Load the persisted object and optionally confirm it's of the proper type.

    :param fn: The file name or Path to load
    :param data_type: The data type the pickle should contain
    :raises TypeError: If ``data_type`` is specified and not the correct format
    :return: The loaded pickle
    """
    with open(fn, "rb") as fh:
        obj = pickle.load(fh)
        if data_type:
            if not isinstance(obj, data_type):
                raise TypeError(f"{fn}'s data isn't an instance of {data_type}.")
        return obj
