import logging
import subprocess


def extend_collection_chain(datastore, chain_name, new_run, logger=None):
    """
    Use subprocess to call the butler collection-chain command.

    Parameters
    ----------
    datastore : str
        Path to the datastore.
    chain_name : str
        Name of the collection chain to extend.
    new_run : str
        Name of the new run collection to add to the chain.
    logger : logging.Logger, optional
        Logger instance to use for logging.
    """
    # If no logger is provided, use the root logger
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        # Build the command
        cmd = [
            "butler",
            "collection-chain",
            datastore,
            chain_name,
            new_run,
            "--mode", "extend"
        ]

        # Execute the command and capture the output
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)

        # Log the output if successful
        logger.info(f"Successfully added '{new_run}' to '{chain_name}'.")
        logger.debug(f"Command output: {result.stdout}")

    except subprocess.CalledProcessError as e:
        # Log the error if the command fails
        logger.error(f"Failed to extend collection chain: {e}")
        logger.debug(f"Error output: {e.stderr}")
