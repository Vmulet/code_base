import matplotlib.pyplot as plt
import pandas as pd

def new_pdf_page():
    """
    Creates a new PDF page with a fixed size.

    Returns:
        matplotlib.figure.Figure: A new figure object with the specified size.
    """
    return plt.figure(figsize=(12, 15))

def add_text_to_fig(text, fig, index, position=111):
    """
    Adds text to a specified figure at a given position.

    Args:
        text (str): The text to add to the figure.
        fig (matplotlib.figure.Figure): The figure to which the text will be added.
        index (int): The font size of the text.
        position (int): The subplot position (default is 111).

    Returns:
        None
    """
    ax = fig.add_subplot(position)
    ax.text(0.5, 0.9, text, size=index, ha="center", wrap=True)
    ax.axis("off")

def next_file(parquet_files):
    """
    Retrieves the next Parquet file from a list of files and reads it into a DataFrame.

    Args:
        parquet_files (list): A list of Parquet file paths.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the next Parquet file.
    """
    if not hasattr(next_file, "counter"):
        next_file.counter = 0
    next_file.counter += 1
    return pd.read_parquet(parquet_files[next_file.counter], engine="pyarrow")