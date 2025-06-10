import matplotlib.pyplot as plt
import pandas as pd

def new_pdf_page():
    """Create a new PDF page with a fixed size."""
    return plt.figure(figsize=(12, 15))

def add_text_to_fig(text, fig, index, position=111):
    """Add text to a figure."""
    ax = fig.add_subplot(position)
    ax.text(0.5, 0.9, text, size=index, ha="center", wrap=True)
    ax.axis("off")

def next_file(parquet_files):
    if not hasattr(next_file, "counter"):
        next_file.counter = 0
    next_file.counter += 1
    return pd.read_parquet(parquet_files[next_file.counter], engine="pyarrow")