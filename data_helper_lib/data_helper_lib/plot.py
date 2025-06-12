import matplotlib.pyplot as plt
import numpy as np
from format_data import syncData

def save_figure(fig, pdf):
    """
    Saves the given figure to a PDF file and closes the figure.

    Args:
        fig (matplotlib.figure.Figure): The figure to save.
        pdf (PdfPages): The PDF file to save the figure into.

    Returns:
        None
    """
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)

def plot_data(
    data,
    fig,
    index,
    ylabel,
    data_column=-1,
    color="tab:blue",
    option="",
    numericYticks=False,
):
    """
    Plots a line graph of the specified data column on the given figure.

    Args:
        data (pd.DataFrame): The data to plot.
        fig (matplotlib.figure.Figure): The figure to plot on.
        index (int): The subplot index for the figure.
        ylabel (str): The label for the y-axis.
        data_column (int): The index of the data column to plot (default is -1).
        color (str): The color of the line (default is "tab:blue").
        option (str): Marker style for the plot (default is "").
        numericYticks (bool): Whether to convert y-axis ticks to numeric values (default is False).

    Returns:
        None
    """
    ycolumn = data[data_column]
    if numericYticks:
        ycolumn = data[data.columns[data_column]].astype(float)
    ax = fig.add_subplot(index)
    ax.plot(data["acquisition_time"].values, ycolumn, color, marker=option)
    ax.set_title(f"Plot of {data_column}")
    ax.set_xlabel("acquisition_time")
    ax.set_ylabel(ylabel)
    ax.grid(True)
    #ax.set_xticks(
    #    ticks=range(0, len(data), max(1, len(data) // 10)),
    #    labels=data["acquisition_time"][:: max(1, len(data) // 10)],
    #    rotation=45,
    #)

def plot_bar_chart(data, fig, index, ylabel, title, data_column=-1, color="tab:blue"):
    """
    Plots a bar chart of the specified data column with acquisition times as x-axis labels.

    Args:
        data (pd.DataFrame): The data to plot.
        fig (matplotlib.figure.Figure): The figure to plot on.
        index (int): The subplot index for the figure.
        ylabel (str): The label for the y-axis.
        title (str): The title of the bar chart.
        data_column (int): The index of the data column to plot (default is -1).
        color (str): The color of the bars (default is "tab:blue").

    Returns:
        None
    """
    ycolumn = data[data_column].astype(float)
    acquisition_times = data["acquisition_time"].astype(str)
    ax = fig.add_subplot(index)
    ax.bar(acquisition_times, ycolumn, color=color, edgecolor="black")
    ax.set_title(title)
    ax.set_xlabel("Acquisition Time")
    ax.set_ylabel(ylabel + "(W)")
    ax.grid(True)
    ax.tick_params(axis='x', rotation=45)
    ax.set_yticks(np.linspace(ycolumn.min(), ycolumn.max(), num=15))
    ax.set_ylim(0, ycolumn.max() * 1.05)

def multi_plot(dataArray, fig, index, ylabel, labels, data_column=-1, option=""):
    """
    Plots multiple datasets on the same subplot for comparison.

    Args:
        dataArray (list): A list of DataFrames to plot.
        fig (matplotlib.figure.Figure): The figure to plot on.
        index (int): The subplot index for the figure.
        ylabel (str): The label for the y-axis.
        labels (list): A list of labels for each dataset.
        data_column (int): The index of the data column to plot (default is -1).
        option (str): Marker style for the plot (default is "").

    Returns:
        None
    """
    ax = fig.add_subplot(index)
    dataArray = syncData(dataArray)
    color = [
        "tab:blue",
        "tab:orange",
        "tab:green",
        "tab:red",
        "tab:purple",
        "tab:brown",
        "tab:pink",
        "tab:gray",
        "tab:olive",
        "tab:cyan",
    ]
    for plotIndex, data in enumerate(dataArray):
        ax.plot(
            data["acquisition_time"].values,
            data[data.columns[data_column]],
            color[plotIndex],
            marker=option,
            label=labels[plotIndex % len(labels)],
        )
    ax.set_xticks(
        ticks=range(0, len(dataArray[0]), max(1, len(dataArray[0]) // 10)),
        labels=dataArray[0]["acquisition_time"][:: max(1, len(dataArray[0]) // 10)],
        rotation=45,
    )
    ax.set_title(f"Plot of {dataArray[0].columns[data_column]}")
    ax.set_xlabel("acquisition_time")
    ax.set_ylabel(ylabel)
    ax.grid(True)
    ax.legend()