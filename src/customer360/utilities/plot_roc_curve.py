import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from sklearn.metrics import auc, roc_curve


def plot_roc_curve(
    y_true,
    y_score,
    filepath=None,
    line_width=2,
    width=10,
    height=8,
    title=None,
    colors=("#FF0000", "#000000"),
):
    """
    Purpose: To plot the ROC curve
    :param y_true:
    :param y_score:
    :param filepath:
    :param line_width:
    :param width:
    :param height:
    :param title:
    :param colors:
    :return:
    """
    """
    Saves a ROC curve in a file or shows it on screen.
    :param y_true: actual values of the response (list|np.array)
    :param y_score: predicted scores (list|np.array)
    :param filepath: if given, the ROC curve is saved in the desired filepath. It should point to a png file in an
    existing directory. If not specified, the curve is only shown (str)
    :param line_width: number indicating line width (float)
    :param width: number indicating the width of saved plot (float)
    :param height: number indicating the height of saved plot (float)
    :param title: if given, title to add to the top side of the plot (str)
    :param colors: color specification for ROC curve and diagonal respectively (tuple of str)
    :return: None
    """
    fpr, tpr, _ = roc_curve(y_true=y_true, y_score=y_score)
    auc_score = auc(fpr, tpr)

    sns.set_style("whitegrid")
    fig = plt.figure(figsize=(width, height))
    major_ticks = np.arange(0, 1.1, 0.1)
    minor_ticks = np.arange(0.05, 1, 0.1)
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xticks(major_ticks)
    ax.set_yticks(major_ticks)
    ax.set_xticks(minor_ticks, minor=True)
    ax.set_yticks(minor_ticks, minor=True)
    ax.grid(which="both", axis="both")
    ax.grid(which="minor", alpha=0.2)
    ax.grid(which="major", alpha=0.5)
    ax.tick_params(which="major", direction="out", length=5)
    plt.plot(
        fpr,
        tpr,
        color=colors[0],
        lw=line_width,
        label="ROC curve (AUC = {:.4f})".format(
            auc_score
        ),  # getting decimal points in auc roc curves
    )
    plt.plot([0, 1], [0, 1], color=colors[1], lw=line_width, linestyle="--")
    plt.xlim([-0.001, 1.001])
    plt.ylim([-0.001, 1.001])
    plt.xlabel("False positive rate", fontsize=15)
    plt.ylabel("True positive rate", fontsize=15)
    if title:
        plt.title(title, fontsize=30, loc="left")
    plt.legend(loc="lower right", frameon=True, fontsize="xx-large", fancybox=True)
    plt.tight_layout()
    if filepath:
        plt.savefig(filepath, dpi=70)
        plt.close()
    else:
        plt.show()
