import pandas as pd
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

if __name__ == '__main__':

    frequent_hosts = pd.read_csv('data/frequentHosts.csv')
    frequent_paths = pd.read_csv('data/frequentPath.csv', delimiter='^')
    code_status = pd.read_csv('data/httpStatusStats.csv')
    stats = pd.read_csv('data/stats.csv')

    fig = plt.figure(constrained_layout=True, figsize=(20,10))
    specs = gridspec.GridSpec(ncols=2, nrows=2, figure=fig) ## Declaring 2x2 figure.

    ax1 = fig.add_subplot(specs[0, 0]) ## First Row
    ax2 = fig.add_subplot(specs[0, 1]) ## First Row second column
    ax3 = fig.add_subplot(specs[1, 0]) ## Second Row First Column
    ax4 = fig.add_subplot(specs[1, 1]) ## Second Row Second Colums

    frequent_hosts = frequent_hosts[:5]
    frequent_paths = frequent_paths[:5]
    code_status = code_status.sort_values(by=['count'], ascending=False)
    stats = stats.rename(columns={"Average": "Average_content_size", "Max": "Max_package_size", "Min": "Min_package_size", "unique_host": "num_unique_hosts"})

    ax1.barh(frequent_hosts["host"], frequent_hosts["count"], color="tab:blue")
    ax1.set_ylabel("hosts")
    ax1.set_title("Most frequent hosts")

    ax2.barh(frequent_paths["path"], frequent_paths["count"], color="tab:red")
    ax2.set_ylabel("paths")
    ax2.set_title("Most frequent paths")
    
    # HTTP status
    cell_text = []
    for row in range(len(code_status)):
        cell_text.append(code_status.iloc[row])

    table = ax3.table(cellText=cell_text, colLabels=code_status.columns, loc='center', colLoc='center', rowLoc='center', cellLoc='center',edges='open')
    for (row, col), cell in table.get_celld().items():
        if (row == 0):
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
    ax3.axis('off')
    ax3.set_title("Http status code")

    # STATS
    cell = []
    for row in range(len(stats)):
        cell.append(stats.iloc[row])

    table2 = ax4.table(cellText=cell, colLabels=stats.columns, loc='center',     colLoc='left', rowLoc='left', cellLoc='left',edges='open')
    table2.set_fontsize(14)
    table2.scale(1.5, 1.5)
    for (row, col), cell in table2.get_celld().items():
        if (row == 0):
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
    ax4.axis('off')
    ax4.set_title("Some additional statistics")

    plt.show()