import numpy as np
import matplotlib.pyplot as plt
plt.style.use('seaborn-darkgrid')
plt.rcParams.update({'font.size': 20})

def make_barchart(ax, x_label_list, y_data, y_label, title, normalize=True):
    x_vals = np.arange(len(x_label_list))
    if normalize:
        temp = np.array(y_data)
        temp = temp/np.sum(temp)
        y_data = list(temp)

    ax.set_position([0.13, 0.27, 0.8, 0.66])
    ax.bar(x_vals, y_data, tick_label=x_label_list, align='center', alpha=0.75)
    ax.set_ylabel(y_label)
    ax.set_ylim((0,0.20))
    ax.set_title(title)
    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_ha('right')

def make_hor_barchart(ax, set_position, x_label_list, y_data, y_label, title, normalize=True):
    x_vals = np.arange(len(x_label_list))
    
    if normalize:
        temp = np.array(y_data)
        temp = temp/np.sum(temp)
        y_data = list(temp)

    ax.set_position(set_position)
    ax.barh(x_vals, y_data, tick_label=x_label_list, align='center', alpha=0.75)
    ax.set_xlabel(y_label)
    ax.set_xlim((0,0.20))
    ax.set_title(title)


def make_hist(ax, line_name, y_data, y_limit, x_label, x_limit, num_bins=50, normalize=False, cumulative=False):
    ax.hist(y_data, bins = num_bins, density=normalize, cumulative=cumulative, label=line_name)
    ax.set_ylabel('pmf' if normalize else 'Count')
    ax.set_ylim((0, 1) if cumulative else y_limit)
    ax.legend()
    ax.set_xlim(x_limit)
    ax.set_xlabel(x_label)

def make_boxplot(ax, array_of_values, y_lim, label_lst, y_label,title):
    ax.boxplot(array_of_values,labels=label_lst)
    ax.set_ylabel(y_label)
    ax.set_xlabel('Last Text Cleaning Step')
    ax.set_title(title)
    ax.set_ylim(y_lim)
    ax.set_position([0.16, 0.16, 0.70, 0.70])

def make_ci_lineplot(ax, line_name, y_data, y_lim, label_lst, y_label, title, alpha=0.05):
    x_values = np.arange(0,7,1)
    y_values = np.mean(y_data, axis=0)
    upper_ci = np.percentile(y_data, 100*(1-(alpha/2)), axis=0)
    lower_ci = np.percentile(y_data, 100*(alpha/2), axis=0)

    
    ax.plot(x_values, y_values, label=line_name)
    ax.fill_between(x_values, upper_ci, lower_ci,alpha=0.50)

    ax.legend()
    ax.set_xlabel('Last Text Cleaning Step')
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.set_ylim(y_lim)
    ax.set_position([0.16, 0.16, 0.70, 0.70])

def save_fig(fig, saved_figure_name):
    plt.savefig(f'../images/{saved_figure_name}', dpi=300)
    plt.close(fig)