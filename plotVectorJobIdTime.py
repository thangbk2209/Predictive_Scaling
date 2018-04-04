import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import read_csv
import numpy as np
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import pandas as pd


# I = pd.Index(['cpu_rate','mem_usage','disk_io_time','disk_space'], name="metrics")
mldf = read_csv('results/vectorTimeJobid.csv', header=None, index_col=False)
df = pd.DataFrame(mldf.values)
print df
# revels = rd.pivot("Flavour", "Packet number", "Contents")
# mask = np.zeros_like(df, dtype=np.bool)
# mask[np.triu_indices_from(mask)] = True

f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(150, 20, as_cmap=True)

sns.heatmap(df, cmap=cmap, vmax=1.0, center=0,
            square=True, linewidths=.5, cbar_kws={"shrink": .5})

pp = PdfPages('results/VectorTime.pdf')
# pp.savefig()
plt.savefig('results/VectorTime.png')
pp.close()