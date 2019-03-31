import glob
import os
import matplotlib.pyplot as plt
import matplotlib.ticker
import pandas as pd

if not os.path.isdir("noun-plots"):
    os.mkdir("noun-plots")

rootDirectory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
topicExtraction_output = os.path.join(rootDirectory, "topicExtraction", "src",
                                      "main", "resources", "out")
nounsFilePaths = glob.glob(
    os.path.join(topicExtraction_output, "nouns", "*", "*.csv"))

for nounsFilePath in nounsFilePaths:
    category = os.path.basename(os.path.dirname(nounsFilePath))
    df = pd.read_csv(nounsFilePath)
    df.plot(title=category, kind='barh', x='nouns', y='count')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    formatter = matplotlib.ticker.StrMethodFormatter("{x:.0f}")
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.savefig("noun-plots/" + category + ".png", dpi=100)
    plt.close()