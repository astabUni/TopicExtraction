import glob
import os
import matplotlib.pyplot as plt
import matplotlib.ticker
import pandas as pd

if not os.path.isdir("noun-plots"):
    os.mkdir("noun-plots")
if not os.path.isdir("lda-plots"):
    os.mkdir("lda-plots")

rootDirectory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
topicExtraction_output = os.path.join(rootDirectory, "topicExtraction", "src",
                                      "main", "resources", "out")
nounsFilePaths = glob.glob(
    os.path.join(topicExtraction_output, "nouns", "*", "*.csv"))

for nounsFilePath in nounsFilePaths:
    #name = nounsFilePath.split("\\")[1]
    category = os.path.basename(os.path.dirname(nounsFilePath))
    df = pd.read_csv(nounsFilePath)
    # more than 10? less?
    df_top10 = df.nlargest(10, "count")
    #print(df.nlargest(2, "count"))
    df_top10.plot(title=category, kind='barh', x='nouns', y='count')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    # locator = matplotlib.ticker.MultipleLocator(5)
    # plt.gca().xaxis.set_major_locator(locator)
    formatter = matplotlib.ticker.StrMethodFormatter("{x:.0f}")
    plt.gca().xaxis.set_major_formatter(formatter)
    #plt.show()
    plt.savefig("noun-plots/" + category + ".png", dpi=100)


ldaFilePaths = glob.glob(os.path.join(topicExtraction_output, "lda", "*",
                                      "*.json"))

for ldaFilePath in ldaFilePaths:
    category = os.path.basename(os.path.dirname(ldaFilePath))
    if category == "Computer hardware":
        df = pd.read_json(ldaFilePath) #change to csv
        df.plot(title=category, kind='barh', x='terms', y='termWeights')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.axis([0.024, 0.026, None, None])
        # locator = matplotlib.ticker.MultipleLocator(0.0005)
        # plt.gca().xaxis.set_major_locator(locator)
        plt.tight_layout()
        # plt.show()
        plt.savefig("lda-plots/" + category + ".png", dpi=100)