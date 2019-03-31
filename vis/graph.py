import os
import glob
from collections import defaultdict
import re
import csv
import operator
from graphviz import Digraph
import json

rootDirectory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
topicExtraction_resources = os.path.join(rootDirectory, "topicExtraction",
                                         "src", "main", "resources")
ldaFilePaths = glob.glob(os.path.join(topicExtraction_resources, "out", "lda",
                                      "*", "*.json"))

ldaTopicDistributionFilePaths = glob.glob(os.path.join(
    topicExtraction_resources, "out", "lda", "*", "topicDistribution",
    "*.json"))

categorylinksFilePath = os.path.join(topicExtraction_resources, "in",
                                     "categorylinks.json")

categoryDepthFilePath = os.path.join(topicExtraction_resources, "in",
                                     "category_to_depth.json")

def repeatwordcount(text):
    word_to_count = defaultdict(int)
    for i in text.split():
        word_to_count[i] += 1
    return word_to_count

with open(categorylinksFilePath, "r", encoding="utf-8") as f:
    cat_to_subcat = json.load(f)

cat_to_terms = {}
cat_to_word_count_dict = {}

def getTopicDistributionMaxValue(str):
    id = re.search(id_regexp, line).group(1)
    values = re.search(values_regexp, line).group(1).split(",")
    maxvalue_index = 0
    maxvalue = 0.0
    for v in values:
        if float(v) > maxvalue:
            maxvalue_index = values.index(v)
            maxvalue = float(v)
    return (id, maxvalue_index, maxvalue)

term_regexp = re.compile(r'"terms":\[(\".*)\]')
termweights_regexp = re.compile(r'"termWeights":\[(.*?)\]')
id_regexp = re.compile(r'"id":"([0-9]*)"')
values_regexp = re.compile(r'"values":\[(.*)\]')
topic_regexp = re.compile(r'"topic":([0-9]*)')

bestTopics = {}
for ldaFilePath in ldaFilePaths:
    cat_path = os.path.dirname(ldaFilePath)
    category = os.path.basename(cat_path)
    topicDistribution_path = os.path.join(cat_path, "topicDistribution", "*.json")
    topicDistributionFilePaths = glob.glob(topicDistribution_path)
    id_idx_maxvalue = []
    for topicDistributionFile in topicDistributionFilePaths:
        with open(topicDistributionFile, "r", encoding="utf-8") as f:
            for line in f:
                id_idx_maxvalue.append(getTopicDistributionMaxValue(line))
    noun_count = {}
    nounsFilePath = glob.glob(os.path.join(topicExtraction_resources,
                                 "out", "nouns", category, "*.csv"))
    with open(nounsFilePath[0], "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for line in reader:
            noun_count[line[0]] = line[1]
    topics = {}
    with open(ldaFilePath, "r", encoding="utf-8") as f:
        for line in f:
            topic = int(re.search(topic_regexp, line).group(1))
            for (id, maxvalue_index, maxvalue) in id_idx_maxvalue:
                if topic == maxvalue_index:
                    topicTerms = re.search(term_regexp, line).group(1).replace(
                        "\"", "").split(",")
                    topicTermweights = re.search(termweights_regexp,
                                                   line).group(1).replace(
                        "\"", "").split(",")
                    for idx, topicTerm in enumerate(topicTerms):
                        if topicTerm in noun_count:
                            value = float(noun_count[topicTerm]) * \
                                    float(topicTermweights[idx])
                        else:
                            value = float(topicTermweights[idx])
                        topics[topicTerm] = value
    sorted_topics = dict(sorted(topics.items(), key=operator.itemgetter(
        1), reverse=True)[:5])
    bestTopics[category] = sorted_topics

with open(categoryDepthFilePath,
          "r",
          encoding="utf-8") as f:
    cat_to_depth = json.load(f)

def linebreak(str, maxlength):
    words = str.split(" ")
    newstr = ""
    linelength = 0
    for s in words:
        if linelength + len(s) < maxlength:
            newstr += s + " "
            linelength += len(s) + 1
        else:
            newstr += "\n" + s + " "
            linelength = len(s) + 1
    return newstr.strip()

g = Digraph(filename='data.gv', engine="sfdp", format="png")
g.attr(overlap="false", splines="polyline", layout="neato", sep="+70,70")
colors = {0: "#8aacb8",  1: "#add8e6", 2: "#d6ebf2"}

#Create nodes
for category, depth in cat_to_depth.items():
    if depth <= 2 and category in bestTopics.keys():
        scaling = 1 / (4*(depth+1))
        g.node(category, fillcolor=colors[depth])
        topics = linebreak(', '.join(bestTopics[category]), 27)
        g.node(category, label=linebreak(category, 27) + "\n\n" + topics, color="black", shape="circle",
               style="filled", penwidth="2.0", fontname="Arial Bold",
               fontsize=str(128 * scaling),
               fixedsize="true", width=str(26.5 * scaling))

# Create edges
for cat, subcats in cat_to_subcat.items():
    for subcat in subcats:
        g.edge(cat, subcat)

g.edge_attr.update(weight="0.5", penwidth="2.0", ranksep="0.5")
g.view()
