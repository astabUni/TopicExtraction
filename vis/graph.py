from graphviz import Digraph
import os
import json

# TODO: change paths
RESOURCES_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)))

FILEPATH_GRAPH = os.path.join(RESOURCES_PATH, "data.gv")
MAXDEPTH = 2

with open(RESOURCES_PATH + "\Computer_hardware_categorylinks-d2.json", "r",
          encoding="utf-8") as f:
    cat_to_subcat = json.load(f)

with open(RESOURCES_PATH + "\Computer_hardware_category_to_depth-d2.json",
          "r",
          encoding="utf-8") as f:
    cat_to_depth = json.load(f)

# TODO: use real topics
placeholder_topics = "Topic 1, Topic 2, Topic 3, Topic 4, Topic 5"


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
g.attr(overlap="false", splines="polyline", layout="neato", sep="+50,50")
colors = {0: "#8aacb8",  1: "#add8e6", 2: "#d6ebf2"}

# Create nodes
for category, depth in cat_to_depth.items():
    if (depth <= MAXDEPTH):
        scaling = 1 / (4*(depth+1))
        g.node(category, fillcolor=colors[depth])
        g.node(category, label=linebreak(category, 27) + "\n\n" + linebreak(
            placeholder_topics, 27), color="black", shape="circle",
               style="filled", penwidth="2.0", fontname="Arial Bold",
               fontsize=str(128 * scaling),
               fixedsize="true", width=str(26.5 * scaling))

# Create edges
for cat, subcats in cat_to_subcat.items():
    for subcat in subcats:
        g.edge(cat, subcat)

g.edge_attr.update(weight="0.5", penwidth="2.0", ranksep="0.5")
g.view()
