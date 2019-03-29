from graphviz import Digraph
import os
import json

# TODO: change paths
RESOURCES_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)))

FILEPATH_GRAPH = os.path.join(RESOURCES_PATH, "data.gv")

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
            newstr += s + "\n"
            linelength = 0
    return newstr.strip()


g = Digraph(filename='data.gv', engine="sfdp", format="png")
g.attr(overlap="false", repulsiveforce="10", splines="spline",
       layout="neato", sep="+40,40")

nodes = []

# Collect nodes to create
for cat, subcats in cat_to_subcat.items():
    nodes.append((cat, placeholder_topics))

# Create nodes
for (category, placeholder_topics) in nodes:
    d = cat_to_depth[category] + 1
    # scaling = 2.5 / (d*d)
    scaling = 1 / (3*d)
    g.node(category, label=linebreak(category, 25) + "\n\n" + linebreak(
        placeholder_topics, 25), color="black", fillcolor="lightblue",
           shape="circle", style="filled", penwidth="2.0", fontname="Arial Bold",
		   fontsize=str(48 * scaling), fixedsize="true", width=str(13 * scaling))

# Create edges
for cat, subcats in cat_to_subcat.items():
    for subcat in subcats:
        g.edge(cat, subcat)

g.edge_attr.update(weight="0.5", penwidth="2", ranksep="0.5", minlen="2.5")

g.view()
