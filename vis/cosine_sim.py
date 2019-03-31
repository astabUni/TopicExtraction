import glob
import json
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


rootDirectory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
topicExtraction_resources = os.path.join(rootDirectory, "topicExtraction",
                                         "src", "main", "resources")
ldaFilePaths = glob.glob(os.path.join(topicExtraction_resources, "out", "lda",
                                      "*", "*.json"))
categorylinksFilePath = os.path.join(topicExtraction_resources, "in",
                                     "categorylinks.json")


with open(categorylinksFilePath, "r", encoding="utf-8") as f:
    categorylinks = json.load(f)


cat_to_terms = {}
categories = {}


# Get lda terms
term_regex = re.compile(r'"terms":\[(\".*)\]')
for ldaFilePath in ldaFilePaths:
    category = os.path.basename(os.path.dirname(ldaFilePath))
    terms = ""
    with open(ldaFilePath, "r", encoding="utf-8") as f:
        for line in f:
            terms += re.search(term_regex, line).group(1).replace(
                "\"", "").replace(","," ")
            terms += " "
        cat_to_terms[category] = terms


# calc cosine similarity
for cat, subcats in categorylinks.items():
    for subcat in subcats:
        if cat in cat_to_terms.keys() and subcat in cat_to_terms.keys():
            documents = (cat_to_terms[cat], cat_to_terms[subcat])
            tfidf_vectorizer = TfidfVectorizer()
            tfidf_matrix = tfidf_vectorizer.fit_transform(documents)
            cosine_similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix)
            print(">>>>", cat, "-", subcat)
            print(cosine_similarities)



