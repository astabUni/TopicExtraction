import os
import glob
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
import re
import json

rootDirectory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
topicExtraction_resources = os.path.join(rootDirectory, "topicExtraction",
                                         "src", "main", "resources")
ldaFilePaths = glob.glob(os.path.join(topicExtraction_resources, "out", "lda",
                                      "*", "*.json"))
categorylinksFilePath = os.path.join(topicExtraction_resources, "in",
                                     "categorylinks.json")

def repeatwordcount(text):
    word_to_count = defaultdict(int)
    for i in text.split():
        word_to_count[i] += 1
    return word_to_count

# with open(categorylinksFilePath, "r", encoding="utf-8") as f:
#     cat_to_subcat = json.load(f)

cat_to_terms = {}
cat_to_word_count_dict = {}

# Get lda terms
term_regex = re.compile(r'"terms":\[(\".*)\]')
for ldaFilePath in ldaFilePaths:
    category = os.path.basename(os.path.dirname(ldaFilePath))
    terms = ""
    with open(ldaFilePath, "r", encoding="utf-8") as f:
        for line in f:
            terms += re.search(term_regex, line).group(1).replace(
                "\"", "").replace(","," ")
        cat_to_terms[category] = terms
        cat_to_word_count_dict[category] = repeatwordcount(terms)

# test data
cat_to_subcat = defaultdict(list)
cat_to_subcat["Computer hardware"].append("Embedded systems")
cat_to_subcat["Microprocessors"].append("Microcontrollers")
#

# Compare cat to subcat terms
for cat, subcats in cat_to_subcat.items():
    for subcat in subcats:
        if cat in cat_to_terms.keys() and subcat in cat_to_terms.keys():
            print(">>>>", cat, "-", subcat)
            print("\tcat>", cat_to_terms[cat])
            print("\tsubcat>", cat_to_terms[subcat])
            documents = (cat_to_terms[cat], cat_to_terms[subcat])
            tfidf_vectorizer = TfidfVectorizer()
            tfidf_matrix = tfidf_vectorizer.fit_transform(documents)
            print(cosine_similarity(tfidf_matrix), "\n")
            print(cosine_similarity(tfidf_matrix.transpose()))
