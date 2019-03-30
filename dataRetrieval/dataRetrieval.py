from collections import defaultdict
from lxml import etree
from queue import PriorityQueue
from bz2file import BZ2File
import re
import os.path
import csv
import json
import time

# User inputs
FILENAME_CSV = "categorylinkspage-join.csv"
FILENAME_ARTICLES_XML = "enwiki-20190101-pages-articles-multistream.xml"
ROOTS = ["Computer_hardware"]
MAXDEPTH = 2

# Create paths
RESOURCES_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "resources")
OUTPUT_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                           "output", "articles-d" + str(MAXDEPTH))

if not os.path.isdir(OUTPUT_PATH):
    try:
        os.makedirs(OUTPUT_PATH)
    except OSError as e:
        print("Creating directories %s failed" % OUTPUT_PATH)
        raise e
    else:
        print("Successfully created directories %s" % OUTPUT_PATH)

# Set input file paths
INPUT_FILEPATH_CSV_DUMP = os.path.join(RESOURCES_PATH, FILENAME_CSV)
INPUT_FILEPATH_ARTICLES_XML_BZ2 = os.path.join(RESOURCES_PATH,
                                               FILENAME_ARTICLES_XML)

# Set output file paths
FILEPATH_ARTICLEID_TITLE_CAT_CSV = os.path.join(RESOURCES_PATH,
                                                "articles_" + FILENAME_CSV + \
                                                ".csv")
FILEPATH_CAT_TO_SUBCATS_FULL_JSON = os.path.join(RESOURCES_PATH,
                                                 "categories_" +
                                                 FILENAME_CSV.split(".")[0] + \
                                                 ".json")
FILEPATH_CAT_TO_DEPTH = os.path.join(OUTPUT_PATH, "category_to_depth-d" + str(
    MAXDEPTH))
FILEPATH_CAT_TO_SUBCATS = os.path.join(OUTPUT_PATH,
                                       "categorylinks-d" + str(MAXDEPTH))
FILEPATH_CAT_TO_ARTICLEIDS = os.path.join(OUTPUT_PATH,
                                          "category_to_articleids-d" + str(
                                              MAXDEPTH))
FILEPATH_CAT_TO_ARTICLEIDS_COLLECTED = os.path.join(OUTPUT_PATH,
                                                    "category_to_articleids_collected-d" + str(
                                                        MAXDEPTH))


def printTime(start, end):
    elapsed_time = end - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time - minutes * 60)
    print("Elapsed time: %sm %ss" % (minutes, seconds))


regexps_dict = {
    '\[\[+(Category:)(.*?)\]+\]|\[\[(?:[^\]|]*\|)?([^\]|]*)\]\]': r'\3',
    '\[\[+(File:)(.*?)\]+\]': "",
    '&(amp;)?#([a-zA-Z0-9]*);': ""
}

regexp_title = re.compile('[^\w\s-]')


def normalize(str):
    # return re.sub(regexp_title, "", str).replace("_"," ")
    return str.replace("_", " ")


def clean_text(text):
    for k, v in regexps_dict.items():
        text = re.sub(k, v, text)
    return text.encode('ascii', 'ignore')


def save_as_csv(dict, filepath):
    with open(filepath, "w", encoding="latin-1") as csv_file:
        csvwriter = csv.writer(csv_file, delimiter=",", lineterminator="\n",
                               quoting=csv.QUOTE_ALL)
        for k, vs in dict.items():
            if not isinstance(vs, (list,)):
                vs = [vs]
            csvwriter.writerow([k] + vs)
        print("%s saved." % filepath)


def save_as_json(dict, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(dict, f)
        print("%s saved." % filepath)


def csvdump_extractor(inputfile_csv, outputfile_articles_csv):
    print(
        "Extracting all categories and articles from %s\n..." % inputfile_csv)
    start = time.time()
    try:
        with open(inputfile_csv, "r", encoding="latin-1") as csv_file, open(
                outputfile_articles_csv, "w",
                encoding="utf-8") as new_csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            dialect.escapechar = "\\"
            dialect.quoting = csv.QUOTE_MINIMAL
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            category_to_subcategory = defaultdict(list)
            csvwriter = csv.writer(new_csv_file, delimiter=",", escapechar=" ",
                                   lineterminator="\n",
                                   quoting=csv.QUOTE_ALL)
            for line in reader:
                try:
                    # page_id = 0, page_namespace = 1, page_title = 2, cl_to = 15
                    cat = normalize(line[15])
                    if line[1] == "14":
                        subcat = normalize(line[2])
                        category_to_subcategory[cat].append(subcat)
                        category_to_subcategory[subcat]
                    if line[1] == "0":
                        csvwriter.writerow(
                            [line[0], line[2].replace("\\", "\\\\"), cat])
                except IOError:
                    print("Skipped line: %s" % line)
                    continue
            end = time.time()
            printTime(start, end)
            print("%s saved." % outputfile_articles_csv)
            return category_to_subcategory
    except FileNotFoundError as e:
        print("Inputfile not found:", inputfile_csv)
        raise e


def getcategorydepths(cat_to_subcats, roots, maxdepth):
    print("\nCollecting all subcategories for %s (Max depth: %s)" % (
        ROOTS, maxdepth))
    cat_depth = defaultdict(int)
    queue = PriorityQueue()
    for cat in roots:
        if cat in cat_to_subcats.keys():
            cat_depth[cat] = 0
            queue.put((0, cat))
        else:
            print("Category \'%s\' not found" % cat)
    if len(cat_depth) == 0:
        return cat_depth
    #cat_depth[roots] = 0
    while not queue.empty():
        (d, category) = queue.get()
        if d >= maxdepth:
            break
        try:
            subcategories = cat_to_subcats[category]
            for s in subcategories:
                if s not in cat_depth.keys():
                    cat_depth[s] = d + 1
                    queue.put((d + 1, s))
        except KeyError:
            print(
                "KeyError: Skipped category \'%s\'" % category)
            cat_depth.pop(category, None)
            continue
    print("Found %s categories for starting categories %s, max depth %s" % (
        len(cat_depth), ROOTS, maxdepth))
    return cat_depth


def getsubcats(cat_to_subcats, cat_depth, maxdepth):
    categorylinks = {}
    for cat in cat_depth.keys():
        if cat in cat_to_subcats.keys():
            if cat_depth[cat] < maxdepth:
                categorylinks[cat] = cat_to_subcats[cat]
            else:
                categorylinks[cat] = []
    return categorylinks


def articleid_collector(path, cat_to_depth):
    print("Collecting article ids for %s (Max depth: %s)..." % (
        ROOTS, MAXDEPTH))
    start = time.time()
    try:
        with open(path, "r", encoding="latin-1") as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            dialect.escapechar = "\\"
            # dialect.quotechar = "|"
            dialect.quoting = csv.QUOTE_MINIMAL
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            category_to_articleids = defaultdict(list)
            count = 0
            for line in reader:
                try:
                    page_id = line[0]
                    cl_to = line[2]
                    if cl_to in cat_to_depth.keys():
                        category_to_articleids[cl_to].append(page_id)
                        count += 1
                except:
                    print("Skipped line: %s" % line)
                    continue
            end = time.time()
            print("Article ids collected: %s" % count)
            printTime(start, end)
            return category_to_articleids
    except FileNotFoundError as e:
        print("Inputfile not found:\n", path)
        raise e


# XML Headers
Header = "http://www.mediawiki.org/xml/export-0.10/"
Tagheader = "{" + Header + "}"
Tnamespaces = Tagheader + "namespaces"
Tpage = Tagheader + "page"
Ttitle = Tagheader + "title"
Tid = Tagheader + "id"
Trev = Tagheader + "revision"
Ttext = Tagheader + "text"


def create_subelem(parent, name, content):
    newchild = etree.SubElement(parent, name)
    newchild.text = content
    return newchild


def create_page(elem, title_path, id_path, text_path, newfile):
    id = id_path(elem)[0].text
    title = normalize(title_path(elem)[0].text)
    text = clean_text(text_path(elem)[0].text)
    newpage = etree.Element("page")
    create_subelem(newpage, "title", title)
    create_subelem(newpage, "id", id)
    create_subelem(etree.SubElement(newpage, "revision"), "text", text)
    newfile.write(newpage, pretty_print=True)


def create_namespace(elem, newfile):
    newnamespaces = etree.Element("namespaces")
    with newfile.element("siteinfo"):
        for child in elem:
            nc = create_subelem(newnamespaces, "namespace", child.text)
            for k, v in child.attrib.items():
                nc.attrib[k] = v
        newfile.write(newnamespaces, pretty_print=True)


def articlecollector(path_articles_xml, outpath_articles, articleids):
    print("\nCollecting articles for \'%s\' from %s..." % (
        ROOTS, path_articles_xml))
    title_path = etree.ETXPath("child::" + Ttitle)
    id_path = etree.ETXPath("child::" + Tid)
    text_path = etree.ETXPath("child::" + Trev + "/" + Ttext)
    extracted_ids = set()
    start = time.time()
    try:
        with BZ2File(outpath_articles, "w", compresslevel=9) as file, \
                etree.xmlfile(file, encoding="utf-8") as newfile, \
                newfile.element("mediawiki",
                                xmlns=Header):
            context = etree.iterparse(path_articles_xml,
                                      events=("end",),
                                      tag={Tnamespaces, Tpage})
            for action, elem in context:
                if elem.tag == Tpage and id_path(elem)[0].text in articleids:
                    create_page(elem, title_path, id_path, text_path, newfile)
                    extracted_ids.add(id_path(elem)[0].text)
                elif elem.tag == Tnamespaces:
                    create_namespace(elem, newfile)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
    except FileNotFoundError as e:
        print(e.filename, "not found")
        raise e
    end = time.time()
    printTime(start, end)
    return extracted_ids


def removeuncollected(cat_to_artids, artids_collected):
    for cat, ids in cat_to_artids.items():
        for id in ids[:]:
            if id not in artids_collected:
                cat_to_artids[cat].remove(id)
    return cat_to_artids


def dict_values_to_set(dict):
    s = set()
    for k, vs in dict.items():
        for v in vs:
            s.add(v)
    return s


def main():
    # Extract relevant information from .csv dump, otherwise load
    # category to subcategory dict
    if not (os.path.isfile(FILEPATH_ARTICLEID_TITLE_CAT_CSV) and
            os.path.isfile(
                FILEPATH_CAT_TO_SUBCATS_FULL_JSON)):
        cat_to_subcats_full = csvdump_extractor(INPUT_FILEPATH_CSV_DUMP,
                                                FILEPATH_ARTICLEID_TITLE_CAT_CSV)
        save_as_json(cat_to_subcats_full, FILEPATH_CAT_TO_SUBCATS_FULL_JSON)
    else:
        with open(FILEPATH_CAT_TO_SUBCATS_FULL_JSON, "r") as f:
            cat_to_subcats_full = json.load(f)

    # For a given starting category and maximum depth get all subcategories
    # and depth
    roots = []
    for cat in ROOTS:
        roots.append(normalize(cat))
    cat_to_depth = getcategorydepths(cat_to_subcats_full, roots, MAXDEPTH)

    save_as_json(cat_to_depth, FILEPATH_CAT_TO_DEPTH + ".json")
    # save_as_csv(cat_to_depth, FILEPATH_CAT_TO_DEPTH + ".csv")

    cat_to_subcats = getsubcats(cat_to_subcats_full, cat_to_depth, MAXDEPTH)

    save_as_json(cat_to_subcats, FILEPATH_CAT_TO_SUBCATS + ".json")
    # save_as_csv(cat_to_subcats, FILEPATH_CAT_TO_SUBCATS + ".csv")

    # Collect article ids for wanted categories
    if not os.path.isfile(FILEPATH_CAT_TO_ARTICLEIDS + ".json"):
        cat_to_articleids = articleid_collector(
            FILEPATH_ARTICLEID_TITLE_CAT_CSV,
            cat_to_depth)
        save_as_json(cat_to_articleids, FILEPATH_CAT_TO_ARTICLEIDS + ".json")
        # save_as_csv(cat_to_articleids, FILEPATH_CAT_TO_ARTICLEIDS + ".csv")
    else:
        with open(FILEPATH_CAT_TO_ARTICLEIDS + ".json", "r") as f:
            print("\nLoading article ids")
            cat_to_articleids = json.load(f)
    cat_to_depth.clear()
    cat_to_subcats.clear()

    articleids = dict_values_to_set(cat_to_articleids)

    # Find and copy wanted articles
    filepath_articles_xml_bz2 = os.path.join(OUTPUT_PATH, "articles-d"
                                             + str(MAXDEPTH) + ".xml.bz2")
    articleids_collected = articlecollector(INPUT_FILEPATH_ARTICLES_XML_BZ2,
                                            filepath_articles_xml_bz2,
                                            articleids)

    cat_to_articleids_collected = removeuncollected(cat_to_articleids,
                                                    articleids_collected)
    save_as_json(cat_to_articleids_collected, FILEPATH_CAT_TO_ARTICLEIDS +
                 "_collected.json")
    # save_as_csv(cat_to_articleids_collected, FILEPATH_CAT_TO_ARTICLEIDS +
    #             "_collected.json")

    print("%s unique articleIDs found" % len(articleids))
    print("%s articles found and extracted" % len(articleids_collected))


if __name__ == "__main__":
    main()
