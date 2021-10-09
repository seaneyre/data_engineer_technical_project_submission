import gzip
import json
import csv
import re
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """Creates and returns connection with SQLite database.

    Args:
        db_file (str): name of SQLite database.

    Returns:
        SQLite Connection Object: Connection to SQLite database
    """    
    
    conn = None
   
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def build_soc_lookup(soc_lookup_file):
    """Transforms input soc_lookup_file csv file into a lookup dict for use in mapping ONET to SOC5.

    Args:
        soc_lookup_file (str): Location of ONET to SOC mapping csv file.

    Returns:
        dict: Lookup dict containing onet:soc5 relationships.
    """

    # opens soc5 mapping csv
    reader = csv.DictReader(open(soc_lookup_file))
    # init lookup dict
    soc_lookup = {}
    for row in reader:
        # create dict with onet as key, soc5 as value
        soc_lookup[row['onet']] = row['soc5']
    return soc_lookup

def build_soc_hierarchy(soc_hierarchy_file):
    """Transforms input soc_hierarchy_file csv file into a lookup dict for use in mapping SOCX to SOCY.

    Args:
        soc_hierarchy_file (str): Location of SOC hierarchy csv file.

    Returns:
        dict: Lookup dict containing child:parent relationships for each SOC level.
    """    
    
    # returns child to parent dictionaries for each level of hierarchy
    reader = csv.DictReader(open(soc_hierarchy_file))

    soc_hierarchy = {1: {}, 2: {}, 3 : {}, 4 : {}, 5 : {}}

    for row in reader:
        level = int(row['level'])
        child = row['child']
        parent = row['parent']
        soc_hierarchy[level][child] = parent
    
    return soc_hierarchy

def get_parent_soc(input_level, output_level, input_soc, soc_hierarchy):
    """Returns SOC for specified output_level and input_soc. E.g. for SOC5 43-3021, output_level=2 returns 43-0000.

    Args:
        input_level (int): SOC level of the input_soc. E.g. for 43-3021 input_level=5. 
        output_level (int): SOC level of the desired output.
        input_soc (str): SOC for which you want to return the parent of.
        soc_hierarchy (dict): Generate using build_soc_hierarchy function.

    Returns:
        str: Output SOC for specified output_level and input_soc.
    """

    # loop through levels of soc hierarchy, start at input_level, end at output_level -1.
    for level in range(input_level, output_level, -1):
        try:
            input_soc = soc_hierarchy[level][input_soc]
        # when input_soc has no parent entry in soc_hierarchy, return None.
        except KeyError:
            input_soc = None
            break
        
    output_soc = input_soc
    
    return output_soc

def create_postings_table(conn, if_exists='ignore'):
    """Creates posting table in SQLite database for output.

    Args:
        conn (SQLite Connection Object): Connection to SQLite database
        if_exists (str, optional): Instruction if database already contains 'POSTINGS' table. Options: 'ignore' does nothing, 'replace' drops and re-creates table. Defaults to 'ignore'.
    """    

    cursor = conn.cursor()

    if if_exists == 'ignore':
        sql ='''CREATE TABLE IF NOT EXISTS POSTINGS(
        body TEXT,
        title TEXT,
        expired DATE,
        posted DATE,
        state TEXT,
        city TEXT,
        onet TEXT,
        soc5 TEXT,
        soc TEXT
        )'''
        cursor.execute(sql)
        conn.commit()

    if if_exists == 'replace':
        sql ='DROP TABLE POSTINGS'
        cursor.execute(sql)

        sql ='''CREATE TABLE POSTINGS(
        body TEXT,
        title TEXT,
        expired DATE,
        posted DATE,
        state TEXT,
        city TEXT,
        onet TEXT,
        soc5 TEXT,
        soc2 TEXT
        )'''
        cursor.execute(sql)
        conn.commit()


def main(input_data='sample.gz', output_db='output.db'):
    
    print('Script running...')
    
    # build lookup and hierarchy for use in data processing
    soc_lookup = build_soc_lookup('map_onet_soc.csv')
    soc_hierarchy = build_soc_hierarchy('soc_hierarchy.csv')

    # connect to database and create postings table.
    conn = sqlite3.connect(output_db)
    create_postings_table(conn)

    # summary stats init
    n_docs_rem_html = 0    

    # open (not load) the file 
    with gzip.open(input_data, 'rt', encoding="utf8") as f:
        # stream data line by line
        for i, line in enumerate(f):            
            # reads line as json to create doc
            doc = json.loads(line)
            
            # strip html tags from body
            # checks if the body has html, removes it if so
            if re.sub('<[^<]+?>', '', doc['body']) != doc['body']:
                doc['body'] = re.sub('<[^<]+?>', '', doc['body'])
                n_docs_rem_html += 1                

            # inject soc5 based on onet mapping, uses soc_lookup dict 
            # which is generated by build_soc_lookup function
            soc5 = soc_lookup[doc['onet']]
            doc['soc5'] = soc5

            # inject soc2 based on soc5
            doc['soc2'] = get_parent_soc(5, 2, soc5, soc_hierarchy)

            # INSERT to POSTINGS table
            cursor = conn.cursor()
            sql ='''INSERT INTO POSTINGS VALUES(:body,:title,:expired,:posted,:state,:city,:onet,:soc5,:soc2)'''
            cursor.execute(sql, doc)
            conn.commit()

   
    # more summary stats
    # get count of active postings on 2017-02-01
    sql ="SELECT count(*) FROM POSTINGS WHERE posted <= '2017-02-01' AND expired >= '2017-02-01'"
    cursor.execute(sql)
    n_feb_postings = cursor.fetchall()[0][0]

    print(f'Script complete ({i} documents processed)')
    
    # print summary stats
    print('Count of documents with HTML stripped from body: ', n_docs_rem_html)
    print('Count of documents active on 2017-02-01: ', n_feb_postings)
    print('For count of documents by SOC2, please query the output database with this SQL: SELECT soc2, count(*) FROM POSTINGS GROUP BY soc2 ORDER BY count(*) DESC')
    
    conn.close()

if __name__ == "__main__":
    main()