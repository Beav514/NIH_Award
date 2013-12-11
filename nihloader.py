#TO DO: add fiscal year to NIH_SOURCE_FILE
# Solve this error:
#===============================================================================
# ERROR: Failed to load NIH source file RePORTER_PRJ_X_FY2011.xml 12/21/2012
# Traceback (most recent call last):
#   File "C:\Users\Britton\workspace\nih\nihloader.py", line 293, in <module>
#     sys.exit(main())
#   File "C:\Users\Britton\workspace\nih\nihloader.py", line 287, in main
#     load_fiscal_year_range(fiscal_year_start, fiscal_year_end, database_file_name, is_store_terms, is_store_investigators)
#   File "C:\Users\Britton\workspace\nih\nihloader.py", line 238, in load_fiscal_year_range
#     for award in award_file.awarditer():
#   File "C:\Users\Britton\workspace\nih\nihaward.py", line 255, in awarditer
#     for event, elem in ET.iterparse(file["xml_file"], events=("start", "end")):
#   File "C:\Python33\lib\xml\etree\ElementTree.py", line 1776, in __next__
#     raise e
#   File "C:\Python33\lib\xml\etree\ElementTree.py", line 1788, in __next__
#     self._parser.feed(data)
# xml.etree.ElementTree.ParseError: not well-formed (invalid token): line 1607749, column 35
#===============================================================================

import sys
from nihaward import NIHAwardFile
import os
import sqlite3 as sqlite


def create_nih_tables(database_file_name, is_create_term_table=False, is_create_pi_table=False):
    """Create all NIH tables necessary for storing an NIH award project."""
    #there is no except clause so that errors will automatically be re-raised.
    con = sqlite.connect(database_file_name)
    
    with con:
        cur = con.cursor()
        
        cur.execute("""CREATE TABLE IF NOT EXISTS NIH_SOURCE_FILE (
            nih_source_file_id INTEGER PRIMARY KEY,
            source_file_name TEXT NOT NULL,
            source_file_date TEXT NOT NULL,
            CONSTRAINT NIH_SOURCE_FILE_UK1 UNIQUE (source_file_name, source_file_date)
        )""")
            
        cur.execute("""CREATE TABLE IF NOT EXISTS NIH_PROJECT (
            nih_project_id INTEGER PRIMARY KEY,
            application_id INTEGER NOT NULL,
            nih_source_file_id INTEGER NOT NULL,
            source_file_row_number INTEGER NOT NULL,
            activity TEXT,
            administering_ic TEXT,
            application_type TEXT,
            arra_funded TEXT,
            award_notice_date TEXT,
            budget_start TEXT,
            budget_end TEXT,
            cfda_code INTEGER,
            core_project_num TEXT,
            ed_inst_type TEXT,
            foa_number TEXT,
            full_project_num TEXT,
            funding_ics TEXT,
            fy TEXT,
            ic_name TEXT,
            nih_spending_cats TEXT,
            org_city TEXT,
            org_country TEXT,
            org_dept TEXT,
            org_district INTEGER,
            org_duns TEXT,
            org_fips TEXT,
            org_name TEXT,
            org_state TEXT,
            org_zipcode TEXT,
            phr TEXT,
            program_officer_name TEXT,
            project_start_date TEXT,
            project_end_date TEXT,
            project_title TEXT,
            serial_number TEXT,
            study_section TEXT,
            study_section_name TEXT,
            subproject_id TEXT,
            suffix TEXT,
            support_year TEXT,
            total_cost NUMERIC,
            total_cost_sub_project NUMERIC,
            CONSTRAINT NIH_PROJECT_UK1 UNIQUE (application_id, nih_source_file_id)
        )""")
        
        if is_create_term_table:
            cur.execute("""CREATE TABLE IF NOT EXISTS NIH_PROJECT_TERM (
                nih_project_term_id INTEGER PRIMARY KEY,
                nih_project_id INTEGER NOT NULL,
                term TEXT NOT NULL,
                CONSTRAINT NIH_PROJECT_TERM_UK1 UNIQUE (nih_project_id, term),
                CONSTRAINT NIH_PROJECT_TERM_FK1 FOREIGN KEY (nih_project_id)
                    REFERENCES NIH_PROJECT (nih_project_id)
            )""")
    
        if is_create_pi_table:
            cur.execute("""CREATE TABLE IF NOT EXISTS NIH_PROJECT_INVESTIGATOR (
                nih_project_investigator_id INTEGER PRIMARY KEY,
                nih_project_id INTEGER NOT NULL,
                pi_id TEXT NOT NULL,
                pi_name TEXT NOT NULL,
                CONSTRAINT NIH_PROJECT_INVESTIGATOR_UK1 UNIQUE (nih_project_id, pi_id),
                CONSTRAINT NIH_PROJECT_INVESTIGATOR_FK1 FOREIGN KEY (nih_project_id)
                    REFERENCES NIH_PROJECT (nih_project_id)
            )""")


def insert_source_file(cur, source_file_name, source_file_date):
    """Insert a source file record to denote the source of a set of projects."""
    cur.execute("INSERT INTO NIH_SOURCE_FILE (source_file_name, source_file_date) VALUES(?, ?)",
                (source_file_name, source_file_date))
    
    return cur.lastrowid
    

def insert_project(cur, nih_award_file, nih_source_file_id):
    """Insert all project attributes (except terms and project investigators) from a particular NIH award file item."""
    cur.execute("""INSERT INTO NIH_PROJECT (
                    application_id,
                    nih_source_file_id,
                    source_file_row_number,
                    activity,
                    administering_ic,
                    application_type,
                    arra_funded,
                    award_notice_date,
                    budget_start,
                    budget_end,
                    cfda_code,
                    core_project_num,
                    ed_inst_type,
                    foa_number,
                    full_project_num,
                    funding_ics,
                    fy,
                    ic_name,
                    nih_spending_cats,
                    org_city,
                    org_country,
                    org_dept,
                    org_district,
                    org_duns,
                    org_fips,
                    org_name,
                    org_state,
                    org_zipcode,
                    phr,
                    program_officer_name,
                    project_start_date,
                    project_end_date,
                    project_title,
                    serial_number,
                    study_section,
                    study_section_name,
                    subproject_id,
                    suffix,
                    support_year,
                    total_cost,
                    total_cost_sub_project 
                ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?)""", 
                (nih_award_file.application_id,  
                 nih_source_file_id,
                 nih_award_file.source_file_row_number,
                 nih_award_file.activity,
                 nih_award_file.administering_ic,
                 nih_award_file.application_type,
                 nih_award_file.arra_funded,
                 nih_award_file.award_notice_date,
                 nih_award_file.budget_start,
                 nih_award_file.budget_end,
                 nih_award_file.cfda_code,
                 nih_award_file.core_project_num,
                 nih_award_file.ed_inst_type,
                 nih_award_file.foa_number,
                 nih_award_file.full_project_num,
                 nih_award_file.funding_ics,
                 nih_award_file.fy,
                 nih_award_file.ic_name,
                 nih_award_file.nih_spending_cats,
                 nih_award_file.org_city,
                 nih_award_file.org_country,
                 nih_award_file.org_dept,
                 nih_award_file.org_district,
                 nih_award_file.org_duns,
                 nih_award_file.org_fips,
                 nih_award_file.org_name,
                 nih_award_file.org_state,
                 nih_award_file.org_zipcode,
                 nih_award_file.phr,
                 nih_award_file.program_officer_name,
                 nih_award_file.project_start,
                 nih_award_file.project_end,
                 nih_award_file.project_title,
                 nih_award_file.serial_number,
                 nih_award_file.study_section,
                 nih_award_file.study_section_name,
                 nih_award_file.subproject_id,
                 nih_award_file.suffix,
                 nih_award_file.support_year,
                 nih_award_file.total_cost,
                 nih_award_file.total_cost_sub_project))
        
    return cur.lastrowid
    
    
def insert_project_terms(cur, nih_award_file, nih_project_id):
    """Insert all project terms from a particular NIH award file item."""
    for term in nih_award_file.project_terms:
        cur.execute("INSERT INTO NIH_PROJECT_TERM(nih_project_id, term) VALUES(?,?)", (nih_project_id, term))
    
    
def insert_project_investigators(cur, nih_award_file, nih_project_id):
    """Insert all project investigators from a particular NIH award file item."""
    for pi in nih_award_file.principal_investigators:
        cur.execute("INSERT INTO NIH_PROJECT_INVESTIGATOR(nih_project_id, pi_id, pi_name) VALUES(?,?,?)", (nih_project_id, pi["pi_id"], pi["pi_name"]))


def insert_award_file(cur, nih_award_file, nih_source_file_id, is_insert_term, is_insert_pi):
    """For a particular NIH award file item, insert all parts of it unless specified otherwise."""
    nih_project_id = insert_project(cur, nih_award_file, nih_source_file_id)
    
    if is_insert_term:
        insert_project_terms(cur, nih_award_file, nih_project_id)
        
    if is_insert_pi:
        insert_project_investigators(cur, nih_award_file, nih_project_id)
    
    return nih_project_id
    


def load_fiscal_year_range(fiscal_year_start, fiscal_year_end, database_file_name = 'nih_database.db', is_store_terms=False, is_store_investigators=False):
    """For a range of fiscal years, download into a sqlite database all NIH award data."""
    # Create sqlite tables
    create_nih_tables(database_file_name, is_store_terms, is_store_investigators)
    
    # Set up the NIH award file object to get files for a range of years
    award_file = NIHAwardFile(fiscal_year_start, fiscal_year_end)
    
    # Download and unzip xml files from NIH ExPORTER website
    award_file.get_files_in_fiscal_year_range()
  
    con = sqlite.connect(database_file_name)
     
    try:
        with con:
            cur = con.cursor() 
            # Loops through each line in each file within the fiscal year range.
            for award in award_file.awarditer():
                
                # Are we at the beginning of a new file? 0 = Yes.
                if award.source_file_row_number == 0:
                    
                    #if there was an open transaction (perhaps from a previous file) commit it.
                    con.commit()
                    
                    # Assume that this source file has not been loaded into the database.
                    is_source_file_loaded = False
                    
                    try:
                        # Create a new source file row in the database
                        cur_file_id = insert_source_file(cur, award.source_file_name, award.source_file_date)
                        con.commit()
                    except sqlite.IntegrityError:
                        # Assume that the IntegrityError resulted from a unique key violation. So lookup the key.
                        cur.execute("SELECT nih_source_file_id FROM NIH_SOURCE_FILE WHERE source_file_name = ? AND source_file_date = ?"
                                    ,(award.source_file_name, award.source_file_date))
                        row = cur.fetchone()
                        cur_file_id = row[0]
                        
                        # The source file record exists. Are there any projects? If so that indicates, this has already been loaded.
                        cur.execute("SELECT COUNT(*) FROM NIH_PROJECT WHERE nih_source_file_id = ?", (cur_file_id,))
                        row = cur.fetchone()
                        is_source_file_loaded = row[0] > 0
                     
                     
                # Insert all specified parts of the award file IF the file hasn't already been loaded.
                # If it has been loaded, then skip the insert until we come to something new.
                if not is_source_file_loaded:
                    insert_award_file(cur, award, cur_file_id, is_store_terms, is_store_investigators)
    
                    
    except:
        print("ERROR: Failed to load NIH source file", award.source_file_name, award.source_file_date)
        raise

def main():
    #change current working directory
    os.chdir('C:/Users/Britton/Documents/test')
    
    database_file_name = 'nih_database.db'
    fiscal_year_start = '2011'
    fiscal_year_end = '2011'
    # For sake of brevity do not store, terms or investigators
    is_store_terms = False
    is_store_investigators = False
    
    load_fiscal_year_range(fiscal_year_start, fiscal_year_end, database_file_name, is_store_terms, is_store_investigators)
    
    print('COMPLETED')


if __name__ == "__main__":
    sys.exit(main())