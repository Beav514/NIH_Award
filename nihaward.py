'''
This module is used for processing the U.S. National Institute of Health's (NIH's) list of yearly grant awards
that are available online through their ExPORTER system. This system offers both CSV and XML version,
however, only the XML version offers the additional one-to-many fields for project terms and 
principal investigators. Therefore, this processer only use the XML version.

Given a particular fiscal year range, this will find the files on the NIH's ExPORTER website and download the files
in their zipped format. It will then unpack the zipped file into the current working directory. Once downloaded
there is an iterator to run through all of the files.

Note: in past fiscal years, there is only one file per fiscal year. In the current fiscal year there is one file
per week.

@author: Britton Ward (brittonward.com)

'''


try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import re
from bs4 import BeautifulSoup
import urllib.request
import shutil
import os 
import zipfile



class NIHAward:
    """Class that holds fields found within an US National Institute of Health (NIH) grant award."""
    
    def __init__(self, source_file_name, source_fiscal_year, source_file_date, source_file_row_number):
        """
        Reset all of the public attributes for this class which are just the names of the tags in 
        the original file but in all lower case.
        
        """
        
        # This is the name of the file where this was found and the row number it was found on where 0 is first row.
        self.source_file_name = source_file_name
        self.source_fiscal_year = source_fiscal_year
        self.source_file_row_number = source_file_row_number
        self.source_file_date = source_file_date
        
        # The names below are identical to the XML element names used within the file except they are lower case.
        self.application_id = '' # This uniquely identifies an instance of an award.
        self.activity = ''
        self.administering_ic = ''
        self.application_type = ''
        self.arra_funded = ''
        self.budget_start = ''
        self.budget_end = ''
        self.foa_number = ''
        self.full_project_num = ''
        self.funding_ics = ''
        self.fy = ''
        self.nih_spending_cats = ''
        self.org_city = ''
        self.org_country = ''
        self.org_district = ''
        self.org_duns = ''
        self.org_dept = ''
        self.org_fips = ''
        self.org_state = ''
        self.org_zipcode = ''
        self.ic_name = ''
        self.org_name = ''
        self.project_title = ''
        self.project_start = ''
        self.project_end = ''
        self.phr = ''
        self.serial_number = ''
        self.study_section = ''
        self.study_section_name = ''
        self.support_year = ''
        self.suffix = ''
        self.subproject_id = ''
        self.total_cost = 0
        self.total_cost_sub_project = 0
        self.core_project_num = ''
        self.cfda_code = ''
        self.program_officer_name = ''
        self.ed_inst_type = ''
        self.award_notice_date = ''
        self.funding_mechanism = ''
        
        # These two lists do not use the same name as the source file.
        self.project_terms = [] #Original Name: PROJECT_TERMSX
        self.principal_investigators = [] #Original Name: PIS

    

class NIHAwardFile:
    """
    For a given fiscal year, gets the zipped XML file from the NIH website, then unzips it into the current working directory. 
    NOTE: The current fiscal year is often multiple files which currently we have no way to handle, though in future we will.
    
    """
    fiscal_year_start = ''
    fiscal_year_stop = ''
    xml_files = []
    NIH_EXPORTER_SITE = 'http://exporter.nih.gov/'
    NIH_EXPORTER_PAGE = 'ExPORTER_Catalog.aspx'
    
    #patterns for various regular expressions that allow for leading and trailing spaces.
    RE_FISCAL_YEAR = "^\s*(19|20)\d{2}\s*$"
    RE_8_DIGIT_DATE = "^\s*\d{2}/\d{2}/\d{4}\s*$"
    
    # Exceptions for tags we don't want to directly copy as single attributes of NIHAward() in the awarditer() function.
    _AVOIDED_XML_TAGS = ['PIS', 'PI', 'PI_NAME', 'PI_ID', 'PROJECT_TERMSX', 'TERM']
        
    def __init__(self, fiscal_year_start, fiscal_year_stop = None):
        """initializes the NIHAwardFile class for a particular fiscal year range"""
        
        # Error check: does the fiscal year look like a year
        # if not (fiscal_year.isdigit() and len(fiscal_year) == 4):
        if re.match(self.RE_FISCAL_YEAR, fiscal_year_start) is None:
            raise RuntimeError('fiscal_year_start must be 4 digits')
        
        if fiscal_year_stop is None:
            fiscal_year_stop = fiscal_year_start
            
        if re.match(self.RE_FISCAL_YEAR, fiscal_year_stop) is None:
            raise RuntimeError('fiscal_year_stop must be 4 digits')
        
        if int(fiscal_year_start.strip()) > int(fiscal_year_stop.strip()):
            raise RuntimeError('fiscal_year_stop cannot come before fiscal_year_start')
        
        self.fiscal_year_start = int(fiscal_year_start.strip())
        self.fiscal_year_stop = int(fiscal_year_stop.strip())
        
        
    def find_zip_file_urls(self):
        """Returns a list of all XML-based ExPORTER zip files for this instance's fiscal year."""
        urls = []
        
        page = urllib.request.urlopen(self.NIH_EXPORTER_SITE + self.NIH_EXPORTER_PAGE)
        
        soup = BeautifulSoup(page)
        
        table = soup.find_all(name="table", id="ctl00_ContentPlaceHolder1_ProjectData_dgProjectData")
        
        if len(table) == 0:
            raise RuntimeError('HTML table of ExPORTER files could not be found.')
        elif len(table) > 1:
            raise RuntimeError('More than one HTML table of ExPORTER files found. Unsure of which to use.')
        
        table_rows = table[0].find_all('tr')
        
        #skip the header row
        iter_table_rows = iter(table_rows)
        next(iter_table_rows)
        
        #iterate through all of the table row tags (tr)
        for tr in iter_table_rows:
            fy = tr.find_all(text=re.compile(self.RE_FISCAL_YEAR))[0].strip()
            
            href = tr.find_all(href=re.compile("^XMLData/final/RePORTER_PRJ_X_FY"))[0]
            href = self.NIH_EXPORTER_SITE + href.attrs.get("href")
            
            file_date = tr.find_all(text=re.compile(self.RE_8_DIGIT_DATE))[0].strip()
            
            if int(fy) >= self.fiscal_year_start and int(fy) <= self.fiscal_year_stop:
                urls.append( {"fiscal_year":fy, "file_date": file_date, "zip_file": href})
            
        return urls
        
        
    def get_xml_file_from_url(self, url):
        """
        This downloads the url parameter's corresponding zipped xml
        file from the NIH ExPORTER website and unzips the xml file. The zipped versions of the files
        are deleted immediately after being opened. It returns the location of the 
        freshly unzipped file
        
        """
        # Start with no internal path to the xml file.
        xmlfilename = ''
        
        # Download the zip file into the current working directory for this computer.
        localfile = os.path.basename(url)
        
        # NIH nicely names the XML file within the zip file using the same basename.
        # In case we had previously downloaded and unzipped the file previously,
        # let's check whether the XML file is already present.
        if os.path.isfile(os.path.splitext(os.path.join(os.getcwd(), localfile))[0] + '.xml'):
            # There is a corresponding XML file of that name so do nothing but send back this file name.
            xmlfilename = os.path.splitext(localfile)[0] + '.xml'
        else:
            with urllib.request.urlopen(url) as response, open(localfile, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            
            # Unzip the file and get the xml file's location.
            with zipfile.ZipFile(localfile, 'r') as myzip:
                for myzipinfo in myzip.infolist():
                    # If there is anything in the xmlfilename when starting this loop or if the loop continues
                    # that means there may be multiple xml files in which case I don't know what to do with that.
                    if xmlfilename != '':
                        raise RuntimeError('Downloaded zip file contained multiple XML files.')
                        
                    #there usually is  only one file but in case there is some non-xml file ignore it
                    if os.path.splitext(myzipinfo.filename)[1] == '.xml':
                        xmlfilename = myzipinfo.filename
                        myzip.extract(myzipinfo)
    
            # Delete the zipfile because we won't be using it anymore.
            os.remove(localfile)
        
        return xmlfilename


    def get_files_in_fiscal_year_range(self):
        """Find files and download them for the given fiscal year range from NIH website."""
        
        # If there are already XML files, then no need to go further.
        if len(self.xml_files) == 0:
            # No XML files are here so pull them from the NIH website.
            
            urls = self.find_zip_file_urls()
            
            # Check that we only have at least one.
            if len(urls) == 0:
                raise RuntimeError('No files found for this fiscal year range.')
            
            #for each url download the file, unzip the xml portion, and delete the zip.
            #Record the each file's location in our internal variable xml
            for url in urls:
                xml_file = self.get_xml_file_from_url(url["zip_file"])
                url_copy = url.copy()
                url_copy.update({"xml_file": xml_file})
                self.xml_files.append(url_copy)
                print("Downloaded XML file to current directory:", xml_file)
            
        
    def delete_downloaded_xml_files(self):
        """Delete all downloaded XML files."""
        for f in self.xml_files:
            os.remove(f["xml_file"])
    
    
    def awarditer(self):
        """Generator function for processing a single line of the NIH Award ExPORTER file at a time."""
        
        # Process each file located within fiscal year range.
        for file in self.xml_files:
            print('Starting to process file:', file["xml_file"])
            #reset the row number since we've started a new file
            row_number = 0
            
            # Loop through the possibly large XML file using SAX-style method found in ElementTree.
            for event, elem in ET.iterparse(file["xml_file"], events=("start", "end")):
                
                if event == 'start':
                    if elem.tag == 'row':
                        #at beginning of each new row tag create a new award object
                        award = NIHAward(os.path.basename(file["xml_file"]), file["fiscal_year"], file["file_date"], row_number)
                    elif elem.tag == 'PI':
                        #at start of each row's PI tag, reset all values.
                        pi_name = ''
                        pi_id = ''
                        
                elif event == 'end':
                    if elem.tag == 'row':
                        # Discard the row element and everything inside of it now that we've finished processing it.
                        elem.clear()
                        row_number += 1
                        #return control to the caller.
                        yield award
                    
                    elif elem.text is None:
                        #do nothing if the value is not a value
                        pass
                        
                    elif elem.tag == 'TERM':
                        award.project_terms.append(elem.text)
                    elif elem.tag == 'PI_NAME':
                        pi_name = elem.text
                    elif elem.tag == 'PI_ID':
                        pi_id = elem.text
                    elif elem.tag == 'PI':
                        if len(pi_name) > 0 or len(pi_id) > 0:
                            award.principal_investigators.append({'pi_name': pi_name, 'pi_id': pi_id})
                    elif elem.tag not in self._AVOIDED_XML_TAGS:
                        # Since I named most of the attributes (except for the above ones) in this class after the file's xml tags this is easy.
                        setattr(award, elem.tag.lower(), elem.text)
                        
            print('Finished processing file:', file["xml_file"])
            print('Total rows:', row_number)
           



