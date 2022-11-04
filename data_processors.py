#############################
#                           #
# Data processors module.   #
#                           # 
#############################

# pandas.
from pandas import DataFrame, Series, json_normalize, merge

# rdflib.
from rdflib import URIRef, RDF, Graph, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

# json.
import json

# Progress lib.
from progress.bar import ChargingBar
from progress.spinner import Spinner

# constants.
from const import EMOJI_FIRE, ENDPOINT

# common utilities.
from common_utils import csv_to_df, json_to_df

# URIs.
from URIs import * 


#############################
#                           #
# DataProcessor Base Class. #
#                           #
#############################
class DataProcessor(object):
    def __init__(self) -> None:
        
        self.publications_df: DataFrame = DataFrame()
        self.authors_df: DataFrame      = DataFrame()
        self.references_df: DataFrame   = DataFrame()
        self.venues_df: DataFrame       = DataFrame()
        self.publishers_df: DataFrame   = DataFrame()

    #
    # set_publications_df(self, _publication_df: DataFrame): -> None.
    #
    def set_publications_df(self, _publications_df):
        # Check if the Graph is empty.
        if len(_publications_df) < 1 :
            print('-- WARN: Publications Data Frame is empty :(')

        self.publications_df = _publications_df

    #
    # get_publications_df(self): DataFrame.
    #
    def get_publications_df(self) -> DataFrame:
        return self.publications_df

    #
    # set_authors_df(self, _authors_df: DataFrame): -> None.
    #
    def set_authors_df(self, _authors_df: DataFrame) -> None:
        if len(_authors_df) < 1:
            print('-- WARN: Authors Data Frame is empty :(')
        
        self.authors_df = _authors_df

    #
    # get_authors_df(self): DataFrame.
    #
    def get_authors_df(self) -> DataFrame:
        return self.authors_df

    #
    # set_publishers_df(self, _publishers_df: DataFrame): -> None.
    #
    def set_publishers_df(self, _publishers_df: DataFrame) -> None:
        if len(_publishers_df) < 1:
            print('-- WARN: Publishers Data Frame is empty :(')
        
        self.publishers_df = _publishers_df

    #
    # get_publishers_df(self): DataFrame.
    #
    def get_publishers_df(self):
        return self.publishers_df

    #
    # set_references_df(self, _references_df: DataFrame): -> None.
    #
    def set_references_df(self, _references_df: DataFrame) -> None:
        if len(_references_df) < 1:
            print('-- WARN: References Data Frame is empty :(')
        
        self.references_df = _references_df

    #
    # get_references_df(self): DataFrame.
    #
    def get_references_df(self):
        return self.references_df

    #
    # set_venues_df(self, _venues_df: DataFrame): -> None.
    #
    def set_venues_df(self, _venues_df: DataFrame) -> None:
        if len(_venues_df) < 1:
            print('-- WARN: Venues Data Frame is empty :(')
        
        self.venues_df = _venues_df

    #
    # get_venues_df(self): -> DataFrame.
    #
    def get_venues_df(self) -> DataFrame:
        return self.venues_df

    #
    # data_frames_has_been_built(self): -> bool.
    #
    def data_frames_has_been_built(self) -> bool:
        if len(self.get_publications_df()) > 0 and len(self.get_authors_df()) > 0 and len(self.get_publishers_df()) > 0 and len(self.get_references_df()) > 0 and len(self.get_venues_df()) > 0 :
            return True
        else: 
            return False
        
    ############################################################################
    #
    # publicationsDfBuilder(_csv_f_path: string): -> None.
    #
    def publicationsDfBuilder(self, _csv_f_path: str) -> None:

        dtype = {
            'id'                 : 'string',
            'title'              : 'string',
            'type'               : 'string',
            'publication-year'   : 'int',
            'issue'              : 'string',
            'volume'             : 'string',
            'chapter'            : 'string',
            'pubblication_venue' : 'string',
            'venue_type'         : 'string',
            'publisher'          : 'string',
            'event'              : 'string'
        }

        publications_df = csv_to_df(_csv_f_path, dtype) 
        
        self.set_publications_df(publications_df)



    ############################################################################
    #
    # referencesDfBuilder(_json_f_path: string): -> None.
    #
    def referncesDfBuilder(self, _json_f_path: str) -> None:

        try:
            data_frame = json_to_df(_json_f_path)

            # Read from json file only authors objects.
            references = data_frame['references']

            #
            # Create the Refernces DataFrame from json.
            # Data Frame will be expoloded in two col named 'doi' and 'cites'.
            #
            references_df = DataFrame(references.items(), columns = ['doi','cites']).explode('doi')
            
            #
            # Convert refernces_df DataFrame object to a JSON string and
            # deserialize it to a Python object,
            # finally, normalize semi-structured JSON data into a 
            # flat table (DataFrame).
            #
            references_df = json_normalize(json.loads(references_df.to_json(orient = 'records')))

            # Rename columns.
            references_df.rename(
                columns = {
                    'references.doi':'doi',
                    'references.cites':'cites',
                }, 
                inplace = True
            )

            self.set_references_df(references_df)
        except Exception:
            raise 

    ############################################################################
    #
    # venuesDfBuilder(_json_f_path: string): -> None.
    #
    def venuesDfBuilder(self, _json_f_path: str) -> None:        
    
        try:
            data_frame = json_to_df(_json_f_path)

            # Read from json file only authors objects.
            venues = data_frame['venues_id']

            #
            # Create the Venues DataFrame from json.
            # Data Frame will be expoloded in two col named 'doi' and 'venues_id'.
            #
            #venues_df = DataFrame(venues.items(), columns = ['doi','ids']).explode('doi')
            venues_df = DataFrame(venues.items(), columns = ['doi','ids']).explode('ids')
            
            #
            # Convert references_df DataFrame object to a JSON string and
            # deserialize it to a Python object,
            # finally, normalize semi-structured JSON data into a 
            # flat table (DataFrame).
            #
            venues_df = json_normalize(json.loads(venues_df.to_json(orient = 'records')))

            # Rename columns.
            venues_df.rename(
                columns = {
                    'venues.doi':'doi',
                    'venues.ids':'ids',
                }, 
                inplace = True
            )

            self.set_venues_df(venues_df)
        except Exception:
            raise 



    ############################################################################
    #
    # authorsDfBuilder(_json_f_path: string): -> None.
    #
    def authorsDfBuilder(self, _json_f_path: str) -> None:
        
        try:
            data_frame = json_to_df(_json_f_path) 
        
            # Read from json file only authors objects.
            authors = data_frame['authors']

            #
            # Create the authors DataFrame from json.
            # Data Frame will be expoloded in two col named 'doi' and 'author'.
            #
            authors_df = DataFrame(authors.items(), columns = ['doi','author']).explode('author')

            #
            # Convert authors_df DataFrame object to a JSON string and
            # deserialize it to a Python object,
            # finally, normalize semi-structured JSON data into a 
            # flat table (DataFrame).
            #
            authors_df = json_normalize(json.loads(authors_df.to_json(orient = 'records')))
 
            # Rename columns.
            authors_df.rename(
                columns = {
                    'author.family':'family_name',
                    'author.given':'given_name',
                    'author.orcid':'orc_id'
                }, 
                inplace = True
            )   

            self.set_authors_df(authors_df)
        except Exception:
            raise 
   
   
                                
    ############################################################################
    #
    # publishersDfBuilder(_json_f_path: string): -> None.
    #
    def publishersDfBuilder(self, _json_f_path: str) -> None:
                
        try:
            data_frame = json_to_df(_json_f_path)
            
            # Read from json file only publisher objects.
            publishers = data_frame['publishers']
            
            #
            # Create the authors DataFrame from json.
            # Data Frame will be expoloded in two col named 'id' and 'publishers'.
            #                            
            publishers_df = DataFrame(publishers.items(), columns=['id','publishers'])
                                                
            #
            # Convert publishers_df DataFrame object to a JSON string and
            # deserialize it to a Python object,
            # finally, normalize semi-structured JSON data into a 
            # flat table (DataFrame).
            #
            publishers_df = json_normalize(json.loads(publishers_df.to_json(orient='records')))
            
            # Rename columns.
            publishers_df.rename(
                columns = {
                    'publishers.id':'publisher_id',
                    'publishers.name':'publisher_name'
                }, 
                inplace = True
            )
            
            # Delete the 'id col.
            publishers_df = publishers_df.drop('id', axis=1)  
            
            self.set_publishers_df(publishers_df)
                        
        except Exception:
            raise 


###########################
#                         #
# RdbDataProcessor Class. #
#                         #
###########################
class RdbDataProcessor(DataProcessor):

    def __init__(self, _db_path: str) -> None:
        super().__init__()

        # Rdb path. 
        self.db_path: str = _db_path
        
        # Publication.
        self.publication_journal_article_df: DataFrame   = DataFrame()
        self.publication_book_chapter_df: DataFrame      = DataFrame()
        self.publication_proceedings_paper_df: DataFrame = DataFrame()

        # Venue.
        self.venue_journal_df: DataFrame    = DataFrame()
        self.venue_joined_df:DataFrame      = DataFrame()
        self.venue_book_df: DataFrame       = DataFrame()
        self.venue_proceedings_df:DataFrame = DataFrame()
        
        # Create an empty dictionary to store venue names.
        self.venue_names_dict : dict       = {}
 
        # Person.
        self.person_df: DataFrame = DataFrame()

        # Create an empty dictionary to store persons.
        self.person_internal_id_dict: dict = {}

    # Publication.
    def set_publication_journal_article_df(self, _data_frame: DataFrame) -> None:
        # Drop useless columns.
        self.publication_journal_article_df = _data_frame.drop([
            'chapter',
            'publisher',
            'event',
            'type',
            'publication_venue',
            'venue_type'
        ], axis=1, inplace=True)

    def get_publication_journal_article_df(self) -> DataFrame:
        return self.publication_journal_article_df


    def set_publication_book_chapter_df(self, _data_frame: DataFrame) -> None:
        # Drop useless columns.
        self.publication_book_chapter_df = _data_frame.drop([
            'issue',
            'volume',
            'publisher',
            'event',
            'type',
            'publication_venue',
            'venue_type'
        ], axis=1, inplace=True)

    def get_publication_book_chapter_df(self) -> DataFrame:
        return self.publication_book_chapter_df


    def set_publication_proceedings_paper_df(self, _data_frame: DataFrame) -> None:
        # Drop useless columns.
        self.publication_proceedings_paper_df = _data_frame.drop([
            'issue',
            'volume',
            'publisher',
            'chapter',
            'type',
            'publication_venue',
            'venue_type'
        ], axis=1, inplace=True)

    def get_publication_proceedings_paper_df(self) -> DataFrame:
        return self.publication_proceedings_paper_df


    # Venues.
    def set_venue_journal_df(self, _data_frame: DataFrame) -> None:
        self.venue_journal_df = _data_frame

    def get_venue_journal_df(self) -> DataFrame:
        return self.venue_journal_df


    def set_venue_joined_df(self, _data_frame: DataFrame) -> None:
        self.venue_joined_df = _data_frame

    def get_venue_joined_df(self) -> DataFrame:
        return self.venue_joined_df


    def set_venue_book_df(self, _data_frame: DataFrame) -> None:
        self.venue_book_df = _data_frame

    def get_venue_book_df(self) -> DataFrame:
        return self.venue_book_df


    def set_venue_proceedings_df(self, _data_frame: DataFrame) -> None:
        self.venue_proceedings_df = _data_frame

    def get_venue_proceedings_df(self) -> DataFrame:
        return self.venue_proceedings_df

    # Person
    def set_person_df(self, _data_frame: DataFrame) -> None:
        self.person_df = _data_frame

    def get_person_df(self) -> DataFrame:
        return self.person_df

    def do_venue_tables(self) -> None:
        publications_df: DataFrame = self.get_publications_df()
        venues_df: DataFrame = self.get_venues_df()

        # Build a new Venue Data Frame.
        #
        # Get the venues series (column).
        venues_series = publications_df[['id', 'publication_venue', 'venue_type','publisher','event']]

        # Merge Venue Series with venue DataFrame. 
        venues_joined_df: DataFrame = merge(
            venues_series, 
            venues_df, 
            left_on  = 'id',
            right_on = 'doi'
        )

        # Drop useless columns.
        venues_joined_df.drop(["id", "publication_venue", "publisher"], axis=1, inplace=True)

        # Create a new column with internal identifiers for each venue.
        venue_internal_id: list = []

        # Compose the internal id of each publication with the index.
        for index, row in publications_df.iterrows():
            # If the dictionary is empty than assign an empty string to the dictionary.
            if not self.venue_names_dict:
                self.venue_names_dict[row["id"]] = ''

            if row["id"] in self.venue_names_dict:
                self.venue_names_dict[row["id"]] = row["publication_venue"]
            else:
                self.venue_names_dict[row["id"]] = ''
                self.venue_names_dict[row["id"]] = row["publication_venue"]

        # Build new DataFrame from populated dictionary.
        venue_names_df: DataFrame = DataFrame(self.venue_names_dict.items(), columns=["doi", "title"]) 

        # Merge Venue Names DataFrame with merged DataFrame.
        venues_joined_df = merge(
            venue_names_df, 
            venues_joined_df, 
            left_on  = 'doi',
            right_on = 'doi'
        )

        # Compose the internal id of each venue with the index.
        for index, row in venues_df.iterrows():
            venue_internal_id.append("venue-" + str(index))

        # Insert the list above composed.
        venues_joined_df.insert(0, "internalId", Series(venue_internal_id, dtype="string")) 

        # Write Venue joined DataFrame than drop useless columns before split other DataFrame(s).
        self.set_venue_joined_df(venues_joined_df)

        # Build DataFrame(s) needed. Venues.
        journal_df: DataFrame     = self.get_venue_joined_df().query("venue_type == 'journal'")
        book_df: DataFrame        = self.get_venue_joined_df().query("venue_type == 'book'")
        proceedings_df: DataFrame = self.get_venue_joined_df().query("venue_type == 'proceedings'")

        # Drop useless columns.
        journal_df.drop(["event", "doi"], axis=1, inplace=True)
        book_df.drop(["event", "doi"], axis=1, inplace=True)
        proceedings_df.drop(["doi"], axis=1, inplace=True)

        # Update DataFrame(s).
        self.set_venue_journal_df(journal_df)
        self.set_venue_book_df(book_df)
        self.set_venue_proceedings_df(proceedings_df)


    def do_publication_tables(self) -> None:
        publications_df: DataFrame = self.get_publications_df()
        venues_df: DataFrame = self.get_venues_df()

        # Create a new column with internal identifiers for each publication.
        publication_internal_id: list = []

        # Compose the internal id of each publication with the index.
        for index, row in publications_df.iterrows():
            publication_internal_id.append("publication-" + str(index))

        # Insert the list above composed.
        publications_df.insert( 0, "internalId", Series(publication_internal_id, dtype="string"))

        venues_joined_df = self.get_venue_joined_df()
        venues_joined_df.drop(["event", "title", "ids", "venue_type"], axis=1, inplace=True)

        # Merge Venue Joined DataFrame with Publications DataFrame. 
        publication_joined_df = merge(
            publications_df, 
            venues_joined_df, 
            left_on  = 'id',
            right_on = 'doi'
        )

        publication_joined_df.rename(
            columns = {
                'internalId_y':'venue_id',
                'internalId_x':'internal_id'
            }, 
            inplace = True
        )
        
        publication_joined_df.drop(["doi"], axis=1, inplace=True)

        # DataFrame of journal-article. Publications.
        self.set_publication_journal_article_df(publications_df.query("type == 'journal-article'"))
        self.set_publication_book_chapter_df(publications_df.query("type == 'book-chapter'"))
        self.set_publication_proceedings_paper_df(publications_df.query("type == 'proceedings-paper'"))

    def do_person_tables(self) -> None:
        authors_df: DataFrame = self.get_authors_df()

        # Create the Authors set.
        authors_set = set()

        for index, row in authors_df.iterrows():

            # If the dictionary is empty than assign an empty set to the dictionary.
            if not self.person_internal_id_dict:
                self.person_internal_id_dict[row['doi']] = set()

            #
            # If the value of 'doi' is already in dictonary, add the subject.
            # Otherwise create a new key and inizialite it with an empty set,
            # than add the subject.
            #
            if row['doi'] in self.person_internal_id_dict:
                self.person_internal_id_dict[row['doi']].add((row['orc_id'],row['given_name'],row['family_name']))
            else:
                self.person_internal_id_dict[row['doi']] = set()
                self.person_internal_id_dict[row['doi']].add((row['orc_id'],row['given_name'],row['family_name']))

            #
            # If these columns elements are not in set then add them.
            if (row['orc_id'],row['given_name'],row['family_name']) not in authors_set:
                authors_set.add((row['orc_id'],row['given_name'],row['family_name']))

        print(authors_set)

        person_df: DataFrame = DataFrame(authors_set)

        person_df.rename(
            columns = {
                0:'orc_id',
                1:'given_name',
                2:'family_name'
            }, 
            inplace = True
        )

        print(person_df)

        # Create a new column with internal identifiers for each person.
        person_internal_id: list = []

        # Compose the internal id of each person with the index.
        for index, row in person_df.iterrows():
            person_internal_id.append("person-" + str(index))

        # Insert the list above composed.
        person_df.insert( 0, "internalId", Series(person_internal_id, dtype="string"))

        print(person_df)

        self.set_person_df(person_df)

    def do_organization_tables() -> None:
        pass


    def tablesBuilder(self) -> None:

        print('-- INFO: Creating Tables...')

        self.do_venue_tables()
        self.do_publication_tables()
        self.do_person_tables()
        self.do_organization_tables()

        # Test.
        #print('-- INFO: Graph created!')
        #print('-- INFO: Number of triples added to the graph after processing venues and publications: ', len(rdf_graph))

        #self.set_graph(rdf_graph)


#############################
#                           #
# GraphDataProcessor Class. #
#                           #
#############################
class GraphDataProcessor(DataProcessor):

    def __init__(self, _base_url: str) -> None:
        super().__init__()
        
        self.rdf_graph = Graph() # Graph.
        self.base_url = _base_url
        
        self.venue_internal_id_dict: dict             = {} # Internal id to associate it to Venues.
        self.cites_internal_id_dict: dict             = {} # Internal id to associate References to Publications.
        self.author_internal_id_dict: dict            = {} # Internal id to associate Persons to Publications.
        self.publication_cites_internal_id_dict: dict = {} # Internal id to associate a Publication with its cites.
        self.organization_internal_id_dict: dict      = {} # Internal id to associate Venues to Publications.


    #
    # set_graph(self, _graph: Graph): -> None.
    #
    def set_graph(self, _graph: Graph) -> None:
        # Check if the Graph is empty.
        if len(_graph) < 1 :                
            print('-- WARN: The Graph is empty :(')

        self.rdf_graph = _graph

    #
    # get_graph(self): -> Graph.
    #
    def get_graph(self) -> Graph:     
        return self.rdf_graph

    ############################################################################
    #
    # do_organization_triples(self): -> None.
    #
    def do_organization_triples(self) -> None:
        
        rdf_graph = self.get_graph()
        
        publishers_df = self.get_publishers_df()
               
        # Create the Organizations set.
        organizations_set = set()
       
        # Populate the Organizations set.
        for index, row in publishers_df.iterrows():
            #
            # If these columns elements are not in set then add them.
            if (row['publisher_id'], row['publisher_name']) not in organizations_set:

                organizations_set.add((row['publisher_id'], row['publisher_name']))   

        # Create a new data frame with the set.
        organizations_df = DataFrame(organizations_set)
        
        # Rename Data Frame columns.
        organizations_df.rename(columns={
                0:'publisher_id', 
                1:'publisher_name',
            }, inplace=True)

        # Init a progress bar for the process of Organization resource.
        organization_bar = ChargingBar(
               '-- INFO: Creating Organization resources', 
               max    = len(organizations_df), 
               suffix = '%(percent).1f%% / rem: %(remaining)d / t: %(elapsed)ds'
            )

        ###########################################
        #                                         #
        # Create Graph Triples for Organizations. #
        #                                         #
        ########################################### 

        for index, row in organizations_df.iterrows():

            # Create a resource id.
            organization_local_id = 'organization-' + str(index)

            # Create a subject.
            subject = URIRef(self.base_url + organization_local_id)   

            # Associate to publisher crossref the subject above created.
            self.organization_internal_id_dict[row['publisher_id']] = subject

            #
            # Add triples of all Organization's types.
            # Add the resource type.
            #
            rdf_graph.add((subject, RDF.type, Organization))

            #
            # Create the Organization properties object.
            # Create the Organization resource.
            #
            rdf_graph.add((subject, id  , Literal(row['publisher_id'])))
            rdf_graph.add((subject, name, Literal(row['publisher_name'])))

            # Increase the progress bar.
            organization_bar.next()

        # Stop the progress bar.
        organization_bar.finish()

        print('-- INFO: Organization resources type created.')
        print('-- INFO: Literal objects associated to Organization resources.')



    ############################################################################
    #
    # do_venue_triples(self): -> None.
    #
    def do_venue_triples(self) -> None:

        rdf_graph = self.get_graph()

        publications_df = self.get_publications_df()
        venues_df       = self.get_venues_df() 

        #
        # Build a Venue Data Frame.
        #
        # Get the venues series (column).
        venues_series = publications_df[['id', 'publication_venue', 'venue_type','publisher','event']]

        venues_joined_df = merge(
            venues_series, 
            venues_df, 
            left_on  = 'id',
            right_on = 'doi'
        )

        # Create the Venues set.
        venues_set = set()

        # Populate the Venues set.
        for index, row in venues_series.iterrows():
            # if venue exists. 
            if row['publication_venue'] != '' and row['publisher'] != '':

                #
                # If these columns elements are not in set then add them.
                if (row['id'], row['publication_venue'], row['venue_type'], row['publisher'], row['event']) not in venues_set:

                    venues_set.add((row['id'], row['publication_venue'], row['venue_type'], row['publisher'], row['event']))

        # Create a venues Data Frame.
        venues_df = DataFrame(venues_set)

        # Rename Data Frame columns.
        venues_df.rename(columns={
                0:'id', 
                1:'title', 
                2:'type',
                3:'publisher',
                4:'event'
            }, inplace=True)

        # Init a progress bar for the process of Venue resource.
        venues_bar = ChargingBar(
                '-- INFO: Creating Venue resources',
                max    = len(venues_df),
                suffix = '%(percent).1f%% / rem: %(remaining)d / t: %(elapsed)ds'
            )
                
        ####################################
        #                                  #
        # Create Graph Triples for Venues. #
        #                                  #
        ####################################

        for index, row in venues_df.iterrows():
                                   
            # Create a resource id.
            venue_local_id = 'venue-' + str(index)
            
            # Create a subject.
            subject = URIRef(self.base_url + venue_local_id)
            
            # Associate to venues 'title' the subject above created.
            self.venue_internal_id_dict[row['title']] = subject
        
            # Add triples of all Venue's types.
            match row['type']:
                case 'journal':
                    #
                    # Create the Journal resource.
                    #
                    rdf_graph.add((subject, RDF.type, Journal))

                case 'book':
                    #
                    # Create the Book resource.
                    #
                    rdf_graph.add((subject, RDF.type, Book))

                case 'proceedings':
                    #
                    # Create the Proceedings resource.
                    #
                    rdf_graph.add((subject, RDF.type, Proceedings))

                    #
                    # Create the Proceedings property object.
                    #
                    rdf_graph.add((subject, event, Literal(row['event'])))

            # Add property object for all resources.f
            rdf_graph.add((subject, title, Literal(row['title'])))
            
            # Associate every Venue to an Identifiable Entity.
            for venues_joined_index, venues_joined_row in venues_joined_df.iterrows():
                if row['id'] == venues_joined_row['id']:
                    for item in venues_joined_row['ids']:
                        rdf_graph.add((subject, id, Literal(item)))
           
            ######################################################
            #                                                    #
            # Create the relation with Venues and Organizations. #
            #                                                    #
            ######################################################

            for key, value in self.organization_internal_id_dict.items():
                if key == row['publisher']:
                    rdf_graph.add((subject, hasPublisher, value))    
            
            # Increase the progress bar.
            venues_bar.next()
        
        # Stop the progress bar.
        venues_bar.finish()
                
        print('-- INFO: Venue resources type created: Journal, Book and Proceedings.')
        print('-- INFO: Literal objects associated to Proceedings resources.')
        print('-- INFO: Venue and Organization relations created.')
        
            
        
    ############################################################################
    #
    # do_person_triples(self): -> None.
    #
    def do_person_triples(self) -> None:

        rdf_graph = self.get_graph()

        authors_df = self.get_authors_df()
                
        # Create the Authors set.
        authors_set = set()
 
        # Init a progress bar for the process of Person resources.
        authors_bar = ChargingBar(
                '-- INFO: Creating Person resources',
                max    = len(authors_df),
                suffix = '%(percent).1f%% / rem: %(remaining)d / t: %(elapsed)ds'
            )
        
        #####################################
        #                                   #
        # Create Graph Triples for Persons. #
        #                                   #
        #####################################

        for index, row in authors_df.iterrows():

            # Create a resource id.
            author_local_id = 'person-' + str(index)

            # Create a subject.
            subject = URIRef(self.base_url + author_local_id)

            # If the dictionary is empty than assign an empty set to the dictionary.
            if not self.author_internal_id_dict:
                self.author_internal_id_dict[row['doi']] = set()

            #
            # If the value of 'doi' is already in dictonary, add the subject.
            # Otherwise create a new key and inizialite it with an empty set,
            # than add the subject.
            #
            if row['doi'] in self.author_internal_id_dict:
                self.author_internal_id_dict[row['doi']].add(subject)
            else:
                self.author_internal_id_dict[row['doi']] = set()
                self.author_internal_id_dict[row['doi']].add(subject)

            #
            # If these columns elements are not in set then add them.
            if (row['doi'], row['orc_id']) not in authors_set:
                authors_set.add((row['doi'], row['orc_id']))

            #
            # Don't duplicate authors.
            #            
            for item in authors_set:
                if row['orc_id'] == item[1]:
                    #
                    # Add triples of all Author's types.
                    # Create the Author resource.
                    #            
                    rdf_graph.add((subject, RDF.type, Person))
        
                    #
                    # Create the Author properties object.
                    #
                    rdf_graph.add((subject, id         , Literal(row['orc_id'])))
                    rdf_graph.add((subject, givenName  , Literal(row['given_name'])))
                    rdf_graph.add((subject, familyName , Literal(row['family_name'])))

            # Increase the progress bar.
            authors_bar.next()

        # Stop the progress bar.
        authors_bar.finish()

        print('-- INFO: Person resources type created.')
        print('-- INFO: Literal objects associated to Person resources.')



    ############################################################################
    #
    # do_publication_triples(self): -> None.
    #
    def do_publication_triples(self) -> None:
        
        rdf_graph = self.get_graph()
        
        references_df = self.get_references_df()   
        publications_df = self.get_publications_df()
                                 
        
        for index, row in references_df.iterrows():
            # Initialize empty dictonary on the first loop cicle.
            if not self.cites_internal_id_dict and len(row['cites']) > 0:
                self.cites_internal_id_dict[row['doi']] = set()

            row_cites = row['cites']

            #
            # If the value of 'doi' is already in dictonary, add the subject.
            # Otherwise create a new key and inizialite it with an empty set,
            # than add the subject.
            #
            if len(row['cites']) > 0:

                if row['doi'] in self.cites_internal_id_dict:
                                    
                    for item in row_cites:
                        self.cites_internal_id_dict[row['doi']].add(item)

                else:
                    self.cites_internal_id_dict[row['doi']] = set()
                    
                    for item in row_cites:
                        self.cites_internal_id_dict[row['doi']].add(item)

        # Init a progress bar for the process of Publication resources.
        publication_bar = ChargingBar(
                '-- INFO: Creating Publication resources',
                max    = len(publications_df),
                suffix = '%(percent).1f%% / rem: %(remaining)d / t: %(elapsed)ds'
            )

        ########################################## 
        #                                        #
        # Create Graph Triples for Publications. #
        #                                        #
        ##########################################

        for index, row in publications_df.iterrows():
            
            # Create a resource id.
            publication_local_id = 'publication-' + str(index)

            # Create a subject.
            subject = URIRef(self.base_url + publication_local_id)
            
            # Store the id col value (doi).
            publication_doi = row['id']
            
            # Fill up the dictionary.
            if publication_doi not in self.publication_cites_internal_id_dict:
                self.publication_cites_internal_id_dict[publication_doi] = subject
                                                        
            # Add triples of all Publication's types.
            match row['type']:
                case 'journal-article':
                    #
                    # Create the JournalArticle resource.
                    #
                    rdf_graph.add((subject, RDF.type, JournalArticle))

                    #
                    # Create the JournalArticle property objects.
                    #
                    rdf_graph.add((subject, issue, Literal(row['issue'])))
                    rdf_graph.add((subject, volume, Literal(row['volume'])))
                                        
                case 'book-chapter':
                    #
                    # Create the Bookchapter resource.
                    #
                    rdf_graph.add((subject, RDF.type, BookChapter))

                    #
                    # Create the Bookchapter property object.
                    #
                    rdf_graph.add((subject, chapterNumber, Literal(row['chapter'])))

                case 'proceeding-papers':
                    #
                    # Create the ProceedingsPaper resource.
                    #
                    rdf_graph.add((subject, RDF.type, ProceedingsPaper))

            ########################################################
            #                                                      #
            # Create relation with the Publication and the Person. #
            #                                                      #
            ########################################################  
            for key, value in self.author_internal_id_dict.items():
                if key == row['id']:
                    for person in value:
                        rdf_graph.add((subject, hasAuthor, person))
    
            #######################################################
            #                                                     #
            # Create relation with the Publication and the Venue. #
            #                                                     #
            #######################################################
            
            if row['publication_venue'] != '':
                rdf_graph.add((subject, hasPublicationVenue, self.venue_internal_id_dict[row['publication_venue']]))

            #
            # Add common properties to all Publications.
            #
            rdf_graph.add((subject, id , Literal(row['id'])))
            rdf_graph.add((subject, title              , Literal(row['title'])))
            rdf_graph.add((subject, publicationYear    , Literal(row['publication_year'])))


            # Increase the progress bar.
            publication_bar.next()

        # Stop the progress bar.
        publication_bar.finish()

        print('-- INFO: Publication resources type created: JournalArticle, BookChapter, ProceedingsPapers')
        print('-- INFO: Literal objects associated to JournalArticle resources.')
        print('-- INFO: Literal objects associated to BookChapter resources.')  
        print('-- INFO: Publication and Person relation created.')
        print('-- INFO: Publication and Venue relation created.')

        ########################################################
        #                                                      #
        # Create relation with the Publication and References. #
        #                                                      #
        ########################################################

        spinner = Spinner('-- INFO: Create relation with the Publication and References ')
        state   = '' 
        counter = 0 # counter.

        while state != 'FINISHED':

            for cite_key, cite_value in self.cites_internal_id_dict.items():

                counter += 1

                for publication_key, publication_value in self.publication_cites_internal_id_dict.items():

                    if cite_key == publication_key:

                        for item in cite_value:
                            for key, value in self.publication_cites_internal_id_dict.items():
            
                                spinner.next()

                                if item == key:
                                    rdf_graph.add((publication_value, hasCites, value))
  
            if len(self.cites_internal_id_dict) == counter:
                state = 'FINISHED'

        print('\n-- INFO: Publication and References relation created.')


    ############################################################################
    #
    # graphBuilder(self): -> bool.
    #
    def graphBuilder(self) -> bool:       

        print('-- INFO: Creating Graph...')
               
        self.do_organization_triples()
                
        self.do_venue_triples()
        
        self.do_person_triples()
        
        self.do_publication_triples()
        
        # Get the rdf graph populated.
        rdf_graph = self.get_graph()
     
        rdf_graph.serialize('./data-test/json-graph.json', format='json-ld')

        # Test.
        print('-- INFO: Graph created!')
        print('-- INFO: Number of triples added to the graph after processing venues and publications: ', len(rdf_graph))

        self.set_graph(rdf_graph)



    ############################################################################
    #
    # tripletoreDeploy(self): -> None
    #
    def triplestoreDeploy(self) -> None:

        try:
            # Create a Triplestore to do teploy triples.
            triplestore = SPARQLUpdateStore()

            # It opens the connection with the SPARQL endpoint instance.
            triplestore.open((ENDPOINT, ENDPOINT))

            rdf_graph = self.get_graph()

            # Init a progress bar for the process of adding triples to the store.
            triplestore_bar = ChargingBar(
                '-- INFO: Deploying Graph: ',
                max    = len(rdf_graph),
                suffix = '%(percent).1f%% / rem: %(remaining)d / t: %(elapsed)ds'
            )

            for triple in rdf_graph.triples((None, None, None)):
                #
                # FIXME: fix UTF-8 encoding .
                # 
                triplestore.add(triple)
                triplestore_bar.next()

            triplestore_bar.finish()

            print('-- INFO: Graph has been deployed!', EMOJI_FIRE, EMOJI_FIRE, EMOJI_FIRE)

        except Exception as error:
                print('-- ERR: Blazegraph instance is not running.\n',error.args)

                # Generate exception.
                raise
        finally:
            # Once finished, remeber to close the connection.
            triplestore.close()
