
##########################
#                        #
# QueryProcessor Module. #
#                        #
##########################

# string.
from sqlite3 import connect
from string import Template

# constants internal.
from const import *

# Pandas
import pandas as pd
from pandas import DataFrame, read_sql

# data model internal.
from data_model import JournalArticle, Organization, Person, Proceedings, Publication, Venue

# sparql_dataframe.
from sparql_dataframe import get

# triplestore processor.
from triplestore_processor import TriplestoreProcessor

# relationa processor.
from relational_processor import RelationalProcessor


##############################
#                            #
# QueryProcessor Base Class. #
#                            #
##############################
class QueryProcessor(object):
    def __init__(self) -> None:
        pass

####################################
#                                  #
# TriplestoreQueryProcessor Class. #
#                                  #
####################################
class TriplestoreQueryProcessor(QueryProcessor, TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()
        
    # getPublicationsPublishedInYear(_year: int): -> DataFrame.
    def getPublicationsPublishedInYear(self, _year: int) -> DataFrame:
        endpoint_url = self.getEnpointUrl()
        
        # Parametrize query by passing Publication year.
        sparql_query = Template(PUBLICATIONS_PUBLISHED_IN_YEAR).substitute(YEAR = _year)
        
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql
            

    # getPublicationsByAuthorId(_id: str): -> DataFrame.
    def getPublicationsByAuthorId(self, _id: str) -> DataFrame:

        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing Author id.
        sparql_query = Template(PUBLICATIONS_BY_AUTHOR_ID).substitute(AUTHOR_ID = _id)
        
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getMostCitedPublication(): -> DataFrame. 
    def getMostCitedPublication(self) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        df_sparql = get(endpoint_url, MOST_CITED_PUBLICATION, True)

        return df_sparql

    
    # getMostCitedVenue(): -> DataFrame.
    def getMostCitedVenue(self) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        df_sparql = get(endpoint_url, MOST_CITED_VENUE, True)

        return df_sparql


    # getVenuesByPublisherId(_id: str): -> DataFrame.
    def getVenuesByPublisherId(self, _id: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing publisher id (Organization).
        sparql_query = Template(VENUES_BY_PUBLISHER_ID).substitute(PUBLISHER_ID = _id)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getPublicationInVenue(_venueId: str): -> DataFrame.
    def getPublicationInVenue(self, _venueId: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing Venue id.
        sparql_query = Template(PUBLICATION_IN_VENUE).substitute(VENUE_ID = _venueId)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getJournalArticlesInIssue(_issue: str, _volume: str, _journalId: str): -> DataFrame.
    def getJournalArticlesInIssue(self, _issue: str, _volume: str, _journalId: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing Issue, Volume, and Venue issn.
        sparql_query = Template(JOURNAL_ARTICLES_IN_ISSUE).substitute(
                                                                ISSUE    = _issue,
                                                                VOLUME   = _volume,
                                                                VENUE_ID = _journalId
                                                            )
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getJournalArticlesInVolume(_volume: str, _journalId: str): -> DataFrame.
    def getJournalArticlesInVolume(self, _volume: str, _journalId: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing Volume and Venue issn.
        sparql_query = Template(JOURNAL_ARTICLES_IN_VOLUME).substitute(
                                                                VOLUME   = _volume,
                                                                VENUE_ID = _journalId
                                                            )
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getJournalArticlesInJournal(_journalId: str): -> DataFrame.
    def getJournalArticlesInJournal(self, _journalId: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing journal issn.
        sparql_query = Template(JOURNAL_ARTICLES_IN_JOURNAL).substitute(VENUE_ID = _journalId)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql

    # getProceedingsByEvent(_eventPartialName: str): -> DataFrame.
    def getProceedingsByEvent(self, _eventPartialName:str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing journal issn.
        sparql_query = Template(PROCEEDINGS_BY_EVENT).substitute(EVENT_PARTIAL_NAME = _eventPartialName)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql
    
    # getPublicationAuthors(_publicationId: str): -> DataFrame.
    def getPublicationAuthors(self, _publicationId: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing publication doi.
        sparql_query = Template(PUBLICATION_AUTHORS).substitute(PUBLICATION_ID = _publicationId)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getPublicationsByAuthorName(_authorPartialName: str): -> DataFrame.
    def getPublicationsByAuthorName(self, _authorPartialName: str) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing Person name.
        sparql_query = Template(PUBLICATIONS_BY_AUTHOR_NAME).substitute(AUTHOR_NAME = _authorPartialName)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql
 

    # getDistinctPublisherOfPublications(_pubIdList: list[str]): -> DataFrame.
    def getDistinctPublisherOfPublications(self, _pubIdList: list[str]) -> DataFrame:
        endpoint_url = self.getEnpointUrl()

        # Parametrize query by passing a list of publication doi.
        sparql_query = Template(DISTINCT_PUBLISHER_OF_PUBLICATIONS).substitute(PUBLICATIONS_LIST = _pubIdList)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


###################################
#                                 #
# RelationalQueryProcessor Class. #
#                                 #
###################################
"""
class RelationalQueryProcessor(QueryProcessor, RelationalProcessor):
    def __init__(self) -> None:
        super().__init__()

    def getPublicationsPublishedInYear(self, _year: int) -> DataFrame:
        pass

    def getPublicationsByAuthorId(self, _id: str) -> DataFrame:
        pass

    def getMostCitedPublication(self) -> DataFrame:
        pass

    def getMostCitedVenue(self) -> DataFrame:
        pass

    def getVenuesByPublisherId(self, _id: str) -> DataFrame:
        pass

    def getPublicationInVenue(self, _venueId: str) -> DataFrame:
        pass

    def getJournalArticlesInIssue(self, _issue: str, _volume: str, _journalId: str) -> DataFrame:
        pass

    def getJournalArticlesInVolume(self, _volume: str, _journalId: str) -> DataFrame:
        pass

    def getJournalArticlesInJournal(self, _journalId: str) -> DataFrame:
        pass

    def getProceedingsByEvent(self, _eventPartialName:str) -> DataFrame:
        pass

    def getPublicationAuthors(self, _publicationId: str) -> DataFrame:
        pass

    def getPublicationsByAuthorName(self, _authorPartialName: str) -> DataFrame:
        pass

    def getDistinctPublisherOfPublications(self, _pubIdList: list[str]) -> DataFrame:
        pass
"""

####################################
#                                  #
#  RelationalQueryProcessor class. #
#                                  #
####################################
class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()
        
    def getPublicationsPublishedInYear(self, year):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, JournalArticle.title, PubID.id, publication_year, issue, volume, 'NA' as chapternumber
                                FROM JournalArticle
                                LEFT JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                                WHERE publication_year= """ + str(year) + """

                                UNION
                        SELECT BookChapter.internalID, BookChapter.title, PubID.id, publication_year,'NA' as issue,'NA' as volume, chapternumber
                                FROM BookChapter
                                LEFT JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                                WHERE publication_year= """ + str(year) + """
                                
                                UNION
                        SELECT ProceedingPaper.internalID, ProceedingPaper.title, PubID.id, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber
                                FROM ProceedingPaper
                                LEFT JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                                WHERE publication_year= """ + str(year) + """

                                """
            df_sql = read_sql(query, con)
            #getPublicationsPublishedInYear = pd.DataFrame(df_sql)
            #return getPublicationsPublishedInYear
            return df_sql

    def getPublicationsByAuthorId(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber,
                                    author, PersonID.id ,'NA' as event
                            FROM JournalArticle 
                            LEFT JOIN Author ON JournalArticle.author ==Author.AuthorID
                            LEFT JOIN PubID ON JournalArticle.internalID == PubID.PublicationID 
                            LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                            WHERE PersonID.id='""" + id + """'
                            UNION
                            SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber,
                                   author, PersonID.id , 'NA' as event
                            FROM BookChapter 
                            LEFT JOIN Author ON BookChapter.author ==Author.AuthorID
                            LEFT JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON  Author.person_internal_id ==PersonID.person_internal_id
                            WHERE PersonID.id='""" + id + """'
                            UNION
                            SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber,
                                   author, PersonID.id, 'NA' as event
                            FROM ProceedingPaper
                            LEFT JOIN Author ON ProceedingPaper.author ==Author.AuthorID
                            LEFT JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                            LEFT JOIN PersonID ON  Author.person_internal_id ==PersonID.person_internal_id
                            WHERE PersonID.id='""" + id + """'
                         """
            df_sql = read_sql(query, con)
            return df_sql

    def getMostCitedVenue(self):
        with connect(self.getDbPath()) as con: #modificata
            query = """SELECT VenueID.id, VenueID.VenueID, Journal.title
                               FROM JournalArticle JOIN (SELECT PublicationID  FROM Cites
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub
                               ON JournalArticle.internalID == pub.PublicationID
                               LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                               LEFT JOIN Journal ON Journal.internalID == JournalArticle.PublicationVenue

                               UNION
                               SELECT  VenueID.id, VenueID.VenueID, Book.title
                               FROM BookChapter JOIN (SELECT PublicationID  FROM Cites
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub
                               ON BookChapter.internalID == pub.PublicationID
                               LEFT JOIN VenueID ON BookChapter.PublicationVenue == VenueID.VenueID
                               LEFT JOIN Book ON Book.internalID == BookChapter.PublicationVenue
                               

                               UNION
                               SELECT  VenueID.id, VenueID.VenueID, Proceeding.title
                               FROM ProceedingPaper JOIN (SELECT PublicationID  FROM Cites
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub
                               ON ProceedingPaper.internalID == pub.PublicationID
                               LEFT JOIN VenueID ON ProceedingPaper.PublicationVenue == VenueID.VenueID
                               LEFT JOIN Proceeding ON Proceeding.internalID == ProceedingPaper.PublicationVenue
                            """
            df_sql = read_sql(query, con)
            return df_sql

    def getMostCitedPublication(self):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, author, PersonID.id, given_name, family_name, PublicationVenue, VenueID.id, Journal.title, 'NA' as event, Journal.Publisher, OrgID.id, Organization.name, Pub2.id
                               FROM JournalArticle JOIN (SELECT PublicationID  FROM Cites
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub
                               ON JournalArticle.internalID == pub.PublicationID
                               LEFT JOIN PubID ON pub.PublicationID == PubID.PublicationID
                               LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                               LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                               LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                               LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                               LEFT JOIN VenueID ON Journal.internalID == VenueID.VenueID
                               LEFT JOIN Organization ON Journal.Publisher == Organization.internalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               LEFT JOIN Cites ON JournalArticle.cites == Cites.internalID
                               LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                               UNION
                               SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber,
                                   author, PersonID.id , given_name, family_name,
                                   PublicationVenue, VenueID.id, Book.title, 'NA' as event, Book.Publisher, OrgID.id, Organization.name, Pub2.id
                               FROM BookChapter JOIN (SELECT PublicationID  FROM Cites
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub
                               ON BookChapter.internalID == pub.PublicationID
                               LEFT JOIN PubID ON pub.PublicationID == PubID.PublicationID
                               LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                               LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                               LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                               LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                               LEFT JOIN VenueID ON Book.internalID == VenueID.VenueID
                               LEFT JOIN Organization ON Book.Publisher == Organization.internalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               LEFT JOIN Cites ON BookChapter.cites == Cites.internalID
                               LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID
                               UNION
                               SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber,
                                   author, PersonID.id , given_name, family_name,
                                   PublicationVenue, VenueID.id, Proceeding.title, event, Proceeding.Publisher, OrgID.id, Organization.name, Pub2.id
                               FROM ProceedingPaper JOIN (SELECT PublicationID  FROM Cites
                                                            GROUP BY PublicationID
                                                            ORDER BY COUNT (*) DESC
                                                            LIMIT 1) pub
                               ON ProceedingPaper.internalID == pub.PublicationID
                               LEFT JOIN PubID ON pub.PublicationID == PubID.PublicationID
                               LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                               LEFT JOIN PersonID ON Author.person_internal_id == PersonID.person_internal_id
                               LEFT JOIN Person ON PersonID.person_internal_id == Person.internalID
                               LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                               LEFT JOIN VenueID ON Proceeding.internalID == VenueID.VenueID
                               LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               LEFT JOIN Cites ON ProceedingPaper.cites == Cites.internalID
                               LEFT JOIN PubID Pub2 ON Cites.PublicationID == Pub2.PublicationID ;
                            """
            df_sql = read_sql(query, con)
            return df_sql

    def getVenuesByPublisherId(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT VenueID.id, OrgID.id, Organization.name
                                FROM Journal
                                LEFT JOIN OrgID ON Journal.Publisher == OrgID.OrgID
                                LEFT JOIN VenueID ON VenueID.VenueID == Journal.internalID
                                LEFT JOIN Organization ON Organization.internalID == Journal.Publisher

                                WHERE OrgID.id= '""" + id + """'
                                UNION
                                SELECT VenueID.id, OrgID.id, Organization.name
                                FROM Book
                                LEFT JOIN OrgID ON Book.Publisher == OrgID.OrgID
                                LEFT JOIN VenueID ON VenueID.VenueID == Book.internalID
                                LEFT JOIN Organization ON Organization.internalID == Book.Publisher


                                WHERE OrgID.id='""" + id + """'
                                UNION
                                SELECT VenueID.id, OrgID.id, Organization.name
                                FROM Proceeding
                                LEFT JOIN OrgID ON Proceeding.Publisher == OrgID.OrgID
                                LEFT JOIN VenueID ON VenueID.VenueID == Proceeding.internalID
                                LEFT JOIN Organization ON Organization.internalID == Proceeding.Publisher


                                WHERE OrgID.id='""" + id + """'
                             """
            df_sql = read_sql(query, con)
            return df_sql

    def getPublicationInVenue(self, id):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume, 'NA' as chapternumber, VenueID.id

                                FROM JournalArticle
                                LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                                LEFT JOIN PubID on PubID.PublicationID == JournalArticle.internalID

                            WHERE VenueID.id='""" + id + """'
                            UNION
                            SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year,'NA' as issue,'NA' as volume, chapternumber, VenueID.id

                                FROM BookChapter
                                LEFT JOIN VenueID ON BookChapter.PublicationVenue == VenueID.VenueID
                                LEFT JOIN PubID on PubID.PublicationID == Bookchapter.internalID

                                WHERE VenueID.id='""" + id + """'
                            UNION
                            SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year,'NA' as issue,'NA' as volume,'NA' as chapternumber, VenueID.id

                                FROM ProceedingPaper
                                LEFT JOIN VenueID ON ProceedingPaper.PublicationVenue == VenueID.VenueID
                                LEFT JOIN PubID on PubID.PublicationID == ProceedingPaper.internalID


                               WHERE VenueID.id='""" + id + """'
                         """
            df_sql = read_sql(query, con)
            return df_sql

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume,
                                   VenueID.id
                            FROM JournalArticle
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN PubID ON PubID.PublicationID == JournalArticle.PublicationVenue

                            WHERE VenueID.id= '""" + journalId + """'and JournalArticle.volume= """ + str(
                volume) + """ and JournalArticle.issue= """ + str(issue) + """
                        """
            df_sql = read_sql(query, con)
            return df_sql

    def getJournalArticlesInVolume(self, volume, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume,
                                   VenueID.id
                            FROM JournalArticle
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN PubID ON PubID.PublicationID == JournalArticle.PublicationVenue

                            WHERE VenueID.id= '""" + journalId + """' and JournalArticle.volume= """ + str(
                volume) + """
                        """
            df_sql = read_sql(query, con)
            return df_sql

    def getJournalArticlesInJournal(self, journalId):
        with connect(self.getDbPath()) as con:
            query = """SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, issue, volume,
                                   VenueID.id
                            FROM JournalArticle
                            LEFT JOIN VenueID ON JournalArticle.PublicationVenue == VenueID.VenueID
                            LEFT JOIN PubID ON PubID.PublicationID == JournalArticle.PublicationVenue

                            WHERE VenueID.id= '""" + journalId + """'
                        """
            df_sql = read_sql(query, con)
            return df_sql

    def getProceedingsByEvent(self, PartialName):
        with connect(self.getDbPath()) as con:
            query = """SELECT Proceeding.internalID, title, OrgID.id, name, Proceeding.event
                               FROM Proceeding
                               LEFT JOIN Organization ON Proceeding.Publisher == Organization.InternalID
                               LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                               WHERE lower(event) LIKE '%""" + PartialName + """%' ;
                            """
            df_sql = read_sql(query, con)
            return df_sql

    def getPublicationAuthors(self, publicationId):
        with connect(self.getDbPath()) as con:
            query = """SELECT  Person.internalID, PersonID.id,  given_name, family_name, PubID.id
                               FROM JournalArticle
                               LEFT JOIN PubID ON PubID.PublicationID == JournalArticle.internalID
                               LEFT JOIN Author ON  Author.AuthorID ==JournalArticle.author
                               LEFT JOIN Person ON Person.internalID == Author.person_internal_id
                               LEFT JOIN PersonID ON Person.internalID == PersonID.person_internal_id
                               WHERE PubID.id='""" + publicationId + """'
                               UNION
                               SELECT Person.internalID, PersonID.id, given_name, family_name, PubID.id
                               FROM Bookchapter
                               LEFT JOIN PubID ON PubID.PublicationID == Bookchapter.internalID
                               LEFT JOIN Author ON  Author.AuthorID ==Bookchapter.author
                               LEFT JOIN Person ON Person.internalID == Author.person_internal_id
                               LEFT JOIN PersonID ON Person.internalID == PersonID.person_internal_id
                               WHERE PubID.id='""" + publicationId + """'
                               UNION
                               SELECT Person.internalID, PersonID.id,  given_name, family_name, PubID.id
                               FROM ProceedingPaper
                               LEFT JOIN PubID ON PubID.PublicationID == ProceedingPaper.internalID
                               LEFT JOIN Author ON  Author.AuthorID ==ProceedingPaper.author
                               LEFT JOIN Person ON Person.internalID == Author.person_internal_id
                               LEFT JOIN PersonID ON Person.internalID == PersonID.person_internal_id
                               WHERE PubID.id= '""" + publicationId + """';

                            """
            df_sql = read_sql(query, con)
            return df_sql


    def getPublicationsByAuthorName(self, authorPartialName):
        with connect(self.getDbPath()) as con:
            query = """ SELECT JournalArticle.internalID, PubID.id, JournalArticle.title, publication_year, JournalArticle.issue, volume, 'NA' as chapternumber, Person.given_name, Person.family_name
                                FROM JournalArticle

                                LEFT JOIN Author ON JournalArticle.author == Author.AuthorID
                                LEFT JOIN Person ON Person.internalID == Person.internalID
                                LEFT JOIN PubID ON JournalArticle.internalID == PubID.PublicationID


                           WHERE lower(Person.family_name) LIKE '%""" + authorPartialName + """%'  OR lower(Person.given_name) LIKE '%""" + authorPartialName + """%'
                           UNION
                           SELECT BookChapter.internalID, PubID.id, BookChapter.title, publication_year, 'NA' as issue , 'NA' as volume, chapternumber, Person.given_name, Person.family_name
                                FROM BookChapter

                                LEFT JOIN Author ON BookChapter.author == Author.AuthorID
                                LEFT JOIN Person ON Person.internalID == Person.internalID
                                LEFT JOIN PubID ON BookChapter.internalID == PubID.PublicationID

                           WHERE lower(Person.family_name) LIKE '%""" + authorPartialName + """%'  OR lower(Person.given_name) LIKE '%""" + authorPartialName + """%'
                           UNION
                           SELECT ProceedingPaper.internalID, PubID.id, ProceedingPaper.title, publication_year, 'NA' as issue  , 'NA' as volume, 'NA' as chapternumber, Person.given_name, Person.family_name
                                FROM ProceedingPaper

                                LEFT JOIN Author ON ProceedingPaper.author == Author.AuthorID
                                LEFT JOIN Person ON Person.internalID == Person.internalID
                                LEFT JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID

                           WHERE lower(Person.family_name) LIKE '%""" + authorPartialName + """%'  OR lower(Person.given_name) LIKE '%""" + authorPartialName + """%' ;
                        """
            df_sql = read_sql(query, con)
            return df_sql

    def getDistinctPublisherOfPublications(self, pubIdList):
        lst = []
        for item in pubIdList:
            with connect(self.getDbPath()) as con:
                query = """SELECT  Organization.internalID ,OrgID.id, Organization.name, PubID.id
                                FROM JournalArticle
                                LEFT  JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                                LEFT JOIN Journal  ON JournalArticle.PublicationVenue == Journal.internalID
                                LEFT JOIN Organization ON Journal.Publisher == Organization.internalID
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                   WHERE PubID.id = '""" + item + """'
                                   UNION
                                   SELECT Organization.internalID ,OrgID.id, Organization.name, PubID.id
                                FROM BookChapter
                                LEFT  JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                                LEFT JOIN Book  ON BookChapter.PublicationVenue == Book.internalID
                                LEFT JOIN Organization ON Book.Publisher == Organization.internalID
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                   WHERE PubID.id ='""" + item + """'
                                   UNION
                                   SELECT Organization.internalID ,OrgID.id, Organization.name, PubID.id
                                FROM ProceedingPaper
                                LEFT  JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                                LEFT JOIN Proceeding  ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                                LEFT JOIN Organization ON Proceeding.Publisher == Organization.internalID
                                LEFT JOIN OrgID ON Organization.internalID == OrgID.OrgID
                                   WHERE PubID.id ='""" + item + """' ;
                                """
                df_sql = read_sql(query, con)
                lst.append(df_sql)
        final = pd.concat(lst)
        return final

################################
#                              #
# GenericQueryProcessor Class. #
#                              #
################################
class GenericQueryProcessor(object):
    def __init__(self) -> None:
        # List of queryProcessor instances.
        self.queryProcessor: list[QueryProcessor] = [] # QueryProcessor [0..*].

    def cleanQueryProcessor(self) -> bool:
        if len(self.queryProcessor) > 0:
            self.queryProcessor = []
            return True
        else: 
            return False

    def addQueryProcessor(self, _processors: list[QueryProcessor]) -> bool:
        result: bool = False
        
        for processor in _processors:
            if isinstance(processor, TriplestoreQueryProcessor) or isinstance(processor, RelationalQueryProcessor):
                self.queryProcessor.append(processor)
 
                result = True
            else:
                result = False

        return result


    def getPublicationsPublishedInYear(self, _year: int) -> list[Publication]:
        publications_published_in_year: list = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_year, int):
                triplestore_publication_published_year_df: DataFrame = processor.getPublicationsPublishedInYear(_year)

            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_year, int):
                relational_publication_published_year_df: DataFrame = processor.getPublicationsPublishedInYear(_year)

        if isinstance(triplestore_publication_published_year_df, DataFrame):
            # Triplestore Dataframe clean up.
            triplestore_publication_published_year_df.drop(
                [
                    'publication'
                ],
                axis = 'columns',
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_publication_published_year_df.rename(
                columns = {
                    'publicationId'   : 'publication_doi', 
                    'publicationYear' : 'publication_year', 
                    'publicationTitle': 'publication_title'
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_publication_published_year_df, DataFrame):
            # Relational Dataframe clean up.
            relational_publication_published_year_df = relational_publication_published_year_df.set_axis(
                [
                    'publication_internal_id',
                    'publication_title',
                    'publication_doi',
                    'publication_year',
                    'issue',
                    'volume',
                    'chapter_number'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_publication_published_year_df.drop(
                [
                    'publication_internal_id',
                    'issue',
                    'volume',
                    'chapter_number'
                ],
                axis = 'columns',
                inplace = True
            )

        # Concatenate both DataFrame(s).
        publications_published_in_year_df: DataFrame = pd.concat(
                                                        [triplestore_publication_published_year_df, relational_publication_published_year_df],
                                                        ignore_index=True, 
                                                       ).drop_duplicates().reset_index(drop=True)
        
        for idx, row in publications_published_in_year_df.iterrows():

            publication = Publication(row['publication_doi'], row['publication_year'],row['publication_title'])

            publications_published_in_year.append(publication)

        return publications_published_in_year
 

    def getPublicationsByAuthorId(self, _id: str) -> list[Publication]:
        publications_by_author_id = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_id, str):
                triplestore_publications_by_author_id_df = processor.getPublicationsByAuthorId(_id)

            elif isinstance(processor, RelationalQueryProcessor):
                relational_publications_by_author_id_df: DataFrame = processor.getPublicationsByAuthorId(_id)

        if isinstance(triplestore_publications_by_author_id_df, DataFrame):
            # Triplestore Dataframe clean up.
            triplestore_publications_by_author_id_df.drop(
                [
                    'publication',
                    'authorId',
                    'authorName',
                    'authorFamilyName'
                ],
                axis = 'columns',
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_publications_by_author_id_df.rename(
                columns = {
                    'publicationId'   :'publication_doi', 
                    'publicationYear' :'publication_year', 
                    'publicationTitle':'publication_title'
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_publications_by_author_id_df, DataFrame):
            # Relational Dataframe clean up.
            relational_publications_by_author_id_df = relational_publications_by_author_id_df.set_axis(
                [
                    'publication_internal_id',
                    'publication_doi',
                    'publication_title',
                    'publication_year',
                    'issue',
                    'volume',
                    'chapter_number',
                    'author',
                    'author_id',
                    'event'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_publications_by_author_id_df.drop(
                [
                    'publication_internal_id',
                    'issue',
                    'volume',
                    'chapter_number',
                    'author',
                    'author_id',
                    'event'
                ],
                axis = 'columns',
                inplace = True
            )

        # Concatenate both DataFrame(s).
        publications_by_author_id_df: DataFrame = pd.concat(
                                                    [triplestore_publications_by_author_id_df, relational_publications_by_author_id_df],
                                                    ignore_index=True, 
                                                  ).drop_duplicates().reset_index(drop=True)

        for idx, row in publications_by_author_id_df.iterrows():

            publication = Publication(row['publication_doi'], row['publication_year'],row['publication_title'])

            publications_by_author_id.append(publication)

        return publications_by_author_id


    def getMostCitedPublication(self) -> Publication:
        most_cited_publication = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor):
                triplestore_most_cited_publication_df = processor.getMostCitedPublication()
            elif isinstance(processor, RelationalQueryProcessor):
                relational_most_cited_publication_df = processor.getMostCitedPublication()
                
        if isinstance(triplestore_most_cited_publication_df, DataFrame):
            # Triplestore Dataframe clean up.
            triplestore_most_cited_publication_df.drop(
                [
                    'publication',
                    'noOfCites'
                ],
                axis = 'columns',
                inplace = True
            )
            # Rename DataFrame columns.
            triplestore_most_cited_publication_df.rename(
                columns = {
                    'publicationId'   :'publication_doi',
                    'publicationTitle':'publication_title', 
                    'publicationYear' :'publication_year'
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_most_cited_publication_df, DataFrame):
            # Relational Dataframe clean up.
            relational_most_cited_publication_df = relational_most_cited_publication_df.set_axis(
                [
                    'internalId',
                    'publication_doi',
                    'publication_title',
                    'publication_year',
                    'issue',
                    'volume',
                    'chapternumber',
                    'author',
                    'authorId',
                    'given_name',
                    'family_name',
                    'PublicationVenue',
                    'venueID',
                    'venueTitle',
                    'event',
                    'Publisher',
                    'publisherId',
                    'name',
                    'organizationId'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_most_cited_publication_df.drop(
                [
                    'internalId',
                    'issue',
                    'volume',
                    'chapternumber',
                    'author',
                    'authorId',
                    'given_name',
                    'family_name',
                    'PublicationVenue',
                    'venueID',
                    'venueTitle',
                    'event',
                    'Publisher',
                    'publisherId',
                    'name',
                    'organizationId'
                ],
                axis = 'columns',
                inplace = True
            )

 
        # Concatenate both DataFrame(s).
        most_cited_publication_df: DataFrame = pd.concat(
                                                    [triplestore_most_cited_publication_df, relational_most_cited_publication_df],
                                                    ignore_index=True 
                                               ).drop_duplicates().reset_index(drop=True)

        for idx, row in most_cited_publication_df.iterrows():

            publication = Publication(row['publication_doi'], row['publication_year'],row['publication_title'])

            most_cited_publication.append(publication)

        result = most_cited_publication[0]

        return result


    def getMostCitedVenue(self) -> Venue:
        most_cited_venue = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor):
                triplestore_most_cited_venue_df = processor.getMostCitedVenue()
            elif isinstance(processor, RelationalQueryProcessor):
                relational_most_cited_venue_df = processor.getMostCitedVenue()
         
        if isinstance(triplestore_most_cited_venue_df, DataFrame):
            # Triplestore DataFrame Clean up.
            triplestore_most_cited_venue_df.drop(
                [
                    'venue',
                    'noOfCites'
                ],
                axis = 'columns',
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_most_cited_venue_df.rename(
                columns = {
                    'venueId'   :'venue_id', 
                    'venueTitle':'venue_title', 
                }, 
                inplace = True,
                errors  = 'raise'
            )
            
        if isinstance(relational_most_cited_venue_df, DataFrame):
            # Relational Dataframe clean up.
            relational_most_cited_venue_df = relational_most_cited_venue_df.set_axis(
                [
                    'venue_internal_id',
                    'venue_id',
                    'venue_title'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_most_cited_venue_df.drop(
                [
                    'venue_internal_id'
                ],
                axis = 'columns',
                inplace = True
            )


        # Concatenate both DataFrame(s).
        most_cited_publication_df: DataFrame = pd.concat(
                                                    [triplestore_most_cited_venue_df, relational_most_cited_venue_df],
                                                    ignore_index=True 
                                               ).drop_duplicates().reset_index(drop=True)


        for idx, row in most_cited_publication_df.iterrows():

            publication = Venue(row['venue_id'], row['venue_title'])

            most_cited_venue.append(publication)

        result = most_cited_venue[0]

        return result


    def getVenuesByPublisherId(self, _id: str) -> list[Venue]:
        venue_by_publisher_id = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_id, str):
                triplestore_venues_by_publisher_id_df = processor.getVenuesByPublisherId(_id)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_id, str):
                relational_venues_by_publisher_id_df = processor.getVenuesByPublisherId(_id)

        if isinstance(triplestore_venues_by_publisher_id_df, DataFrame):
            # Triplestore Dataframe clean up.
            triplestore_venues_by_publisher_id_df.drop(
                [
                    'venue',
                    'organization',
                    'organizationId',
                    'organizationName'
                ],
                axis = 'columns',
                inplace = True
            )
            
            # Rename Data Frame columns.
            triplestore_venues_by_publisher_id_df.rename(
                    columns = {
                        'venueId'   : 'venue_id', 
                        'venueTitle': 'venue_title', 
                    }, 
                    inplace = True,
                    errors  = 'raise'
                )

        if isinstance(relational_venues_by_publisher_id_df, DataFrame):
            # Relational Dataframe clean up.
            relational_venues_by_publisher_id_df = relational_venues_by_publisher_id_df.set_axis(
                [
                    'venue_id',
                    'crossref',
                    'venue_title'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_venues_by_publisher_id_df.drop(
                [
                    'crossref'
                ],
                axis = 'columns',
                inplace = True
            )

            # Concatenate both DataFrame(s).
            most_cited_publication_df: DataFrame = pd.concat(
                                                        [triplestore_venues_by_publisher_id_df, relational_venues_by_publisher_id_df],
                                                        ignore_index=True 
                                                   ).drop_duplicates().reset_index(drop=True)

            for idx, row in most_cited_publication_df.iterrows():

                venue = Venue(row['venue_id'], row['venue_title'])

                venue_by_publisher_id.append(venue)

        return venue_by_publisher_id


    def getPublicationInVenue(self, _venueId: str) -> list[Publication]:
        publication_in_venue = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_venueId, str):
                triplestore_publication_in_venue_df = processor.getPublicationInVenue(_venueId)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_venueId, str):
                relational_publication_in_venue_df = processor.getPublicationInVenue(_venueId)
                
        if isinstance(triplestore_publication_in_venue_df, DataFrame):
            # Triplestore DataFrame clean up.
            triplestore_publication_in_venue_df.drop(
                [
                    'venue',
                    'venueId',
                    'venueTitle',
                    'publication'
                ],
                axis = 'columns',
                inplace = True
            )
 
            # Rename Data Frame columns.
            triplestore_publication_in_venue_df.rename(
                columns = {
                    'publicationId'   :'publication_doi', 
                    'publicationTitle':'publication_title', 
                    'publicationYear' :'publication_year' 
                }, 
                inplace = True,
                errors  = 'raise'
            )
        
        if isinstance(relational_publication_in_venue_df, DataFrame):
            # Relational DataFrame clean up.
            relational_publication_in_venue_df = relational_publication_in_venue_df.set_axis(
                [
                    'publication_internal_id',
                    'publication_doi',
                    'publication_title',
                    'publication_year',
                    'issue',
                    'volume',
                    'chapter_number',
                    'venue_id'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_publication_in_venue_df.drop(
                [
                    'publication_internal_id',
                    'issue',
                    'volume',
                    'chapter_number',
                    'venue_id'
                ],
                axis = 'columns',
                inplace = True
            )

        # Concatenate both DataFrame(s).
        publication_in_venue_df: DataFrame = pd.concat(
                                                [triplestore_publication_in_venue_df, relational_publication_in_venue_df],
                                                ignore_index=True 
                                             ).drop_duplicates().reset_index(drop=True)

        for idx, row in publication_in_venue_df.iterrows():

            publication = Publication(row['publication_doi'], row['publication_year'],row['publication_title'])

            publication_in_venue.append(publication)

        return publication_in_venue


    def getJournalArticlesInIssue(self, _issue: str, _volume: str, _journalId: str) -> list[JournalArticle]:
        journal_articles_in_issue = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_issue, str) and isinstance(_volume, str) and isinstance(_journalId, str):
                triplestore_journal_articles_in_issue_df = processor.getJournalArticlesInIssue(_issue, _volume, _journalId)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_issue, str) and isinstance(_volume, str) and isinstance(_journalId, str):
                relational_journal_articles_in_issue_df = processor.getJournalArticlesInIssue(_issue, _volume, _journalId)

        if isinstance( triplestore_journal_articles_in_issue_df, DataFrame):
            # Triplestore DataFrame clean up.
            triplestore_journal_articles_in_issue_df.drop(
                [
                    'JournalArticle',
                    'journal',
                    'venueId'
                ],
                axis = 'columns',
                inplace = True
            )
                
            # Rename Data Frame columns.
            triplestore_journal_articles_in_issue_df.rename(
                columns = {
                    'JournalArticleId'              :'journal_article_doi', 
                    'JournalArticleTitle'           :'journal_article_title', 
                    'JournalArticlePublicationYear' :'journal_article_publication_year', 
                    'issue'                         :'issue', 
                    'volume'                        :'volume', 
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_journal_articles_in_issue_df, DataFrame):
            # Relational DataFrame clean up.                
            relational_journal_articles_in_issue_df = relational_journal_articles_in_issue_df.set_axis(
                [
                    'journal_article_interal_id',
                    'journal_article_doi',
                    'journal_article_title',
                    'journal_article_publication_year',
                    'issue',
                    'volume',
                    'venue_id'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_journal_articles_in_issue_df.drop(
                [
                    'journal_article_interal_id',
                    'venue_id'
                ],
                axis = 'columns',
                inplace = True
            )

        # Concatenate both DataFrame(s).
        journal_articles_in_issue_df: DataFrame = pd.concat(
                                                    [triplestore_journal_articles_in_issue_df, relational_journal_articles_in_issue_df],
                                                    ignore_index=True 
                                                  ).drop_duplicates().reset_index(drop=True)

        for idx, row in journal_articles_in_issue_df.iterrows():

            journal_article = JournalArticle(
                                row['journal_article_doi'],
                                row['journal_article_publication_year'],
                                row['journal_article_title'],
                                row['issue'],
                                row['volume']
                              )

            journal_articles_in_issue.append(journal_article)

        return journal_articles_in_issue


    def getJournalArticlesInVolume(self, _volume: str, _journalId: str) -> list[JournalArticle]:
        journal_articles_in_volume = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_volume, str) and isinstance(_journalId, str):
                triplestore_journal_articles_in_volume_df = processor.getJournalArticlesInVolume(_volume, _journalId)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_volume, str) and isinstance(_journalId, str):
                relational_journal_articles_in_volume_df = processor.getJournalArticlesInVolume(_volume, _journalId)

        if isinstance(triplestore_journal_articles_in_volume_df, DataFrame):
            # Triplestore DataFrame clean up.
            triplestore_journal_articles_in_volume_df.drop(
                [
                    'JournalArticle',
                    'journal',
                    'venueId'
                ],
                axis = 'columns',
                inplace = True
            )
                
            # Rename Data Frame columns.
            triplestore_journal_articles_in_volume_df.rename(
                columns = {
                    'JournalArticleId'             : 'journal_article_doi', 
                    'JournalArticleTitle'          : 'journal_article_title', 
                    'JournalArticlePublicationYear': 'journal_article_publication_year', 
                    'issue'                        : 'issue', 
                    'volume'                       : 'volume'
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_journal_articles_in_volume_df, DataFrame):
            # Relational DataFrame clean up.                
            relational_journal_articles_in_volume_df = relational_journal_articles_in_volume_df.set_axis(
                [
                    'journal_article_interal_id',
                    'journal_article_doi',
                    'journal_article_title',
                    'journal_article_publication_year',
                    'issue',
                    'volume',
                    'venue_id'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_journal_articles_in_volume_df.drop(
                [
                    'journal_article_interal_id',
                    'venue_id'
                ],
                axis = 'columns',
                inplace = True
            )
            
         # Concatenate both DataFrame(s).
        journal_articles_in_volume_df: DataFrame = pd.concat(
                                                        [triplestore_journal_articles_in_volume_df, relational_journal_articles_in_volume_df],
                                                        ignore_index=True 
                                                   ).drop_duplicates().reset_index(drop=True)

        for idx, row in journal_articles_in_volume_df.iterrows():

            journal_article = JournalArticle(
                                row['journal_article_doi'],
                                row['journal_article_publication_year'],
                                row['journal_article_title'],
                                row['issue'],
                                row['volume']
                              )
            
            journal_articles_in_volume.append(journal_article)

        return journal_articles_in_volume


    def getJournalArticlesInJournal(self, _journalId: str) -> list[JournalArticle]:
        journal_articles_in_journal = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_journalId, str):
                triplestore_journal_articles_in_journal_df = processor.getJournalArticlesInJournal(_journalId)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_journalId, str):
                relational_journal_articles_in_journal_df = processor.getJournalArticlesInJournal(_journalId)

        if isinstance(triplestore_journal_articles_in_journal_df, DataFrame):
            # Triplestore DataFrame clean up.
            triplestore_journal_articles_in_journal_df.drop(
                [
                    'JournalArticle',
                    'journal',
                    'venueId'
                ],
                axis = 'columns',
                inplace = True
            )
            
            # Rename Data Frame columns.
            triplestore_journal_articles_in_journal_df.rename(
                columns = {
                    'JournalArticleId'              :'journal_article_doi', 
                    'JournalArticleTitle'           :'journal_article_title', 
                    'JournalArticlePublicationYear' :'journal_article_publication_year', 
                    'issue'                         :'issue', 
                    'volume'                        :'volume' 
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_journal_articles_in_journal_df, DataFrame):
            # Relational DataFrame clean up.
            relational_journal_articles_in_journal_df = relational_journal_articles_in_journal_df.set_axis(
                [
                    'journal_article_internal_id', 
                    'journal_article_doi', 
                    'journal_article_title', 
                    'journal_article_publication_year', 
                    'issue', 
                    'volume',
                    'venue_id'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_journal_articles_in_journal_df.drop(
                [
                    'journal_article_internal_id',
                    'venue_id'
                ],
                axis = 'columns',
                inplace = True
            )

         # Concatenate both DataFrame(s).
        journal_articles_in_journal_df: DataFrame = pd.concat(
                                                        [triplestore_journal_articles_in_journal_df, relational_journal_articles_in_journal_df],
                                                        ignore_index=True 
                                                    ).drop_duplicates().reset_index(drop=True)

        for idx, row in journal_articles_in_journal_df.iterrows():

            journal_article = JournalArticle(
                                row['journal_article_doi'],
                                row['journal_article_publication_year'],
                                row['journal_article_title'],
                                row['issue'],
                                row['volume']
                              )

            journal_articles_in_journal.append(journal_article)

        return journal_articles_in_journal

    def getProceedingsByEvent(self, _eventPartialName: str) -> list[Proceedings]:

        proceeding_by_event = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_eventPartialName, str):
                triplestore_proceeding_by_event_df = processor.getProceedingsByEvent(_eventPartialName)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_eventPartialName, str):
                relational_proceeding_by_event_df = processor.getProceedingsByEvent(_eventPartialName)

        if isinstance(triplestore_proceeding_by_event_df, DataFrame):
            # Triplestore DataFrame clean up.
            triplestore_proceeding_by_event_df.drop(
                [
                    'proceedings'
                ], 
                axis    = 'columns', 
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_proceeding_by_event_df.rename(
                columns = {
                    'proceedingsId' : 'proceedings_id',
                    'proceedingsTitle' : 'proceedings_title',
                    'proceedingsEvent' : 'proceedings_event'
                }, 
                inplace = True,
                errors  = 'raise'
            )
            
        if isinstance(relational_proceeding_by_event_df, DataFrame):
            # Relational DataFrame clean up.
            relational_proceeding_by_event_df = relational_proceeding_by_event_df.set_axis(
                [
                    'proceedings_internal_id',
                    'proceedings_title',
                    'proceedings_id',
                    'da_togliere',
                    'proceedings_event'
                ],
                axis    = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_proceeding_by_event_df.drop(
                [
                    'proceedings_internal_id',
                    'da_togliere'
                ], 
                axis    = 'columns', 
                inplace = True
            )

        # Concatenate both DataFrame(s).
        proceeding_by_event_df: DataFrame = pd.concat(
                                                [triplestore_proceeding_by_event_df, relational_proceeding_by_event_df],
                                                ignore_index = True
                                            ).drop_duplicates().reset_index(drop=True)

        for idx, row in proceeding_by_event_df.iterrows():

            proceedings = Proceedings(
                            row['proceedings_id'],
                            row['proceedings_title'],
                            row['proceedings_event']
                        )

            proceeding_by_event.append(proceedings)

        return proceeding_by_event


    def getPublicationAuthors(self, _publicationId: str) -> list[Person]:
        
        publication_authors = []

        for processor in self.queryProcessor:

            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_publicationId, str):
                triplestore_publication_authors_df = processor.getPublicationAuthors(_publicationId)
            elif isinstance(processor, RelationalQueryProcessor) and isinstance(_publicationId, str):
                relational_publication_authors_df = processor.getPublicationAuthors(_publicationId)

        if isinstance(triplestore_publication_authors_df, DataFrame):
            # Triplestore DataFrame clean up.
            triplestore_publication_authors_df.drop(
                [
                    'publication', 
                    'publicationId', 
                    'publicationYear', 
                    'publicationTitle', 
                    'author'
                ], 
                axis    = 'columns', 
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_publication_authors_df.rename(
                columns = {
                    'authorName'       :'author_name', 
                    'authorFamilyName' :'author_family_name',
                    'authorId'         :'author_id'
                }, 
                inplace = True,
                errors  = 'raise'
            )
            
        if isinstance(relational_publication_authors_df, DataFrame):
            # Relational DataFrame clean up.
            relational_publication_authors_df = relational_publication_authors_df.set_axis(
                [
                    'internalId',
                    'publicationId',
                    'author_name',
                    'author_family_name',
                    'author_id'
                ],
                axis    = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_publication_authors_df.drop(
                [
                    'internalId', 
                    'publicationId'
                ], 
                axis    = 'columns', 
                inplace = True
            )

        # Concatenate both DataFrame(s).
        publication_authors_df: DataFrame = pd.concat(
                [triplestore_publication_authors_df, relational_publication_authors_df],
                ignore_index = True
            ).drop_duplicates().reset_index(drop=True)


        for idx, row in publication_authors_df.iterrows():

            author = Person(
                        row['author_id'],
                        row['author_name'],
                        row['author_family_name']
                     )

            publication_authors.append(author)

        return publication_authors


    def getPublicationsByAuthorName(self, _authorPartialName: str) -> list[Publication]:
        
        publications_by_author_name = []

        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor) and isinstance(_authorPartialName, str):
                triplestore_publications_by_author_name_df = processor.getPublicationsByAuthorName(_authorPartialName)
            elif isinstance(processor, RelationalQueryProcessor)  and isinstance(_authorPartialName, str):
                relational_publications_by_author_name_df = processor.getPublicationsByAuthorName(_authorPartialName)

        if isinstance(triplestore_publications_by_author_name_df, DataFrame):
            # Triplestore Dataframe clean up.
            triplestore_publications_by_author_name_df.drop(
                [
                    'publication', 
                    'author', 
                    'authorName', 
                    'authorFamilyName', 
                    'authorId'
                ],
                axis    = 'columns',
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_publications_by_author_name_df.rename(
                columns = {
                    'publicationId'    :'publication_doi', 
                    'publicationYear'  :'publication_year', 
                    'publicationTitle' :'publication_title'
                }, 
                inplace = True,
                errors  = 'raise'
            )

        if isinstance(relational_publications_by_author_name_df, DataFrame):
        # Relational Dataframe clean up.
            relational_publications_by_author_name_df.drop(
                [
                    'internalID', 
                    'issue', 
                    'volume', 
                    'chapternumber', 
                    'given_name', 
                    'family_name'
                ], 
                axis    = 'columns', 
                inplace = True
            )

            # Rename DataFrame columns.
            relational_publications_by_author_name_df.rename(
                columns = {
                    'id'              : 'publication_doi', 
                    'publication_year': 'publication_year', 
                    'title'           : 'publication_title'
                }, 
                inplace = True,
                errors  = 'raise'
            )
            
        # Concatenate both DataFrame(s).
        publications_by_author_name_df: DataFrame = pd.concat(
                                                        [triplestore_publications_by_author_name_df, relational_publications_by_author_name_df],
                                                        ignore_index=True 
                                                    ).drop_duplicates().reset_index(drop=True)
            
        for idx, row in publications_by_author_name_df.iterrows():

            publication = Publication(
                            row['publication_doi'],
                            row['publication_year'],
                            row['publication_title']
                          )
            publications_by_author_name.append(publication)
            
        return publications_by_author_name


    def getDistinctPublisherOfPublications(self, _pubIdList: list[str]) -> list[Organization]:

        publishers_list = []

        #
        # Compose a pattern to template string parameter.
        #
        if isinstance(_pubIdList,list):
            # ONLY fo the SPARQL query we create a string (that represent a fake tuple)
            # as input of the `getDistinctPublisherOfPublications` for the `TriplestoreQueryProcessor`
            pubIdListTuple: str = '(' # Init the result string.
            counter: int    = 0   # Init the counter to know where i am iterating the list.

            # For each string in the list:
            for item in _pubIdList:
                
                # Incr counter.
                counter += 1

                if counter < len(_pubIdList):

                    pubIdListTuple = pubIdListTuple + "'" + item + "'" + ','

                elif counter == len(_pubIdList):

                    pubIdListTuple = pubIdListTuple + "'" + item + "'" + ')'

                else:
                    pubIdListTuple = pubIdListTuple + ')'


        for processor in self.queryProcessor:
            # Type guard.
            if isinstance(processor, TriplestoreQueryProcessor):
                triplestore_publishers_df = processor.getDistinctPublisherOfPublications(pubIdListTuple)
            elif isinstance(processor, RelationalQueryProcessor):
                relational_publishers_df = processor.getDistinctPublisherOfPublications(_pubIdList)

        if isinstance(triplestore_publishers_df, DataFrame):
            # Triplestore DtaFrame clean up.
            triplestore_publishers_df.drop(
                [
                    "organization"
                ],
                axis = 'columns',
                inplace = True
            )

            # Rename Data Frame columns.
            triplestore_publishers_df.rename(
                columns = {
                    'organizationId'   :'organization_id', 
                    'organizationName' :'organization_name', 
                }, 
                inplace = True,
                errors  = 'raise'
            )
            
        if isinstance(relational_publishers_df, DataFrame):
            # Relational Dataframe clean up.
            relational_publishers_df = relational_publishers_df.set_axis(
                [
                    'organization_internal_id',
                    'organization_id',
                    'organization_name',
                    'publication_doi'
                ],
                axis = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_publishers_df.drop(
                [
                    'organization_internal_id',
                    'publication_doi'
                ],
                axis = 'columns',
                inplace = True
            )

        # Concatenate both DataFrame(s).
        publishers_df: DataFrame = pd.concat(
                                        [triplestore_publishers_df, relational_publishers_df],
                                        ignore_index=True, 
                                   ).drop_duplicates().reset_index(drop=True) 

        for idx, row in publishers_df.iterrows():

                organization = Organization(row['organization_id'],row['organization_name'])
                
                publishers_list.append(organization)

        return publishers_list
