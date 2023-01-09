##########################
#                        #
# Rdb Processing module. #
#                        #
##########################

from pandas import read_csv, Series, DataFrame, read_sql, merge
import pandas as pd
from json import load
from sqlite3 import connect
from data_model import *

###################################
#                                 #
# RelationalProcessor Base Class. #
#                                 #
###################################


class RelationalProcessor(object):
    # None because it doesn't return anything.
    def __init__(self) -> None:
        self.dbPath: str = '' # string [0..1].

    def getDbPath(self) -> str:
        return self.dbPath

    def setDbPath(self, _path: str) -> bool:

        if isinstance(_path, str) and _path != '':
            self.dbPath = _path
            return True
        else: 
            return False


class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):

        if path.split(".")[1] == 'csv':
            with connect(self.getDbPath()) as con:
                con.commit()

            cursor = con.cursor()
            publications = read_csv(path,
                                    keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "type": "string",
                                        "publication_year": "int",
                                        "issue": "string",
                                        "volume": "string",
                                        "chapter": "string",
                                        "publication_venue": "string",
                                        "venue_type": "string",
                                        "publisher": "string",
                                        "event": "string"
                                    })

            # PUBBLICATIONS
            publication_ids = publications[["id"]]

            publication_internal_id = []
            for idx, row in publications.iterrows():
                publication_internal_id.append("publication-" + str(idx))

            publication_ids.insert(0, "PublicationID", Series(publication_internal_id, dtype="string"))
            with connect(self.getDbPath()) as con:

                create_publication_ids = '''
                                  DROP TABLE IF EXISTS PubID;
                                  CREATE TABLE IF NOT EXISTS PubID
                                  (PublicationID, id);
                                '''

                cursor.executescript(create_publication_ids)
                publication_ids.to_sql('PubID', con, index=False, if_exists='replace')

            # Venues
            VenueID = pd.DataFrame({'VenueID': pd.Series(dtype='str'),
                                    'id': pd.Series(dtype='str')
                                    })
            with connect(self.getDbPath()) as con:

                create_VenueID = '''
                                  DROP TABLE IF EXISTS VenueID;
                                  CREATE TABLE IF NOT EXISTS VenueID
                                  (VenueID, id);
                                '''

                cursor.executescript(create_VenueID)
                VenueID.to_sql('VenueID', con, index=False, if_exists='replace')

            # Persons ID
            PersonID = pd.DataFrame({'person_internal_id': pd.Series(dtype='str'),
                                     'id': pd.Series(dtype='str')
                                     })
            with connect(self.getDbPath()) as con:

                create_PersonID = '''
                                  DROP TABLE IF EXISTS PersonID;
                                  CREATE TABLE IF NOT EXISTS PersonID
                                  (person_internal_id, id);
                                '''

                cursor.executescript(create_PersonID)
                PersonID.to_sql('PersonID', con, index=False, if_exists='replace')

            # Person Table
            PersonTable = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                        'given_name': pd.Series(dtype='str'),
                                        'family_name': pd.Series(dtype='str')
                                        })
            with connect(self.getDbPath()) as con:

                create_PersonTable = '''
                                  DROP TABLE IF EXISTS Person;
                                  CREATE TABLE IF NOT EXISTS Person
                                  (internalID, given_name, family_name);
                                '''

                cursor.executescript(create_PersonID)
                PersonTable.to_sql('Person', con, index=False, if_exists='replace')

            # Author Table
            Authors = pd.DataFrame({'AuthorID': pd.Series(dtype='str'),
                                    'person_internal_id': pd.Series(dtype='str')
                                    })
            with connect(self.getDbPath()) as con:

                create_Authors = '''
                                  DROP TABLE IF EXISTS Author;
                                  CREATE TABLE IF NOT EXISTS Author
                                  (AuthorID, person_internal_id);
                                '''

                cursor.executescript(create_Authors)
                Authors.to_sql('Author', con, index=False, if_exists='replace')

            # Organization Table
            OrganizationTable = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                              'name': pd.Series(dtype='str')
                                              })
            with connect(self.getDbPath()) as con:

                create_OrganizationTable = '''
                                  DROP TABLE IF EXISTS Organization;
                                  CREATE TABLE IF NOT EXISTS Organization
                                  (internalID, name);
                                '''

                cursor.executescript(create_OrganizationTable)
                OrganizationTable.to_sql('Organization', con, index=False, if_exists='replace')

            # for Organization ID table
            OrganizationsID_table = pd.DataFrame({'OrgID': pd.Series(dtype='str'),
                                                  'id': pd.Series(dtype='str')
                                                  })
            with connect(self.getDbPath()) as con:

                create_OrganizationsID_table = '''
                                  DROP TABLE IF EXISTS OrgID;
                                  CREATE TABLE IF NOT EXISTS OrgID
                                  (OrgID, id);
                                '''

                cursor.executescript(create_OrganizationsID_table)
                OrganizationsID_table.to_sql('OrgID', con, index=False, if_exists='replace')

            # citations table
            citationTable = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                          'PublicationID': pd.Series(dtype='str')
                                          })
            with connect(self.getDbPath()) as con:

                create_citationTable = '''
                                  DROP TABLE IF EXISTS Cites;
                                  CREATE TABLE IF NOT EXISTS Cites
                                  (internalID, PublicationID);
                                '''

                cursor.executescript(create_citationTable)
                citationTable.to_sql('Cites', con, index=False, if_exists='replace')

            journals = publications.query("venue_type == 'journal'")
            journals = journals.reset_index()

            journals_df = journals[["publication_venue", "id", "publisher"]]

            venues_id_count = 0
            lst1 = []
            lst2 = []
            for idx, row in journals.iterrows():
                lst1.append("venue-" + str(idx + venues_id_count))
                lst2.append(row["id"])
                last_venue = idx + venues_id_count

            venues_id_journal = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })

            journal_temp = merge(journals_df, venues_id_journal, left_on="id", right_on="doi", how="left")
            journalfinal = merge(journal_temp, OrganizationsID_table, left_on="publisher", right_on="id", how="left")

            if len(OrganizationsID_table) == 0:
                JournalTable = journalfinal[["VenueID", "publication_venue", "publisher"]]
                JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
                JournalTable = JournalTable.rename(columns={"publisher": "Publisher"})
                JournalTable = JournalTable.rename(columns={"publication_venue": "title"})
            else:
                JournalTable = journalfinal[["VenueID", "publication_venue", "OrgID"]]
                JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
                JournalTable = JournalTable.rename(columns={"OrgID": "Publisher"})
                JournalTable = JournalTable.rename(columns={"publication_venue": "title"})

            with connect(self.getDbPath()) as con:

                create_journaltable = '''
                                  DROP TABLE IF EXISTS Journal;
                                  CREATE TABLE IF NOT EXISTS Journal
                                  (VenueID, publication_venue, OrgID);
                                '''

                cursor.executescript(create_journaltable)
                JournalTable.to_sql('Journal', con, index=False, if_exists='replace')

            # Book Table
            books = publications.query("venue_type == 'book'")
            books = books.reset_index()

            books_df = books[["publication_venue", "id", "publisher"]]

            venues_id_count2 = 0
            lst1 = []
            lst2 = []
            for idx, row in books.iterrows():
                lst1.append("venue-" + str(idx + venues_id_count2 + last_venue))
                lst2.append(row["id"])
                last_last_venue = idx + venues_id_count2 + last_venue + 1

            venues_id_book = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })
            book_temp = merge(books_df, venues_id_book, left_on="id", right_on="doi", how="left")
            bookfinal = merge(book_temp, OrganizationsID_table, left_on="publisher", right_on="id", how="left")
            if len(OrganizationsID_table) == 0:

                BookTable = bookfinal[["VenueID", "publication_venue", "publisher"]]
                BookTable = BookTable.rename(columns={"VenueID": "internalID"})
                BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
                BookTable = BookTable.rename(columns={"publication_venue": "title"})
            else:
                BookTable = bookfinal[["VenueID", "publication_venue", "OrgID"]]
                BookTable = BookTable.rename(columns={"VenueID": "internalID"})
                BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
                BookTable = BookTable.rename(columns={"publication_venue": "title"})

            with connect(self.getDbPath()) as con:

                create_booktable = '''
                                  DROP TABLE IF EXISTS Book;
                                  CREATE TABLE IF NOT EXISTS Book
                                  (internalID, title, Publisher);
                                '''

                cursor.executescript(create_booktable)
                BookTable.to_sql('Book', con, index=False, if_exists='replace')

            ##Proceeding Table

            proceedings = publications.query("venue_type == 'proceedings'")

            proceedings_df = proceedings[["publication_venue", "id", "publisher", "event"]]

            venues_id_count3 = 0
            lst1 = []
            lst2 = []
            for idx, row in proceedings.iterrows():
                lst1.append("venue-" + str(idx + venues_id_count2 + last_last_venue))
                lst2.append(row["id"])

            venues_id_pp = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })

            proceeding1 = merge(proceedings_df, venues_id_pp, left_on="id", right_on="doi", how="left")
            proceedingfinal = merge(proceeding1, OrganizationsID_table, left_on="publisher", right_on="id", how="left")

            if len(OrganizationsID_table) == 0:
                ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "publisher", "event"]]
                ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
                ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
                ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})
            else:
                ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "OrgID", "event"]]
                ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
                ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
                ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})

            with connect(self.getDbPath()) as con:

                create_proceedingtable = '''
                                  DROP TABLE IF EXISTS Proceeding;
                                  CREATE TABLE IF NOT EXISTS Proceeding
                                  (internalID, title, Publisher, event);
                                '''

                cursor.executescript(create_proceedingtable)
                ProceedingTable.to_sql('Proceeding', con, index=False, if_exists='replace')

            a = pd.DataFrame({'AuthorID': pd.Series(dtype='str'),
                              'person_internal_id': pd.Series(dtype='str')
                              })

            test_df = merge(publications, a, left_on="id", right_on="AuthorID", how="left")
            nan_Authors = test_df[test_df['AuthorID'].isna()]
            nan_Authors = nan_Authors[['AuthorID', 'person_internal_id']]
            notnan_Authors = test_df[test_df['AuthorID'].notna()]
            notnan_Authors = notnan_Authors[['AuthorID', 'person_internal_id']]

            pubs_dict = (publication_ids.set_index('id').T.to_dict('list'))
            lst1 = []
            lst2 = []
            lst3 = []
            for idx, row in notnan_Authors.iterrows():
                if pubs_dict.get(row["AuthorID"], None) != None:
                    lst1.append(row["AuthorID"])
                    lst2.append(row["person_internal_id"])
                    pub = pubs_dict[row["AuthorID"]][0]
                    id = pub.split("-")[1]
                    auth = "authors-" + id
                    lst3.append(auth)

            newAuth = DataFrame({
                "doi": Series(lst1, dtype="string", name="doi"),
                "person_internal_id": Series(lst2, dtype="string", name="person_internal_id"),
                "AuthorsID": Series(lst3, dtype="string", name="AuthorsID")
            })

            Auth_f = newAuth[["AuthorsID", "person_internal_id"]]
            Auth_f = Auth_f.rename(columns={"AuthorsID": "AuthorID"})

            Authors = pd.concat([Auth_f, nan_Authors])
            Authors = Authors[Authors['AuthorID'].notna()]

            # Journal articles table
            journal_articles = publications.query("type == 'journal-article'")

            pub_joined = merge(publication_ids, journal_articles, left_on="id", right_on="id")

            j = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                              'doi': pd.Series(dtype='str')
                              })

            cit_joined = merge(pub_joined, j, left_on="id", right_on="doi", how="left")
            cit_joined = cit_joined[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "id"]]
            cit_joined = cit_joined.drop_duplicates()

            new_join = merge(cit_joined, newAuth, left_on="id", right_on="doi", how="left")
            new_join = new_join[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "id", "AuthorsID"]]
            authors_joined = new_join.drop_duplicates()

            v = venues_id_journal.drop_duplicates()
            v = v.rename(columns={
                "doi": "doi_v"})
            venue_joined = merge(authors_joined, v, left_on="id", right_on="doi_v", how="left")
            venue_joined = venue_joined.drop_duplicates()
            venue_joined = venue_joined[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID", "issue", "volume"]]

            JournalArticleTable = venue_joined.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorsID": "author",
                "VenueID": "PublicationVenue"})

            with connect(self.getDbPath()) as con:

                create_journalarticletable = '''
                                  DROP TABLE IF EXISTS JournalArticle;
                                  CREATE TABLE IF NOT EXISTS JournalArticle
                                  (internalID, cites, author, PublicationVenue);
                                '''

                cursor.executescript(create_journalarticletable)
                JournalArticleTable.to_sql('JournalArticle', con, index=False, if_exists='replace')

            # Book chapter table

            book_chapters = publications.query("type == 'book-chapter'")
            join_pub_bookchapter = merge(publication_ids, book_chapters, left_on="id", right_on="id")

            df_temp_join = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                         'doi': pd.Series(dtype='str')
                                         })

            join_cites_bookch = merge(join_pub_bookchapter, df_temp_join, left_on="id", right_on="doi", how="left")

            join_cites_bookch = join_cites_bookch[
                ["PublicationID", "publication_year", "title", "internalID", "chapter", "id"]]

            join_cites_bookch = join_cites_bookch.drop_duplicates()

            new_join_bc = merge(join_cites_bookch, newAuth, left_on="id", right_on="doi", how="left")
            new_join_bc = new_join_bc[
                ["PublicationID", "publication_year", "title", "internalID", "chapter", "id", "AuthorsID"]]
            join_authors_bookch = new_join_bc.drop_duplicates()

            df_temp_venue = venues_id_book.drop_duplicates()
            df_temp_venue = df_temp_venue.rename(columns={
                "doi": "doi_v"})

            join_venues_bookch = merge(join_authors_bookch, df_temp_venue, left_on="id", right_on="doi_v", how="left")
            join_venues_bookch = join_venues_bookch.drop_duplicates()
            join_venues_bookch = join_venues_bookch[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID", "chapter"]]

            BookChapterTable = join_venues_bookch.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorsID": "author",
                "VenueID": "PublicationVenue",
                "chapter": "chapternumber"
            })

            with connect(self.getDbPath()) as con:

                create_bookchaptertable = '''
                                  DROP TABLE IF EXISTS BookChapter;
                                  CREATE TABLE IF NOT EXISTS BookChapter
                                  (internalID, cites, author, PublicationVenue);
                                '''

                cursor.executescript(create_bookchaptertable)
                BookChapterTable.to_sql('BookChapter', con, index=False, if_exists='replace')

            # Proceeding Papers Table

            proceeding_papers = publications.query("type == 'proceedings-paper'")
            join_publications_proceedings = merge(publication_ids, proceeding_papers, left_on="id", right_on="id")

            j_2 = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                'doi': pd.Series(dtype='str')
                                })

            join_cites_proceedings = merge(join_publications_proceedings, j_2, left_on="id", right_on="doi", how="left")

            join_cites_proceedings = join_cites_proceedings[
                ["PublicationID", "publication_year", "title", "internalID", "id"]]

            join_cites_proceedings = join_cites_proceedings.drop_duplicates()

            new_join_pp = merge(join_cites_proceedings, newAuth, left_on="id", right_on="doi", how="left")
            new_join_pp = new_join_pp[
                ["PublicationID", "publication_year", "title", "internalID", "id", "AuthorsID"]]
            join_authors_proceedings = new_join_pp.drop_duplicates()
            v_2 = venues_id_pp.drop_duplicates()
            v_2 = v_2.rename(columns={
                "doi": "doi_v"})

            join_venues_proceedings = merge(join_authors_proceedings, v_2, left_on="id", right_on="doi_v", how="left")
            join_venues_proceedings = join_venues_proceedings.drop_duplicates()
            join_venues_proceedings = join_venues_proceedings[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID"]]

            ProceedingsPapersTable = join_venues_proceedings.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorsID": "author",
                "VenueID": "PublicationVenue"
            })
            with connect(self.getDbPath()) as con:

                create_proceedingspaperstable = '''
                                  DROP TABLE IF EXISTS ProceedingPaper;
                                  CREATE TABLE IF NOT EXISTS ProceedingPaper
                                  (internalID, cites, author, PublicationVenue);
                                '''

                cursor.executescript(create_proceedingspaperstable)
                ProceedingsPapersTable.to_sql('ProceedingPaper', con, index=False, if_exists='replace')

        if path.split(".")[1] == 'json':

            con = connect(self.getDbPath(), uri=True)
            query = """
                    SELECT  a.id, a.title, a.type,
                    a.publication_year , a.issue, a.volume, a.chapter,  a.venue_type, a.publication_venue,
                    a.publisher, a.event
                    FROM
                    (
                    Select PubID.id as id, JournalArticle.title as title, 'journal-article' as type,
                    publication_year , issue, volume, 'NA' as chapter, 'journal' as venue_type, Journal.title as publication_venue,
                    OrgID.id as publisher, 'NA' as event
                    FROM JournalArticle
                    JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                    Left JOIN Journal ON JournalArticle.PublicationVenue == Journal.internalID
                    Left JOIN OrgID ON Journal.Publisher == OrgID.OrgID

                    UNION
                    SELECT PubID.id as id, BookChapter.title as title, 'book-chapter' as type, publication_year , 'NA' as issue,'NA' as volume, chapternumber, 'book' as venue_type, Book.title as publication_venue,OrgID.id as publisher, 'NA' as event
                    FROM BookChapter
                    JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                    Left JOIN Book ON BookChapter.PublicationVenue == Book.internalID
                    Left JOIN OrgID ON Book.Publisher == OrgID.OrgID

                    UNION
                    SELECT PubID.id as id, ProceedingPaper.title as title, 'proceedings-paper' as type, publication_year , 'NA' as issue,'NA' as volume,  'NA' as chapternumber, 'proceedings' as venue_type, Proceeding.title as publication_venue,OrgID.id as publisher, event
                    FROM ProceedingPaper
                    JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                    Left JOIN Proceeding ON ProceedingPaper.PublicationVenue == Proceeding.internalID
                    Left JOIN OrgID ON Proceeding.Publisher == OrgID.OrgID) a

                    ;
                    """
            publications = read_sql(query, con)

            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)

                # creating venue
                venues = json_doc["venues_id"]

                lst_doi = []
                lst_issn = []

                for key in venues:
                    for item in venues[key]:
                        lst_doi.append(key)
                        lst_issn.append(item)

                venues_df = DataFrame({
                    "doi": Series(lst_doi, dtype="string", name="doi"),
                    "issn": Series(lst_issn, dtype="string", name="issn")
                })

                venue_internal_id = []

                with connect(self.getDbPath()) as con:
                    query = """

                    SELECT Journal.internalID as internalID, PubID.id as id
                    FROM JournalArticle
                    JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                    JOIN Journal ON JournalArticle.PublicationVenue == Journal.internalID

                    UNION

                    SELECT Book.internalID as internalID, PubID.id as id
                    FROM BookChapter
                    JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                    JOIN Book ON BookChapter.PublicationVenue == Book.internalID

                    UNION

                    SELECT Proceeding.internalID as internalID, PubID.id as id
                    FROM ProceedingPaper
                    JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                    JOIN Proceeding ON ProceedingPaper.PublicationVenue == Proceeding.internalID

                    """
                    df_sql = read_sql(query, con)

                    df_sql = df_sql.values.tolist()

                    ven_dict = {}
                    for elem in df_sql:
                        if elem[1] in ven_dict:
                            ven_dict[elem[1]].append(elem[0])
                        else:
                            ven_dict[elem[1]] = [elem[0]]

                    l1 = []
                    l2 = []
                    for idx, row in venues_df.iterrows():

                        if ven_dict.get(row["doi"], None) != None:
                            l1.append(row["doi"])
                            l2.append(ven_dict.get(row["doi"])[0])

                            venue_internal_id.append(ven_dict.get(row["doi"])[0])

                    venueID_df = DataFrame({
                        "doiordinati": Series(l1, dtype="string", name="doiordinati"),
                        "VenueID": Series(l2, dtype="string", name="VenueID")
                    })

                    venueID_df = venueID_df.drop_duplicates()
                    doi_merge = merge(venueID_df, venues_df, left_on="doiordinati", right_on="doi")

                    VenueID = doi_merge[["VenueID", "issn"]]
                    VenueID = VenueID.rename(columns={"issn": "id"})

            # Strating for authors... retrieving from JSON data
            authors = json_doc["authors"]

            lst_item = []
            lst_doi_authors = []

            for key in authors:
                for item in authors[key]:
                    lst_doi_authors.append(key)
                    lst_item.append(item)

            le_persone = []

            for item in lst_item:
                if item not in le_persone:
                    le_persone.append(item)

            familylst = []
            for item in le_persone:
                familylst.append(item.get("family"))

            givenlst = []
            for item in le_persone:
                givenlst.append(item.get("given"))

            orcidlst = []
            for item in le_persone:
                orcidlst.append(item.get("orcid"))

            le_persone = DataFrame({
                "orcid": Series(orcidlst, dtype="string", name="orcid"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
            })

            person_internal_id = []

            # finding how many persons

            with connect(self.getDbPath()) as con:
                query = "SELECT count(*) FROM Person"
                df_sql = read_sql(query, con)
                person_count = df_sql.values.tolist()[0][0] + 1

            # append person internal ids
            for idx, row in le_persone.iterrows():
                person_internal_id.append("person-" + str(idx + person_count))

            PersonID = DataFrame({
                "person_internal_id": Series(person_internal_id, dtype="string", name="person_internal_id"),
                "orcid": Series(orcidlst, dtype="string", name="orcid")
            })

            PersonID = PersonID.rename(columns={"orcid": "id"})

            PersonTable = DataFrame({
                "person_internal_id": Series(person_internal_id, dtype="string", name="person_internal_id"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
            })

            PersonTable = PersonTable.rename(columns={"person_internal_id": "internalID"})

            # Author Table
            lst_doi_single_authors = []
            for key in authors:
                lst_doi_single_authors.append(key)

            single_authors_df = DataFrame({
                "doi_single_authors": Series(lst_doi_single_authors, dtype="string", name="doi_single_authors"),
            })

            authors_internal_id = []
            with connect(self.getDbPath()) as con:
                query = "SELECT count(*) FROM Author"
                df_sql = read_sql(query, con)
                author_count = df_sql.values.tolist()[0][0] + 1

            for idx, row in single_authors_df.iterrows():
                authors_internal_id.append("authors-" + str(idx + author_count))

            authorsID_df = DataFrame({
                "doi_single_authors": Series(lst_doi_single_authors, dtype="string", name="doi_single_authors"),
                "AuthorID": Series(authors_internal_id, dtype="string", name="AuthorID")
            })

            orcidlst_collection = []
            for item in lst_item:
                orcidlst_collection.append(item.get("orcid"))

                doi_authors_orcidlst = DataFrame({
                    "lst_doi_authors": Series(lst_doi_authors, dtype="string", name="lst_doi_authors"),
                    "orcidlst_collection": Series(orcidlst_collection, dtype="string", name="orcidlst_collection"),
                })

            unione = merge(authorsID_df, doi_authors_orcidlst, left_on="doi_single_authors", right_on="lst_doi_authors")

            AuthorsID = unione[["AuthorID"]]

            PersonTable1 = DataFrame({
                "person_internal_id": Series(person_internal_id, dtype="string", name="person_internal_id"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
                "orcid": Series(orcidlst, dtype="string", name="orcid")
            })

            doi_orcid_persontable_merged = merge(doi_authors_orcidlst, PersonTable1, left_on="orcidlst_collection",
                                                 right_on="orcid")

            union_doi_authors_personid = doi_orcid_persontable_merged[["lst_doi_authors", "person_internal_id"]]

            union_merged_authorid_df = merge(union_doi_authors_personid, authorsID_df, left_on="lst_doi_authors",
                                             right_on="doi_single_authors")

            Authors = union_merged_authorid_df[["lst_doi_authors", "person_internal_id"]]
            Authors = Authors.rename(columns={"lst_doi_authors": "AuthorID"})

            # publicatoins id
            publication_ids = publications[["id"]]

            publication_internal_id = []

            with connect(self.getDbPath()) as con:
                query = """
                SELECT internalID,  PubID.id as id
                   FROM JournalArticle
                   JOIN PubID ON JournalArticle.internalID == PubID.PublicationID
                UNION
                SELECT internalID,  PubID.id as id
                   FROM BookChapter
                   JOIN PubID ON BookChapter.internalID == PubID.PublicationID
                UNION
                SELECT internalID,  PubID.id as id
                   FROM ProceedingPaper
                   JOIN PubID ON ProceedingPaper.internalID == PubID.PublicationID
                """
                df_sql = read_sql(query, con)
                df_sql = df_sql.values.tolist()

                pub_dict = {}
                for elem in df_sql:
                    if elem[1] in pub_dict:
                        pub_dict[elem[1]].append(elem[0])
                    else:
                        pub_dict[elem[1]] = [elem[0]]

                for idx, row in publications.iterrows():
                    publication_internal_id.append(pub_dict.get(row["id"])[0])

            publication_ids.insert(0, "PublicationID", Series(publication_internal_id, dtype="string"))

            # organization table
            organization = json_doc["publishers"]

            j = "name"
            name_list = [val[j] for key, val in organization.items()]

            OrganizationTable = DataFrame({
                "name": Series(name_list, dtype="string", name="name")
            })

            organization_internal_id = []

            with connect(self.getDbPath()) as con:
                query = "SELECT count(*) FROM Organization"
                df_sql = read_sql(query, con)
                org_count = df_sql.values.tolist()[0][0] + 1

            for idx, row in OrganizationTable.iterrows():
                organization_internal_id.append("Org-" + str(idx + org_count))

            OrganizationTable.insert(0, "OrgID", Series(organization_internal_id, dtype="string"))
            OrganizationTable = OrganizationTable.rename(columns={"OrgID": "internalID"})

            # Organization ids
            crossref_list = []

            for k in organization:
                crossref_list.append(k)

            orgId_df = DataFrame({
                "id": Series(crossref_list, dtype="string", name="id"),
                "name": Series(name_list, dtype="string", name="name")
            })

            orgId_joined = merge(OrganizationTable, orgId_df, left_on="name", right_on="name")

            OrganizationsID_table = orgId_joined[
                ["internalID", "id"]]
            OrganizationsID_table = OrganizationsID_table.rename(columns={"internalID": "OrgID"})

            # for linking between citation and publication

            citations = json_doc["references"]

            lst_doi = []
            lst_cit = []
            cit_id = []
            count = 0
            for key in citations:
                if len(citations[key]) == 0:
                    cit_id.append("cite-" + str(count))
                    lst_doi.append(key)
                    lst_cit.append("NA")
                else:
                    for item in citations[key]:
                        if item == None:
                            lst_cit.append(None)
                        else:
                            cit_id.append("cite-" + str(count))
                            lst_doi.append(key)
                            lst_cit.append(item)
                    count = count + 1

            cite1_df = DataFrame({
                "cit": Series(lst_cit, dtype="string", name="citeid"),
                "doi": Series(lst_doi, dtype="string", name="doi"),
                "internalID": Series(cit_id, dtype="string", name="internalID")

            })

            cite_df = merge(publication_ids, cite1_df, left_on="id", right_on="cit")

            citationTable = cite_df[["internalID", "PublicationID"]]

            # Journal Table
            journals = publications.query("venue_type == 'journal'")

            journals_df = journals[["publication_venue", "id", "publisher"]]

            VenueID_journal = doi_merge[["VenueID", "doi"]]
            VenueID_journal = VenueID_journal.drop_duplicates()

            with connect(self.getDbPath()) as con:
                query = """SELECT PubID.id, Journal.publisher as Publisher FROM Journal join JournalArticle on JournalArticle.PublicationVenue  = Journal.internalID
                        join PubID on JournalArticle.internalID = PubID.PublicationID
                        """
                publisher = read_sql(query, con)

            OrganizationsID_journal_table = merge(publisher, OrganizationsID_table, left_on="Publisher", right_on="id")
            journal_temp = merge(journals_df, VenueID_journal, left_on="id", right_on="doi")
            journalfinal = merge(journal_temp, OrganizationsID_journal_table, left_on="id", right_on="id_x")
            JournalTable = journalfinal[["VenueID", "publication_venue", "OrgID"]]
            JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
            JournalTable = JournalTable.rename(columns={"OrgID": "Publisher"})
            JournalTable = JournalTable.rename(columns={"publication_venue": "title"})

            # Book table
            books = publications.query("venue_type == 'book'")

            books_df = books[["publication_venue", "id", "publisher"]]

            VenueID_book = doi_merge[["VenueID", "doi"]]
            VenueID_book = VenueID_book.drop_duplicates()

            with connect(self.getDbPath()) as con:
                query = """SELECT PubID.id, Book.publisher as Publisher FROM Book join BookChapter
                on BookChapter.PublicationVenue  = Book.internalID
                        join PubID on BookChapter.internalID = PubID.PublicationID
                        """
                publisher = read_sql(query, con)

            OrganizationsID_book_table = merge(publisher, OrganizationsID_table, left_on="Publisher", right_on="id")
            book_temp = merge(books_df, VenueID_book, left_on="id", right_on="doi")
            journal_temp = merge(journals_df, VenueID_journal, left_on="id", right_on="doi")

            journalfinal = merge(journal_temp, OrganizationsID_journal_table, left_on="id", right_on="id_x")
            bookfinal = merge(book_temp, OrganizationsID_book_table, left_on="id", right_on="id_x")

            BookTable = bookfinal[["VenueID", "publication_venue", "OrgID"]]
            BookTable = BookTable.rename(columns={"VenueID": "internalID"})
            BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
            BookTable = BookTable.rename(columns={"publication_venue": "title"})

            # proceeding table
            proceedings = publications.query("venue_type == 'proceedings'")

            proceedings_df = proceedings[["publication_venue", "id", "publisher", "event"]]

            VenueID_proceedings = doi_merge[["VenueID", "doi"]]
            VenueID_proceedings = VenueID_proceedings.drop_duplicates()

            with connect(self.getDbPath()) as con:
                query = """SELECT PubID.id, Proceeding.publisher as Publisher FROM Proceeding join ProceedingPaper
                on ProceedingPaper.PublicationVenue  = Proceeding.internalID
                        join PubID on ProceedingPaper.internalID = PubID.PublicationID
                        """
                publisher = read_sql(query, con)

            OrganizationsID_proceeding_table = merge(publisher, OrganizationsID_table, left_on="Publisher",
                                                     right_on="id")

            proceeding1 = merge(proceedings_df, VenueID_proceedings, left_on="id", right_on="doi")

            proceedingfinal = merge(proceeding1, OrganizationsID_proceeding_table, left_on="id", right_on="id_x")

            ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "OrgID", "event"]]
            ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
            ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
            ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})

            # joining Authors id with Publications

            join_author_to_pub = merge(publications, unione, left_on="id", right_on="doi_single_authors", how="left")
            join_author_to_pub = join_author_to_pub[["doi_single_authors", "AuthorID"]]
            join_author_to_pub = join_author_to_pub.drop_duplicates()
            join_author_to_pub = join_author_to_pub[join_author_to_pub['doi_single_authors'].notna()]

            dics = join_author_to_pub.set_index('doi_single_authors').T.to_dict('list')

            lst1 = []
            lst2 = []
            for idx, row in Authors.iterrows():
                AID = row["AuthorID"]
                p_internal_id = row["person_internal_id"]
                if dics.get(AID, None) == None:
                    lst1.append(AID)
                else:
                    lst1.append(dics[AID][0])
                lst2.append(p_internal_id)

            Authors = DataFrame({
                "AuthorID": Series(lst1, dtype="string", name="AuthorID"),
                "person_internal_id": Series(lst2, dtype="string", name="person_internal_id")

            })

            # Journal Articles Table
            journal_articles = publications.query("type == 'journal-article'")

            pub_joined = merge(publication_ids, journal_articles, left_on="id", right_on="id")

            cit_joined = merge(pub_joined, cite1_df, left_on="id", right_on="doi")
            cit_joined = cit_joined[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "doi"]]

            cit_joined = cit_joined.drop_duplicates()

            authors_joined = merge(cit_joined, unione, left_on="doi", right_on="doi_single_authors")
            authors_joined = authors_joined.drop_duplicates()

            authors_joined = authors_joined[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "doi", "AuthorID"]]

            venue_joined = merge(authors_joined, venueID_df, left_on="doi", right_on="doiordinati")
            venue_joined = venue_joined.drop_duplicates()
            venue_joined = venue_joined[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID", "issue", "volume"]]

            JournalArticleTable = venue_joined.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue"})

            # Book chapter table
            book_chapters = publications.query("type == 'book-chapter'")

            join_pub_bookchapter = merge(publication_ids, book_chapters, left_on="id", right_on="id")

            join_cites_bookch = merge(join_pub_bookchapter, cite1_df, left_on="id", right_on="doi")
            join_cites_bookch = join_cites_bookch[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "chapter"]]

            join_cites_bookch = join_cites_bookch.drop_duplicates()

            join_authors_bookch = merge(join_cites_bookch, unione, left_on="doi", right_on="doi_single_authors")
            join_authors_bookch = join_authors_bookch.drop_duplicates()
            join_authors_bookch = join_authors_bookch[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "AuthorID", "chapter"]]

            join_venues_bookch = merge(join_authors_bookch, venueID_df, left_on="doi", right_on="doiordinati")
            join_venues_bookch = join_venues_bookch.drop_duplicates()
            join_venues_bookch = join_venues_bookch[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID", "chapter"]]

            BookChapterTable = join_venues_bookch.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue",
                "chapter": "chapternumber"})

            # Proceeding Papers Table

            proceeding_papers = publications.query("type == 'proceedings-paper'")

            join_publications_proceedings = merge(publication_ids, proceeding_papers, left_on="id", right_on="id")

            join_cites_proceedings = merge(join_publications_proceedings, cite1_df, left_on="id", right_on="doi")
            join_cites_proceedings = join_cites_proceedings[
                ["PublicationID", "publication_year", "title", "internalID", "doi"]]

            join_cites_proceedings = join_cites_proceedings.drop_duplicates()

            join_authors_proceedings = merge(join_cites_proceedings, unione, left_on="doi",
                                             right_on="doi_single_authors")
            join_authors_proceedings = join_authors_proceedings.drop_duplicates()
            join_authors_proceedings = join_authors_proceedings[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "AuthorID"]]

            join_venues_proceedings = merge(join_authors_proceedings, venueID_df, left_on="doi", right_on="doiordinati")
            join_venues_proceedings = join_venues_proceedings.drop_duplicates()
            join_venues_proceedings = join_venues_proceedings[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID"]]

            ProceedingsPapersTable = join_venues_proceedings.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue"})

            # Populating Database

            with connect(self.getDbPath()) as con:
                con.commit()
            with connect(self.getDbPath()) as con:

                OrganizationTable.to_sql("Organization", con, if_exists="append", index=False)
                OrganizationsID_table.to_sql("OrgID", con, if_exists="append", index=False)
                PersonTable.to_sql("Person", con, if_exists="append", index=False)
                PersonID.to_sql("PersonID", con, if_exists="append", index=False)
                Authors.to_sql("Author", con, if_exists="append", index=False)
                citationTable.to_sql("Cites", con, if_exists="append", index=False)
                publication_ids.to_sql("PubID", con, if_exists="replace", index=False)
                VenueID.to_sql("VenueID", con, if_exists="append", index=False)
                BookTable.to_sql("Book", con, if_exists="replace", index=False)
                BookChapterTable.to_sql("BookChapter", con, if_exists="replace", index=False)
                JournalTable.to_sql("Journal", con, if_exists="replace", index=False)
                JournalArticleTable.to_sql("JournalArticle", con, if_exists="replace", index=False)
                ProceedingTable.to_sql("Proceeding", con, if_exists="replace", index=False)
                ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="replace", index=False)

        return True








# graph data processor internal.
from data_processors import GraphDataProcessor

####################################
#                                  #
# TriplestoreProcessor Base Class. #
#                                  #
####################################
class TriplestoreProcessor(object):
    # None because it doesn't return anything.
    def __init__(self) -> None:
        self.endpointUrl: str = '' # string [0..1].

    # getEndpointUrl(): string.
    def getEndpointUrl(self) -> str:
        return self.endpointUrl

    # setEndpointUrl(_url: string): boolean.
    def setEndpointUrl(self, _url: str) -> bool:

        if isinstance(_url, str) and _url != '':
            self.endpointUrl = _url
            return True
        else: 
            return False

################################################################################
#                                                                              #
# TriplestoreDataProcessor Class.                                              #
#                                                                              #
# uploadData(): -> bool.                                                       #
# it enables to upload the collection of data specified in the input file path #
# (either in CSV or JSON, according to the formats specified above)            #
# into the database.                                                           #
#                                                                              #
################################################################################
class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self) -> None:        

        super().__init__()  

        #
        # Init processor property with an instance of GraphDataProcessor class.
        # get the endpoint url from inherited class.
        #
        self.processor = GraphDataProcessor(self.getEndpointUrl())

    def uploadData(self, _path: str) -> bool:

        # Flag to return if all operation will be done correctly.
        data_has_been_uploaded: bool = False
        
        # Check if file is type of string and is not empty.
        files_are_valid: bool = isinstance(_path, str) and len(_path) > 0
        
        try:
            if files_are_valid:

                #
                # Process CSV.
                #
                if _path.endswith(".csv"):
                    
                    try:
                        self.processor.publicationsDfBuilder(_path)

                    except Exception:
                        data_has_been_uploaded = False
                        raise

                #
                # Process JSON.
                #
                elif _path.endswith(".json"):

                    try:
                        self.processor.authorsDfBuilder(_path)
                        self.processor.publishersDfBuilder(_path)
                        self.processor.referncesDfBuilder(_path)
                        self.processor.venuesDfBuilder(_path)
    
                    except Exception:
                        data_has_been_uploaded = False
                        raise
                    
                #
                # Build the rdf graph only if all data frames are filled correctly.
                #
                if self.processor.data_frames_has_been_built():
                    try:
                        # Build the rdf graph.
                        self.processor.graphBuilder()

                        # Deploy all triples to blazegraph.
                        self.processor.triplestoreDeploy()

                        data_has_been_uploaded = True
                    except Exception:
                        data_has_been_uploaded = False
                        raise

            else:
                data_has_been_uploaded = False
                raise Exception("-- ERR: The file you provided is not supported, or an empty string, or malformed.")

        except Exception as error:
            print(error)

        finally:
                return data_has_been_uploaded




#############################
#                           #
# Data processors module.   #
#                           # 
#############################

# pandas.
import pandas as pd
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
    def set_publications_df(self, _publications_df: DataFrame) -> None:
        # Check if the DataFrame is empty.
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

            # Read from json file only references objects.
            references = data_frame['references']

            #
            # Create the References DataFrame from json.
            # Data Frame will be exploded in two col named 'doi' and 'cites'.
            #
            references_df = DataFrame(references.items(), columns = ['doi','cites']).explode('doi')
            
            #
            # Convert references_df DataFrame object to a JSON string and
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

            # Read from json file only venues objects.
            venues = data_frame['venues_id']

            #
            # Create the Venues DataFrame from json.
            # Data Frame will be exploded in two col named 'doi' and 'venues_id'.
            #
            venues_df = DataFrame(venues.items(), columns = ['doi','ids']).explode('ids')
            
            #
            # Convert venues_df DataFrame object to a JSON string and
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
            # Data Frame will be exploded in two col named 'doi' and 'author'.
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
            # Data Frame will be exploded in two col named 'id' and 'publishers'.
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
            rdf_graph.add((subject, RDF.type, OrganizationURI))

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

                if venues_joined_row['id'] == row['id']:
                    #for item in venues_joined_row['ids']:
                    rdf_graph.add((subject, id, Literal(venues_joined_row['ids'])))
           
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

        # Create the Authors and Subject dictionary.
        authors_subject_dict: dict = {}
 
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

        # Create a DataFrame copy and drop useless columns.
        no_doi_authors_df = authors_df.copy()       
        no_doi_authors_df.drop(
            [
                'doi'
            ],
            axis = 'columns',
            inplace = True            
        )

        # Remove duplicates.
        unique_authors_df = no_doi_authors_df.drop_duplicates().copy().reset_index(drop=True)

        # Iterate over unique_authors_df for create Person triplets.
        for index, row in unique_authors_df.iterrows():

            # Create a resource id.
            author_local_id = 'person-' + str(index)

            # Create a subject.
            subject = URIRef(self.base_url + author_local_id)

            authors_subject_dict[row['orc_id']] = subject

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

        # Iterate over author_df to create internal dictionary for assign Authors to Publications.
        for index, row in authors_df.iterrows():

            if row['orc_id'] in authors_subject_dict.keys():

                # If the dictionary is empty than inizialite it with an empty set as value.
                if not self.author_internal_id_dict:
                    self.author_internal_id_dict[row['doi']] = set()

                #
                # If the value of 'doi' is already in dictonary, add the subject.
                # Otherwise create a new key and inizialite it with an empty set,
                # than add the subject.
                #
                if row['doi'] in self.author_internal_id_dict: 
                    # Iterate over the dictionary items.
                    for orc_id, person in authors_subject_dict.items():
                        if orc_id == row['orc_id']:
                                self.author_internal_id_dict[row['doi']].add(person)
                else:
                    self.author_internal_id_dict[row['doi']] = set()
                    # Iterate over the dictionary items.
                    for orc_id, person in authors_subject_dict.items():
                        if orc_id == row['orc_id']:
                                self.author_internal_id_dict[row['doi']].add(person)

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

        spinner = Spinner('-- INFO: Create relation with the Publication and References')
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
     
        # A serialized version of the graph is saved as JSON.
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
        endpoint_url = self.getEndpointUrl()
        
        # Parametrize query by passing Publication year.
        sparql_query = Template(PUBLICATIONS_PUBLISHED_IN_YEAR).substitute(YEAR = _year)
        
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql
            

    # getPublicationsByAuthorId(_id: str): -> DataFrame.
    def getPublicationsByAuthorId(self, _id: str) -> DataFrame:

        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing Author id.
        sparql_query = Template(PUBLICATIONS_BY_AUTHOR_ID).substitute(AUTHOR_ID = _id)
        
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getMostCitedPublication(): -> DataFrame. 
    def getMostCitedPublication(self) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        df_sparql = get(endpoint_url, MOST_CITED_PUBLICATION, True)

        return df_sparql

    
    # getMostCitedVenue(): -> DataFrame.
    def getMostCitedVenue(self) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        df_sparql = get(endpoint_url, MOST_CITED_VENUE, True)

        return df_sparql


    # getVenuesByPublisherId(_id: str): -> DataFrame.
    def getVenuesByPublisherId(self, _id: str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing publisher id (Organization).
        sparql_query = Template(VENUES_BY_PUBLISHER_ID).substitute(PUBLISHER_ID = _id)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getPublicationInVenue(_venueId: str): -> DataFrame.
    def getPublicationInVenue(self, _venueId: str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing Venue id.
        sparql_query = Template(PUBLICATION_IN_VENUE).substitute(VENUE_ID = _venueId)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getJournalArticlesInIssue(_issue: str, _volume: str, _journalId: str): -> DataFrame.
    def getJournalArticlesInIssue(self, _issue: str, _volume: str, _journalId: str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

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
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing Volume and Venue issn.
        sparql_query = Template(JOURNAL_ARTICLES_IN_VOLUME).substitute(
                                                                VOLUME   = _volume,
                                                                VENUE_ID = _journalId
                                                            )
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getJournalArticlesInJournal(_journalId: str): -> DataFrame.
    def getJournalArticlesInJournal(self, _journalId: str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing journal issn.
        sparql_query = Template(JOURNAL_ARTICLES_IN_JOURNAL).substitute(VENUE_ID = _journalId)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql

    # getProceedingsByEvent(_eventPartialName: str): -> DataFrame.
    def getProceedingsByEvent(self, _eventPartialName:str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing journal issn.
        sparql_query = Template(PROCEEDINGS_BY_EVENT).substitute(EVENT_PARTIAL_NAME = _eventPartialName)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql
    
    # getPublicationAuthors(_publicationId: str): -> DataFrame.
    def getPublicationAuthors(self, _publicationId: str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing publication doi.
        sparql_query = Template(PUBLICATION_AUTHORS).substitute(PUBLICATION_ID = _publicationId)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql


    # getPublicationsByAuthorName(_authorPartialName: str): -> DataFrame.
    def getPublicationsByAuthorName(self, _authorPartialName: str) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing Person name.
        sparql_query = Template(PUBLICATIONS_BY_AUTHOR_NAME).substitute(AUTHOR_NAME = _authorPartialName)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql
 

    # getDistinctPublisherOfPublications(_pubIdList: list[str]): -> DataFrame.
    def getDistinctPublisherOfPublications(self, _pubIdList: list[str]) -> DataFrame:
        endpoint_url = self.getEndpointUrl()

        # Parametrize query by passing a list of publication doi.
        sparql_query = Template(DISTINCT_PUBLISHER_OF_PUBLICATIONS).substitute(PUBLICATIONS_LIST = _pubIdList)
        df_sparql = get(endpoint_url, sparql_query, True)

        return df_sparql



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
            query = """SELECT Proceeding.internalID, title, Proceeding.event, VenueID.id
                               FROM Proceeding
                               LEFT JOIN VenueID ON VenueID.VenueID == Proceeding.internalID
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
                    'proceedings_event',
                    'proceedings_id'
                ],
                axis    = 'columns',
                #inplace = True # FutureWarning: DataFrame.set_axis 'inplace' keyword is deprecated and will be removed in a future version.
                copy = False
            )

            relational_proceeding_by_event_df.drop(
                [
                    'proceedings_internal_id',
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








################################################################################
#                                                                              #
# Queries.                                                                     #
#                                                                              #
################################################################################

from const import *
from string import Template
from query_processors import QueryProcessor
from data_model import *


#
# Publications published in year.
#
def do_query_publications_published_in_year(_generic_query_processor: QueryProcessor) -> None:

    # Define a counter to know how many item are retrieved.
    counter: int = 0

    # Define a list to collect items to print in the file.
    result_string_list: list = []

    #
    # Do the query,
    #
    res_publications_published_in_year: list[Publication] = _generic_query_processor.getPublicationsPublishedInYear(PUBLICATIONS_PUBLISHED_IN_YEAR_YEAR)

    # Build the template string.
    header_publications_published_in_year = 'Publications published in $YEAR:\n\n'
    # Pupulate the template string parameter.
    header_publications_published_in_year = Template(header_publications_published_in_year).substitute(YEAR = PUBLICATIONS_PUBLISHED_IN_YEAR_YEAR)

    #
    # Opens file and casts as f then, for item in the list write the file.
    #
    with open('./queries-results/publication_published_in_year.txt', 'w', encoding='utf-8') as f: 
        f.write(header_publications_published_in_year)

    if len(res_publications_published_in_year) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Publications found for the year "$YEAR"'
        print_info = Template(string_to_template).substitute(YEAR = PUBLICATIONS_PUBLISHED_IN_YEAR_YEAR)
        print(print_info)

        with open('./queries-results/publication_published_in_year.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getPublicationsPublishedInYear` produced zero results for the year "$YEAR"'
            print_info = Template(string_to_template).substitute(YEAR = PUBLICATIONS_PUBLISHED_IN_YEAR_YEAR)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_publications_published_in_year)

        # For each publication in the list:
        for publication in res_publications_published_in_year:
            counter += 1

            publication_doi  = publication.getIds()
            publication_name = publication.getTitle()
            publication_year = publication.getPublicationYear()
            
            # Build the template string.
            result_string = '\nDoi: $DOI.\nName: $NAME.\nYear: $YEAR.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        DOI  = publication_doi,
                                                        NAME = publication_name,
                                                        YEAR = publication_year
                                                    )
            
            # Append the result_string to the result list.
            result_string_list.append(result_string)
            
            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_publications_published_in_year):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/publication_published_in_year.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)
    #
    # File closed automatically.
    #

    print('-- INFO: "Publications published in year" query has been processed. ', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/publication_published_in_year.txt'' ')



#
# Publications by Author id.
#
def do_publications_by_author_id(_generic_query_processor: QueryProcessor) -> None:

    # Define a counter to know how many item are retrieved.
    counter = 0
    
    # Define a list to collect items to print in the file.
    result_string_list = []
    
    #
    # Do the query.
    #
    res_publications_by_author_id: list[Publication] = _generic_query_processor.getPublicationsByAuthorId(PUBLICATION_BY_AUTHOR_ID_AUTHOR_ID)

    # Build the template string.
    header_publications_by_author_id = 'List of publications by Author ID "$AUTHOR_ID":\n\n'
    # Pupulate the template string parameter.
    header_publications_by_author_id = Template(header_publications_by_author_id).substitute(AUTHOR_ID = PUBLICATION_BY_AUTHOR_ID_AUTHOR_ID)

    # Append the header title to the list.
    result_string_list.append(header_publications_by_author_id)

    if len(res_publications_by_author_id) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Publications found for the Author ID "$AUTHOR_ID"'
        print_info = Template(string_to_template).substitute(AUTHOR_ID = PUBLICATION_BY_AUTHOR_ID_AUTHOR_ID)
        print(print_info)

        with open('./queries-results/publication_by_author_id.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getPublicationsByAuthorId` produced zero results for the Author ID "$AUTHOR_ID"'
            print_info = Template(string_to_template).substitute(AUTHOR_ID = PUBLICATION_BY_AUTHOR_ID_AUTHOR_ID)
            f.write(print_info)

    else:
        # For each publication in the list:
        for publication in res_publications_by_author_id:
            counter += 1

            publication_doi  = publication.getIds()
            publication_name = publication.getTitle()
            
            # Build the template string.
            result_string = '\nDoi: $DOI.\nName: $NAME.\nAuthor ORC-ID: $AUTHOR_ID.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        DOI       = publication_doi,
                                                        NAME      = publication_name,
                                                        AUTHOR_ID = PUBLICATION_BY_AUTHOR_ID_AUTHOR_ID
                                                    )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_publications_by_author_id):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/publication_by_author_id.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Publications by author id" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/publication_by_author_id.txt'' ')



#
# Most cited Publication.
#
def do_most_cited_publication(_generic_query_processor: QueryProcessor) -> None:
   
    # Define a list to collect items to print in the file.
    result_string_list = []
    
    #
    # Do the query.
    #
    res_most_cited_publication: list[Publication] = _generic_query_processor.getMostCitedPublication()

    # Build the template string.
    header_most_cited_publication = 'The most cited Publication is:\n\n'

    # Append the header title to the list.
    result_string_list.append(header_most_cited_publication)

    publication_doi  = res_most_cited_publication.getIds()
    publication_name = res_most_cited_publication.getTitle()

    result_string = '\nDoi: $DOI.\nName: $NAME.\n'
    # Pupulate the template string parameters.
    result_string = Template(result_string).substitute(
                                                DOI  = publication_doi,
                                                NAME = publication_name
                                            )

    # Append the result_string to the result list.
    result_string_list.append(result_string)

    #
    # Opens file and casts as f then, for item in the list write the file.
    #
    with open('./queries-results/most_cited_publication.txt', 'w', encoding='utf-8') as f: 
        for string in result_string_list:
            f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Most cited Publication" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/most_cited_publication.txt'' ')



#
# Most cited Venue.
#
def do_most_cited_venue(_generic_query_processor: QueryProcessor) -> None:

    # Define a list to collect items to print in the file.
    result_string_list = []
    
    #
    # Do the query.
    #
    res_most_cited_venue: list[Venue] = _generic_query_processor.getMostCitedVenue()

    # Build the template string.
    header_most_cited_venue = 'The most cited Venue is:\n\n'

    # Append the header title to the list.
    result_string_list.append(header_most_cited_venue)

    venue_id   = res_most_cited_venue.getIds()
    venue_name = res_most_cited_venue.getTitle()

    result_string = '\nId: $ID.\nName: $NAME.\n'
    # Pupulate the template string parameters.
    result_string = Template(result_string).substitute(
                                                ID   = venue_id,
                                                NAME = venue_name
                                            )

    # Append the result_string to the result list.
    result_string_list.append(result_string)

    #
    # Opens file and casts as f then, for item in the list write the file.
    #
    with open('./queries-results/most_cited_venue.txt', 'w', encoding='utf-8') as f: 
        for string in result_string_list:
            f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Most cited Venue" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/most_cited_venue.txt'' ')



#
# Venues by Publisher id.
#
def do_venues_by_publisher_id(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []
    
    #
    # Do the query.
    #
    res_venues_by_publisher_id: list[Venue] = _generic_query_processor.getVenuesByPublisherId(VENUES_BY_PUBLISHER_ID_PUBLISHER_ID)

    # Build the template string.
    header_venues_by_publisher_id = 'Venues published by Publisher with id = $PUBLISHER_ID:\n\n'
    header_venues_by_publisher_id = Template(header_venues_by_publisher_id).substitute(PUBLISHER_ID = VENUES_BY_PUBLISHER_ID_PUBLISHER_ID)

    if len(res_venues_by_publisher_id) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No venues found for the Publisher Id "$PUBLISHER_ID"'
        print_info = Template(string_to_template).substitute(PUBLISHER_ID = VENUES_BY_PUBLISHER_ID_PUBLISHER_ID)
        print(print_info)

        with open('./queries-results/venues_by_publisher_id.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getVenuesByPublisherId` produced zero results for the Publisher Id "$PUBLISHER_ID"'
            print_info = Template(string_to_template).substitute(PUBLISHER_ID = VENUES_BY_PUBLISHER_ID_PUBLISHER_ID)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_venues_by_publisher_id)

        # For each venue in the list:
        for venue in res_venues_by_publisher_id:
            counter += 1

            venue_id   = venue.getIds()
            venue_name = venue.getTitle()

            result_string = '\nId: $ID.\nName: $NAME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        ID   = venue_id,
                                                        NAME = venue_name
                                                    )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_venues_by_publisher_id):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/venues_by_publisher_id.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Venues by Publisher id" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/venues_by_publisher_id.txt'' ')


#
# Publication in Venue.
#
def do_publication_in_venue(_generic_query_processor: QueryProcessor) -> None:

    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_publication_in_venue = _generic_query_processor.getPublicationInVenue(PUBLICATION_IN_VENUE_VENUE_ID)

    # Build the template string.
    header_publication_in_venue = 'Publications by Venue id = $VENUE_ID:\n\n'
    header_publication_in_venue = Template(header_publication_in_venue).substitute(VENUE_ID = PUBLICATION_IN_VENUE_VENUE_ID)

    if len(res_publication_in_venue) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Publications found for the Venue Id "VENUE_ID"'
        print_info = Template(string_to_template).substitute(VENUE_ID = PUBLICATION_IN_VENUE_VENUE_ID)
        print(print_info)

        with open('./queries-results/publication_in_venue.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getPublicationInVenue` produced zero results for the Venue Id "VENUE_ID"'
            print_info = Template(string_to_template).substitute(VENUE_ID = PUBLICATION_IN_VENUE_VENUE_ID)
            f.write(print_info)
    else:
        # Append the header title to the list.
        result_string_list.append(header_publication_in_venue)

        # For each publication in the list:
        for publication in res_publication_in_venue:
            counter += 1

            publication_id   = publication.getIds()
            publication_name = publication.getTitle()

            result_string = '\nId: $ID.\nName: $NAME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        ID   = publication_id,
                                                        NAME = publication_name
                                                    )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_publication_in_venue):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/publication_in_venue.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Publication in Venue" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/publication_in_venue.txt'' ')


#
# Journal articles in Issue.
#
def do_journal_articles_in_issue(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_journal_articles_in_issue: list[JournalArticle] = _generic_query_processor.getJournalArticlesInIssue(
                                                                                        JOURNAL_ARTICLES_IN_ISSUE_ISSUE, 
                                                                                        JOURNAL_ARTICLES_IN_ISSUE_VOLUME, 
                                                                                        JOURNAL_ARTICLES_IN_ISSUE_VENUE_ID
                                                                                   )

    # Build the template string.
    header_journal_articles_in_issue = 'Journal articles in Issue = $ISSUE, Volume = $VOLUME, Venue id = $VENUE_ID :\n\n'
    header_journal_articles_in_issue = Template(header_journal_articles_in_issue).substitute( 
                                                                                        ISSUE    = JOURNAL_ARTICLES_IN_ISSUE_ISSUE,
                                                                                        VOLUME   = JOURNAL_ARTICLES_IN_ISSUE_VOLUME,
                                                                                        VENUE_ID = JOURNAL_ARTICLES_IN_ISSUE_VENUE_ID
                                                                                  )

    #
    # Opens file and casts as f then, for item in the list write the file.
    #
    with open('./queries-results/journal_articles_in_issue.txt', 'w', encoding='utf-8') as f: 
        f.write(header_journal_articles_in_issue)

    if len(res_journal_articles_in_issue) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Journal Articles found for the Issue "$ISSUE", Volume "$VOLUME" and Venue Id "$VENUE_ID"'
        print_info = Template(string_to_template).substitute(
                                                    ISSUE    = JOURNAL_ARTICLES_IN_ISSUE_ISSUE,
                                                    VOLUME   = JOURNAL_ARTICLES_IN_ISSUE_VOLUME,
                                                    VENUE_ID = JOURNAL_ARTICLES_IN_ISSUE_VENUE_ID
                                                  )
        print(print_info)

        with open('./queries-results/journal_articles_in_issue.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getJournalArticlesInIssue` produced zero results for the Issue "$ISSUE", Volume "$VOLUME" and Venue Id "$VENUE_ID"'
            print_info = Template(string_to_template).substitute(
                                                        ISSUE    = JOURNAL_ARTICLES_IN_ISSUE_ISSUE,
                                                        VOLUME   = JOURNAL_ARTICLES_IN_ISSUE_VOLUME,
                                                        VENUE_ID = JOURNAL_ARTICLES_IN_ISSUE_VENUE_ID
                                                      )
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_journal_articles_in_issue)

        # For each journal_article in the list:
        for journal_article in res_journal_articles_in_issue:
            counter += 1

            journal_article_id     = journal_article.getIds()
            journal_article_name   = journal_article.getTitle()
            journal_article_issue  = journal_article.getIssue()
            journal_article_volume = journal_article.getVolume()

            result_string = '\nId: $ID.\nName: $NAME.\nIssue: $ISSUE.\nVolume: $VOLUME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        ID     = journal_article_id,
                                                        NAME   = journal_article_name,
                                                        ISSUE  = journal_article_issue,
                                                        VOLUME = journal_article_volume
                                                    )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_journal_articles_in_issue):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/journal_articles_in_issue.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Journal articles in Issue" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/journal_articles_in_issue.txt'' ')



#
# Journal articles in Volume.
#
def do_journal_articles_in_volume(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_journal_articles_in_volume: list[JournalArticle] = _generic_query_processor.getJournalArticlesInVolume(JOURNAL_ARTICLES_IN_VOLUME_VOLUME, JOURNAL_ARTICLES_IN_VOLUME_VENUE_ID)

    # Build the template string.
    header_journal_articles_in_volume = 'Journal articles in Volume = $VOLUME, Venue id = $VENUE_ID :\n\n'
    header_journal_articles_in_volume = Template(header_journal_articles_in_volume).substitute( 
                                                                                        VOLUME   = JOURNAL_ARTICLES_IN_VOLUME_VOLUME,
                                                                                        VENUE_ID = JOURNAL_ARTICLES_IN_VOLUME_VENUE_ID
                                                                                    )

    if len(res_journal_articles_in_volume) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Jounal Articles found for the Volume "$VOLUME" and the Venue Id "$VENUE_ID"'
        print_info = Template(string_to_template).substitute( 
                                                    VOLUME   = JOURNAL_ARTICLES_IN_VOLUME_VOLUME,
                                                    VENUE_ID = JOURNAL_ARTICLES_IN_VOLUME_VENUE_ID
                                                  )
        print(print_info)

        with open('./queries-results/journal_articles_in_volume.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getJournalArticlesInVolume` produced zero results for the Volume "$VOLUME" and the Venue Id "$VENUE_ID"'
            print_info = Template(string_to_template).substitute( 
                                                        VOLUME   = JOURNAL_ARTICLES_IN_VOLUME_VOLUME,
                                                        VENUE_ID = JOURNAL_ARTICLES_IN_VOLUME_VENUE_ID
                                                      )
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_journal_articles_in_volume)

        # For each journal_article in the list:
        for journal_article in res_journal_articles_in_volume:
            counter += 1

            journal_article_id     = journal_article.getIds()
            journal_article_name   = journal_article.getTitle()
            journal_article_issue  = journal_article.getIssue()
            journal_article_volume = journal_article.getVolume()

            result_string = '\nId: $ID.\nName: $NAME.\nIssue: $ISSUE.\nVolume: $VOLUME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        ID     = journal_article_id,
                                                        NAME   = journal_article_name,
                                                        ISSUE  = journal_article_issue,
                                                        VOLUME = journal_article_volume
                                                    )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_journal_articles_in_volume):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/journal_articles_in_volume.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: ''Journal articles in Volume'' Query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/journal_articles_in_volume.txt'' ')



#
# Journal articles in Journal.
#
def do_journal_articles_in_journal(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_journal_articles_in_journal: list[JournalArticle] = _generic_query_processor.getJournalArticlesInJournal(JOURNAL_ARTICLES_IN_JOURNAL_VENUE_ID)

    # Build the template string.
    header_journal_articles_in_journal = 'Journal articles in Journal Id = $VENUE_ID :\n\n'
    header_journal_articles_in_journal = Template(header_journal_articles_in_journal).substitute(VENUE_ID = JOURNAL_ARTICLES_IN_JOURNAL_VENUE_ID)

    if len(res_journal_articles_in_journal) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Journal Articles found for the Venue Id "$VENUE_ID"'
        print_info = Template(string_to_template).substitute(VENUE_ID = JOURNAL_ARTICLES_IN_JOURNAL_VENUE_ID)
        print(print_info)

        with open('./queries-results/journal_articles_in_journal.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getJournalArticlesInJournal` produced zero results for the Venue Id "$VENUE_ID"'
            print_info = Template(string_to_template).substitute(VENUE_ID = JOURNAL_ARTICLES_IN_JOURNAL_VENUE_ID)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_journal_articles_in_journal)

        # For each journal_article in the list:
        for journal_article in res_journal_articles_in_journal:
            counter += 1

            journal_article_id     = journal_article.getIds()
            journal_article_name   = journal_article.getTitle()
            journal_article_issue  = journal_article.getIssue()
            journal_article_volume = journal_article.getVolume()

            result_string = '\nId: $ID.\nName: $NAME.\nIssue: $ISSUE.\nVolume: $VOLUME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        ID     = journal_article_id,
                                                        NAME   = journal_article_name,
                                                        ISSUE  = journal_article_issue,
                                                        VOLUME = journal_article_volume
                                                    )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_journal_articles_in_journal):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/journal_articles_in_journal.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Journal Articles in Journal" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/journal_articles_in_journal.txt'' ')


#
# Proceedings Events.
#
def do_proceedings_by_event(_generic_query_processor: QueryProcessor) -> None:

    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_proceedings_by_event: list[Proceedings] = _generic_query_processor.getProceedingsByEvent(EVENT_PARTIAL_NAME)

    # Build the template string.
    string_to_template = 'Proceedings by event partial name: $EVENT :\n\n'
    header_proceedings_by_event = Template(string_to_template).substitute(EVENT = EVENT_PARTIAL_NAME)

    #
    # Opens file and casts as f then, write the file.
    #
    with open('./queries-results/proceedings_by_event.txt', 'w', encoding='utf-8') as f: 
        f.write(header_proceedings_by_event)

    if len(res_proceedings_by_event) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Proceedings found for the Event "$EVENT"'
        print_info = Template(string_to_template).substitute(EVENT = EVENT_PARTIAL_NAME)
        print(print_info)

        with open('./queries-results/proceedings_by_event.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getProceedingsByEvent` produced zero results for the Event "$EVENT"'
            print_info = Template(string_to_template).substitute(EVENT = EVENT_PARTIAL_NAME)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_proceedings_by_event)

        # For each proceedings in the list:
        for proceedings in res_proceedings_by_event:
            counter += 1

            proceedings_id    = proceedings.getIds()
            proceedings_name  = proceedings.getTitle()
            proceedings_event = proceedings.getEvent()

            result_string = '\nId: $ID.\nName: $NAME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                ID    = proceedings_id,
                                NAME  = proceedings_name,
                                EVENT = proceedings_event
                            )

            # Append the result_string to the result list.
            result_string_list.append(result_string)

            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_proceedings_by_event):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/proceedings_by_event.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)
    #
    # File closed automatically.
    #
    print('-- INFO: "Proceedings by event" query has been processed. ', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/proceedings_by_event.txt'' ')



#
# Publication Authors.
#
def do_publication_authors(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_publication_authors: list[Person] = _generic_query_processor.getPublicationAuthors(PUBLICATION_AUTHORS_PUBLICATION_ID)

    # Build the template string.
    header_publication_authors = 'Authors of Publications with doi: $PUBLICATION_ID:\n\n'
    # Pupulate the template string parameter.
    header_publication_authors = Template(header_publication_authors).substitute(PUBLICATION_ID = PUBLICATION_AUTHORS_PUBLICATION_ID)

    if len(res_publication_authors) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Authors found for the year "$PUBLICATION_ID"'
        print_info = Template(string_to_template).substitute(PUBLICATION_ID = PUBLICATION_AUTHORS_PUBLICATION_ID)
        print(print_info)

        with open('./queries-results/publication_authors.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getPublicationAuthors` produced zero results for the Publication Id "$PUBLICATION_ID"'
            print_info = Template(string_to_template).substitute(PUBLICATION_ID = PUBLICATION_AUTHORS_PUBLICATION_ID)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_publication_authors)
        
        # For each author in the list:
        for author in res_publication_authors:
            counter += 1
            
            author_id          = author.getIds()
            author_name        = author.getGivenName()
            author_family_name = author.getFamilyName()
            
            # Build the template string.
            result_string = '\nId: $ID.\nName: $NAME.\nFamily name: $FAMILY_NAME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        ID          = author_id,
                                                        NAME        = author_name,
                                                        FAMILY_NAME = author_family_name
                                                    )
            
            # Append the result_string to the result list.
            result_string_list.append(result_string)
            
            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_publication_authors):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/publication_authors.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)
    #
    # File closed automatically.
    #

    print('-- INFO: "Publication Authors" query has been processed. ', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/publication_authors.txt'' ')


#
# Publications by Authors name.
#
def do_publications_by_author_name(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_publications_by_author_name: list[Publication] = _generic_query_processor.getPublicationsByAuthorName(PUBLICATIONS_BY_AUTHOR_NAME_NAME)

    # Build the template string.
    header_publications_by_author_name = 'Publications created by Author(s) with name: $AUTHOR_NAME:\n\n'
    # Pupulate the template string parameter.
    header_publications_by_author_name = Template(header_publications_by_author_name).substitute(AUTHOR_NAME = PUBLICATIONS_BY_AUTHOR_NAME_NAME)

    if len(res_publications_by_author_name) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Publications found for the Author(s) with name "$AUTHOR_NAME"'
        print_info = Template(string_to_template).substitute(AUTHOR_NAME = PUBLICATIONS_BY_AUTHOR_NAME_NAME)
        print(print_info)

        with open('./queries-results/publications_by_author_name.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getPublicationsByAuthorName` produced zero results for the Author(s) with nam "$AUTHOR_NAME"'
            print_info = Template(string_to_template).substitute(AUTHOR_NAME = PUBLICATIONS_BY_AUTHOR_NAME_NAME)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_publications_by_author_name)
    
        # For each author in the list:
        for publication in res_publications_by_author_name:
            counter += 1
            
            publication_doi   = publication.getIds()
            publication_title = publication.getTitle()
            publication_year  = publication.getPublicationYear()
            
            # Build the template string.
            result_string = '\nDoi: $DOI.\nTitle: $TITLE.\nPublication Year: $YEAR.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(
                                                        DOI   = publication_doi,
                                                        TITLE = publication_title,
                                                        YEAR  = publication_year
                                                    )
            
            # Append the result_string to the result list.
            result_string_list.append(result_string)
            
            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_publications_by_author_name):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/publications_by_author_name.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Publications by Authors name" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/publications_by_author_name.txt'' ')



#
# Distinct Publisher by Authors name.
#
def do_distinct_publisher_of_publications(_generic_query_processor: QueryProcessor) -> None:
    
    # Define a counter to know how many item are retrieved.
    counter = 0

    # Define a list to collect items to print in the file.
    result_string_list = []

    #
    # Do the query.
    #
    res_distinct_publisher_of_publications: list[Organization] = _generic_query_processor.getDistinctPublisherOfPublications(DISTINCT_PUBLISHER_OF_PUBLICATIONS_PUBLICATION_ID_LIST)
    
    # Build the template string.
    header_distinct_publisher_of_publications = 'Publisher of Publication(s) with doi: $PUBLICATIONS_ID_LIST:\n\n'

    publications_ids: str = '' # Init the result string.
    counter: int = 0   # Init the counter to know where i am iterating the list.

    # For each string in the list:
    for item in DISTINCT_PUBLISHER_OF_PUBLICATIONS_PUBLICATION_ID_LIST:
        
        # Incr counter.
        counter += 1
        
        # Remove "'" apex from the string.
        item.replace("'", "")

        if counter < len(DISTINCT_PUBLISHER_OF_PUBLICATIONS_PUBLICATION_ID_LIST):

            publications_ids = item + ', '

        elif counter == len(DISTINCT_PUBLISHER_OF_PUBLICATIONS_PUBLICATION_ID_LIST):

            publications_ids = publications_ids + item
    
    # Pupulate the template string parameter.
    header_distinct_publisher_of_publications = Template(header_distinct_publisher_of_publications).substitute(PUBLICATIONS_ID_LIST = publications_ids)

    if len(res_distinct_publisher_of_publications) <= 0:
        # Build the template string.
        string_to_template = '-- WARN: No Publishers found for the Publications Ids "$PUBLICATIONS_ID_LIST"'
        print_info = Template(string_to_template).substitute(PUBLICATIONS_ID_LIST = publications_ids)
        print(print_info)

        with open('./queries-results/distinct_publisher_of_publications.txt', 'w', encoding='utf-8') as f:
            # Build the template string.
            string_to_template = 'The query `getDistinctPublisherOfPublications` produced zero results for the Publications Ids "$PUBLICATIONS_ID_LIST"'
            print_info = Template(string_to_template).substitute(PUBLICATIONS_ID_LIST = publications_ids)
            f.write(print_info)

    else:
        # Append the header title to the list.
        result_string_list.append(header_distinct_publisher_of_publications)
    
        # For each publisher in the list:
        for publisher in res_distinct_publisher_of_publications:
            counter += 1
            
            organization_id   = publisher.getIds()
            organization_name = publisher.getName()
            
            # Build the template string.
            result_string = '\nId: $ID.\nName: $NAME.\n'
            # Pupulate the template string parameters.
            result_string = Template(result_string).substitute(ID = organization_id, NAME = organization_name)
            
            # Append the result_string to the result list.
            result_string_list.append(result_string)
            
            # If I'm done to iterate over the list then print the number of items found.
            if counter == len(res_distinct_publisher_of_publications):

                items_found = '\nItems found: $COUNT'
                items_found = Template(items_found).substitute(COUNT = counter)
                
                result_string_list.append(items_found)

        #
        # Opens file and casts as f then, for item in the list write the file.
        #
        with open('./queries-results/distinct_publisher_of_publications.txt', 'w', encoding='utf-8') as f: 
            for string in result_string_list:
                f.write(string)

    #
    # File closed automatically.
    #

    print('-- INFO: "Distinct Publisher by Authors name" query has been processed.', EMOJI_STARS, EMOJI_STARS, EMOJI_STARS)
    print('-- INFO: The result has been write in: ''./queries-results/distinct_publisher_of_publications.txt'' ')