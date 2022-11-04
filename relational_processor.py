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

    def uploadData(self,path):

        if path.split(".")[1] == 'csv':
            with connect(self.getDbPath()) as con:
                con.commit()

            #reading csv
            cursor = con.cursor()
            publications = read_csv(path,
                                    keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "type": "string",
                                        "publication_year": "int",
                                        "issue":"string",
                                        "volume":"string",
                                        "chapter":"string",
                                        "publication_venue": "string",
                                        "venue_type":"string",
                                        "publisher":"string",
                                        "event": "string"
                                    })

            ##getting existing tables one by one and if they dont exist then we create empty ones
            VenueID= pd.DataFrame({'VenueID': pd.Series(dtype='str'),
                                   'id': pd.Series(dtype='str')
                                   })
            with connect(self.getDbPath()) as con:

                create_VenueID = '''
                                  DROP TABLE IF EXISTS VenueID;
                                  CREATE TABLE IF NOT EXISTS VenueID
                                  (VenueID, id);
                                '''

                cursor.executescript(create_VenueID)
                VenueID.to_sql('VenueID',con, index=False, if_exists='replace')

            PersonID= pd.DataFrame({'person_internal_id': pd.Series(dtype='str'),
                                    'id': pd.Series(dtype='str')
                                    })
            with connect(self.getDbPath()) as con:

                create_PersonID = '''
                                  DROP TABLE IF EXISTS PersonID;
                                  CREATE TABLE IF NOT EXISTS PersonID
                                  (person_internal_id, id);
                                '''

                cursor.executescript(create_PersonID)
                PersonID.to_sql('PersonID',con, index=False, if_exists='replace')

            PersonTable= pd.DataFrame({'internalID': pd.Series(dtype='str'),
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
                PersonTable.to_sql('Person',con, index=False, if_exists='replace')


            Authors= pd.DataFrame({'AuthorID': pd.Series(dtype='str'),
                                   'person_internal_id': pd.Series(dtype='str')
                                   })
            with connect(self.getDbPath()) as con:

                create_Authors = '''
                                  DROP TABLE IF EXISTS Author;
                                  CREATE TABLE IF NOT EXISTS Author
                                  (AuthorID, person_internal_id);
                                '''

                cursor.executescript(create_Authors)
                Authors.to_sql('Author',con, index=False, if_exists='replace')

            ##PUBBLICATIONSID
            publication_ids = publications[["id"]]

            # Create a new column with internal identifiers for each publication
            publication_internal_id = []
            pub_count=0
            for idx, row in publications.iterrows():
                publication_internal_id.append("publication-" + str(idx+pub_count))

            publication_ids.insert(0, "PublicationID", Series(publication_internal_id, dtype="string"))
            with connect(self.getDbPath()) as con:

                create_publication_ids = '''
                                  DROP TABLE IF EXISTS PubID;
                                  CREATE TABLE IF NOT EXISTS PubID
                                  (PublicationID, id);
                                '''

                cursor.executescript(create_publication_ids)
                publication_ids.to_sql('PubID',con, index=False, if_exists='replace')



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
                OrganizationTable.to_sql('Organization',con, index=False, if_exists='replace')


            OrgId_table = pd.DataFrame({'OrgID': pd.Series(dtype='str'),
                                        'id': pd.Series(dtype='str')
                                        })
            with connect(self.getDbPath()) as con:

                create_OrgId_table = '''
                                  DROP TABLE IF EXISTS OrgID;
                                  CREATE TABLE IF NOT EXISTS OrgID
                                  (OrgID, id);
                                '''

                cursor.executescript(create_OrgId_table)
                OrgId_table.to_sql('OrgID',con, index=False, if_exists='replace')


            CitesTable = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                       'PublicationID': pd.Series(dtype='str')
                                       })
            with connect(self.getDbPath()) as con:

                create_CitesTable = '''
                                  DROP TABLE IF EXISTS Cites;
                                  CREATE TABLE IF NOT EXISTS Cites
                                  (internalID, PublicationID);
                                '''

                cursor.executescript(create_CitesTable)
                CitesTable.to_sql('Cites',con, index=False, if_exists='replace')

            #For each of the venues, we get count of them if they already exist and then add on to them. We also link them to
            # organizations if they exist. If they organization does not exist we leave the publisherid so that it can be linked
            #with organizations the next time they want to be joined.
            ##JOURNALTABLE

            journals = publications.query("venue_type == 'journal'")
            journals=journals.reset_index()

            journals_df = journals[["publication_venue", "id", "publisher"]]

            ven_ids_count =0
            lst1=[]
            lst2=[]
            for idx,row in journals.iterrows():
                lst1.append("venue-" + str(idx+ven_ids_count))
                lst2.append(row["id"])
                last_venue=idx+ven_ids_count

            ven_ids_journal = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })

            journal1 = merge(journals_df, ven_ids_journal, left_on="id", right_on="doi",how="left")
            journalfinal = merge(journal1, OrgId_table, left_on="publisher", right_on="id", how="left")


            if len(OrgId_table)==0:
                JournalTable = journalfinal[["VenueID", "publication_venue", "publisher"]]
                JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
                JournalTable = JournalTable.rename(columns={"publisher": "Publisher"})
                JournalTable = JournalTable.rename(columns={"publication_venue": "title"})
            else:
                JournalTable = journalfinal[["VenueID", "publication_venue", "OrgID"]]
                JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
                JournalTable = JournalTable.rename(columns={"OrgID": "Publisher"})
                JournalTable = JournalTable.rename(columns={"publication_venue": "title"})

            #JournalTable=pd.DataFrame(JournalTable)
            with connect(self.getDbPath()) as con:

                create_journaltable = '''
                                  DROP TABLE IF EXISTS Journal;
                                  CREATE TABLE IF NOT EXISTS Journal
                                  (VenueID, publication_venue, OrgID);
                                '''

                cursor.executescript(create_journaltable)
                JournalTable.to_sql('Journal',con, index=False, if_exists='replace')

        ##Book Table
            books = publications.query("venue_type == 'book'")
            books = books.reset_index()

            books_df =  books[["publication_venue", "id", "publisher" ]]

            ven_ids_count2 = 0
            lst1 = []
            lst2 = []
            for idx, row in books.iterrows():
                lst1.append("venue-" + str(idx+ ven_ids_count2+last_venue))
                lst2.append(row["id"])
                last_last_venue=idx+ ven_ids_count2+last_venue+1


            ven_ids_book = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })
            book1 = merge(books_df, ven_ids_book, left_on="id", right_on="doi", how="left")
            bookfinal = merge(book1, OrgId_table, left_on="publisher", right_on="id", how="left")
            if len(OrgId_table) == 0:

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
                BookTable.to_sql('Book',con, index=False, if_exists='replace')


            ##Proceeding Table

            proceedings = publications.query("venue_type == 'proceedings'")

            proceedings_df =  proceedings[["publication_venue", "id", "publisher", "event" ]]



            ven_ids_count3 = 0
            lst1 = []
            lst2 = []
            for idx, row in proceedings.iterrows():
                lst1.append("venue-" + str(idx + ven_ids_count2 + last_last_venue ))
                lst2.append(row["id"])

            ven_ids_pp = DataFrame({
                "VenueID": Series(lst1, dtype="string", name="VenueID"),
                "doi": Series(lst2, dtype="string", name="doi")
            })


            proceeding1 = merge(proceedings_df, ven_ids_pp, left_on="id", right_on="doi",how="left")
            proceedingfinal = merge(proceeding1, OrgId_table, left_on="publisher", right_on="id", how="left")


            if len(OrgId_table) == 0:
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
                ProceedingTable.to_sql('Proceeding',con, index=False, if_exists='replace')



            #making Authors Table. We check all the Authors if they exist already. If they are not linked with a publication then
            #Author id will be a doi. Otherwise it will be a author internal id (which we create in this step when we merge with
            #publication data). We combine both merged and unmerged authorsid df data and save it to database


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

            Auth_f = newAuth[["AuthorsID","person_internal_id"]]
            Auth_f=Auth_f.rename(columns={"AuthorsID":"AuthorID"})
            #combining both merged and unmerged authors
            Authors = pd.concat([Auth_f, nan_Authors])
            Authors = Authors[Authors['AuthorID'].notna()]

            #For each of the publications, it joins with authors information, if they exist and also joins with the venues
            #that we just created above
            ##JOURNALARCTICLETABLE
            journal_articles = publications.query("type == 'journal-article'")

            pub_joined = merge(publication_ids, journal_articles, left_on="id", right_on="id")
            # if os.path.exists(self.getDbPath()):
            #     with connect(self.getDbPath()) as con:
            #         query="""
            #         select JournalArticle.cites as internalID, PubID.id as doi from JournalArticle join PubID on
            #         JournalArticle.internalID = PubID.PublicationID
            #         """
            #         j = read_sql(query, con)
            # else:
            j = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                              'doi': pd.Series(dtype='str')
                              })

            cit_joined = merge(pub_joined, j, left_on="id", right_on="doi",how="left")
            cit_joined = cit_joined[
                ["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "id"]]
            cit_joined = cit_joined.drop_duplicates()

            new_join =  merge(cit_joined, newAuth, left_on="id", right_on="doi",how="left")
            new_join = new_join[["PublicationID", "publication_year", "title", "internalID", "issue", "volume", "id", "AuthorsID"]]
            authors_joined = new_join.drop_duplicates()

            v=ven_ids_journal.drop_duplicates()
            v = v.rename(columns={
                "doi": "doi_v"})
            venue_joined = merge(authors_joined, v, left_on="id", right_on="doi_v",how="left")
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
                JournalArticleTable.to_sql('JournalArticle',con, index=False, if_exists='replace')


            ###BOOKCHAPTERTABLE

            book_chapters = publications.query("type == 'book-chapter'")
            pub_joined_bc = merge(publication_ids, book_chapters, left_on="id", right_on="id")



            j_1 = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                'doi': pd.Series(dtype='str')
                                })

            cit_joined_bc= merge(pub_joined_bc, j_1, left_on="id", right_on="doi", how="left")

            cit_joined_bc = cit_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "chapter", "id"]]

            cit_joined_bc = cit_joined_bc.drop_duplicates()


            new_join_bc = merge(cit_joined_bc, newAuth, left_on="id", right_on="doi", how="left")
            new_join_bc = new_join_bc[
                ["PublicationID", "publication_year", "title", "internalID", "chapter", "id", "AuthorsID"]]
            authors_joined_bc = new_join_bc.drop_duplicates()

            v_1 = ven_ids_book.drop_duplicates()
            v_1 = v_1.rename(columns={
                "doi": "doi_v"})

            venue_joined_bc = merge(authors_joined_bc, v_1, left_on="id", right_on="doi_v", how="left")
            venue_joined_bc = venue_joined_bc.drop_duplicates()
            venue_joined_bc = venue_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID","chapter"]]


            BookChapterTable = venue_joined_bc.rename(columns={
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
                BookChapterTable.to_sql('BookChapter',con, index=False, if_exists='replace')

            ###PROCEEDINGSPAPERTABLE

            proceeding_papers = publications.query("type == 'proceedings-paper'")
            pub_joined_pp = merge(publication_ids, proceeding_papers, left_on="id", right_on="id")


            j_2 = pd.DataFrame({'internalID': pd.Series(dtype='str'),
                                'doi': pd.Series(dtype='str')
                                })

            cit_joined_pp = merge(pub_joined_pp, j_2, left_on="id", right_on="doi", how="left")

            cit_joined_pp = cit_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "id"]]

            cit_joined_pp = cit_joined_pp.drop_duplicates()

            new_join_pp = merge(cit_joined_pp, newAuth, left_on="id", right_on="doi", how="left")
            new_join_pp = new_join_pp[
                ["PublicationID", "publication_year", "title", "internalID",  "id", "AuthorsID"]]
            authors_joined_pp = new_join_pp.drop_duplicates()
            v_2 = ven_ids_pp.drop_duplicates()
            v_2 = v_2.rename(columns={
                "doi": "doi_v"})

            venue_joined_pp = merge(authors_joined_pp, v_2, left_on="id", right_on="doi_v", how="left")
            venue_joined_pp = venue_joined_pp.drop_duplicates()
            venue_joined_pp = venue_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorsID", "VenueID"]]

            ProceedingsPapersTable = venue_joined_pp.rename(columns={
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
                ProceedingsPapersTable.to_sql('ProceedingPaper',con, index=False, if_exists='replace')

            ##Check if db exists and insert data using queries
            ##ADDING TO DATABASE
            # with connect(self.getDbPath()) as con:
            #     con.commit()
            #with connect(self.getDbPath()) as con:
                #OrganizationTable.to_sql("Organization", con, if_exists="replace", index=False)
                #OrgId_table.to_sql("OrgID", con, if_exists="replace", index=False)
                #PersonTable.to_sql("Person", con, if_exists="replace", index=False)
                #PersonID.to_sql("PersonID", con, if_exists="replace", index=False)
                #Authors.to_sql("Author", con, if_exists="replace", index=False)
                #CitesTable.to_sql("Cites", con, if_exists="replace", index=False)
                #publication_ids.to_sql("PubID", con, if_exists="replace", index=False)
                #VenueID.to_sql("VenueID", con, if_exists="replace", index=False)
                #BookTable.to_sql("Book", con, if_exists="replace", index=False)
                #BookChapterTable.to_sql("BookChapter", con, if_exists="replace", index=False)
                #JournalTable.to_sql("Journal", con, if_exists="replace", index=False)
                #JournalArticleTable.to_sql("JournalArticle", con, if_exists="replace", index=False)
                #ProceedingTable.to_sql("Proceeding", con, if_exists="replace", index=False)
                #ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="replace", index=False)

        if path.split(".")[1] == 'json':

                #checking all the publications and venues and making a dataframe from them. If they dont exist we just create an empty df
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

                ## Making venue
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


                    l1=[]
                    l2=[]
                    for idx, row in venues_df.iterrows():

                        if ven_dict.get(row["doi"],None) !=None:

                            l1.append(row["doi"])
                            l2.append(ven_dict.get(row["doi"])[0])

                            venue_internal_id.append(ven_dict.get(row["doi"])[0])

                    venueID_df = DataFrame({
                        "doiordinati": Series(l1, dtype="string", name="doiordinati"),
                        "VenueID": Series(l2, dtype="string", name="VenueID")
                    })

                    venueID_df= venueID_df.drop_duplicates()
                    doi_merge = merge(venueID_df, venues_df, left_on="doiordinati", right_on="doi")



                    VenueID = doi_merge[["VenueID", "issn"]]
                    VenueID = VenueID.rename(columns={"issn": "id"})


            #Getting all the authors
            authors = json_doc["authors"]

            lst_item = []
            lst_doi_authors = []

            for key in authors:
                for item in authors[key]:
                    lst_doi_authors.append(key)
                    lst_item.append(item)


            people = []

            for item in lst_item:
                if item not in people:
                    people.append(item)



            familylst = []
            for item in people:
                familylst.append(item.get("family"))



            givenlst = []
            for item in people:
                givenlst.append(item.get("given"))



            orcidlst = []
            for item in people:
                orcidlst.append(item.get("orcid"))



            people = DataFrame({
                "orcid": Series(orcidlst, dtype="string", name="orcid"),
                "given_name": Series(givenlst, dtype="string", name="given"),
                "family_name": Series(familylst, dtype="string", name="family"),
            })


            person_internal_id = []

            #getting count of existing person
            #if os.path.exists(self.getDbPath()):
            with connect(self.getDbPath()) as con:
                query = "SELECT count(*) FROM Person"
                df_sql = read_sql(query, con)
                person_count = df_sql.values.tolist()[0][0] + 1

            # else:
            #     person_count =0

            #making new person
            for idx, row in people.iterrows():
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


            ##AUTHORSTABLE
            lst_doi_single_authors = []
            for key in authors:
                lst_doi_single_authors.append(key)



            ciao = DataFrame({
                "doi_single_authors": Series(lst_doi_single_authors, dtype="string", name="doi_single_authors"),
            })

            authors_internal_id = []
            #getting count of existing authors
            with connect(self.getDbPath()) as con:
                query = "SELECT count(*) FROM Author"
                df_sql = read_sql(query, con)
                author_count = df_sql.values.tolist()[0][0] + 1


            # creating new authors
            for idx, row in ciao.iterrows():
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


            doi_orcid_persontable_merged = merge(doi_authors_orcidlst, PersonTable1, left_on="orcidlst_collection", right_on="orcid")

            union_doi_authors_personid = doi_orcid_persontable_merged[["lst_doi_authors", "person_internal_id"]]

            union_merged_authorid_df = merge(union_doi_authors_personid, authorsID_df, left_on="lst_doi_authors", right_on="doi_single_authors")

            #creating authors table with doi and personID. Will use them later when we link publications

            Authors = union_merged_authorid_df[["lst_doi_authors", "person_internal_id"]]
            Authors = Authors.rename(columns={"lst_doi_authors": "AuthorID"})



            ##PUBBLICATIONSID
            publication_ids = publications[["id"]]


            # Create a new column with internal identifiers for each publication
            publication_internal_id = []

                #getting existing ids
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

            ##ORGANIZATIONTABLE
            organization = json_doc["publishers"]


            j = "name"
            name_list = [val[j] for key, val in organization.items()]


            OrganizationTable = DataFrame({
                "name": Series(name_list, dtype="string", name="name")
            })

            organization_internal_id = []
            #getting count of existing orgs
            with connect(self.getDbPath()) as con:
                query = "SELECT count(*) FROM Organization"
                df_sql = read_sql(query, con)
                org_count = df_sql.values.tolist()[0][0] + 1

            #creating new orgs
            for idx, row in OrganizationTable.iterrows():
                organization_internal_id.append("Org-" + str(idx + org_count))


            OrganizationTable.insert(0, "OrgID", Series(organization_internal_id, dtype="string"))
            OrganizationTable = OrganizationTable.rename(columns={"OrgID": "internalID"})


            ##ORGID
            crossref_list = []

            for k in organization:
                crossref_list.append(k)


            orgId_df = DataFrame({
                "id": Series(crossref_list, dtype="string", name="id"),
                "name": Series(name_list, dtype="string", name="name")
            })



            orgId_joined = merge(OrganizationTable, orgId_df, left_on="name", right_on="name")


            OrgId_table = orgId_joined[
                ["internalID", "id"]]
            OrgId_table = OrgId_table.rename(columns={"internalID": "OrgID"})


            citations = json_doc["references"]


            lst_doi = []
            lst_cit = []
            cit_id = []
            count = 0
            #getting cites information. We will use this later to link Publications
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


            CitesTable = cite_df[["internalID", "PublicationID"]]

            # Merging with organization and venueID table and creating venues if they exist

            ##JOURNALTABLE
            journals = publications.query("venue_type == 'journal'")

            journals_df = journals[["publication_venue", "id", "publisher"]]

            VenueID_journal = doi_merge[["VenueID", "doi"]]
            VenueID_journal = VenueID_journal.drop_duplicates()


            with connect(self.getDbPath()) as con:
                query = """SELECT PubID.id, Journal.publisher as Publisher FROM Journal join JournalArticle on JournalArticle.PublicationVenue  = Journal.internalID
                        join PubID on JournalArticle.internalID = PubID.PublicationID
                        """
                publisher = read_sql(query, con)



            OrgId_table_j = merge(publisher,OrgId_table, left_on="Publisher", right_on="id")
            journal1 = merge(journals_df, VenueID_journal, left_on="id", right_on="doi")
            journalfinal = merge(journal1, OrgId_table_j, left_on="id", right_on="id_x")
            JournalTable = journalfinal[["VenueID", "publication_venue", "OrgID"]]
            JournalTable = JournalTable.rename(columns={"VenueID": "internalID"})
            JournalTable = JournalTable.rename(columns={"OrgID": "Publisher"})
            JournalTable = JournalTable.rename(columns={"publication_venue": "title"})


            ##BOOKTABLE
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


            OrgId_table_b = merge(publisher, OrgId_table, left_on="Publisher", right_on="id")
            book1 = merge(books_df, VenueID_book, left_on="id", right_on="doi")
            journal1 = merge(journals_df, VenueID_journal, left_on="id", right_on="doi")


            journalfinal = merge(journal1, OrgId_table_j, left_on="id", right_on="id_x")
            bookfinal = merge(book1, OrgId_table_b, left_on="id", right_on="id_x")


            BookTable = bookfinal[["VenueID", "publication_venue", "OrgID"]]
            BookTable = BookTable.rename(columns={"VenueID": "internalID"})
            BookTable = BookTable.rename(columns={"OrgID": "Publisher"})
            BookTable = BookTable.rename(columns={"publication_venue": "title"})



            ##PROCEEDINGTABLE
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


            OrgId_table_p = merge(publisher, OrgId_table, left_on="Publisher", right_on="id")

            proceeding1 = merge(proceedings_df, VenueID_proceedings, left_on="id", right_on="doi")


            proceedingfinal = merge(proceeding1, OrgId_table_p, left_on="id", right_on="id_x")


            ProceedingTable = proceedingfinal[["VenueID", "publication_venue", "OrgID", "event"]]
            ProceedingTable = ProceedingTable.rename(columns={"VenueID": "internalID"})
            ProceedingTable = ProceedingTable.rename(columns={"OrgID": "Publisher"})
            ProceedingTable = ProceedingTable.rename(columns={"publication_venue": "title"})


            #AuthorID joining with all the  publications if they exist.
            #For all the person that joins with publication we create authorID. For the ones that are not able to join
            #we leave them with just dois so they are able to join with other publications
            joined_auth = merge(publications, unione, left_on="id", right_on="doi_single_authors", how="left")
            joined_auth = joined_auth[["doi_single_authors", "AuthorID"]]
            joined_auth = joined_auth.drop_duplicates()
            joined_auth = joined_auth[joined_auth['doi_single_authors'].notna()]

            dics = joined_auth.set_index('doi_single_authors').T.to_dict('list')

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

            #For each of the publications, we merge them with citations, authors and venues to get the final tables

            ##JOURNALARCTICLETABLE
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


            # BOOKCHAPTERTABLE
            book_chapters = publications.query("type == 'book-chapter'")

            pub_joined_bc = merge(publication_ids, book_chapters, left_on="id", right_on="id")

            cit_joined_bc = merge(pub_joined_bc, cite1_df, left_on="id", right_on="doi")
            cit_joined_bc = cit_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "chapter"]]

            cit_joined_bc = cit_joined_bc.drop_duplicates()

            authors_joined_bc = merge(cit_joined_bc, unione, left_on="doi", right_on="doi_single_authors")
            authors_joined_bc = authors_joined_bc.drop_duplicates()
            authors_joined_bc = authors_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "AuthorID", "chapter"]]


            venue_joined_bc = merge(authors_joined_bc, venueID_df, left_on="doi", right_on="doiordinati")
            venue_joined_bc = venue_joined_bc.drop_duplicates()
            venue_joined_bc = venue_joined_bc[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID", "chapter"]]


            BookChapterTable = venue_joined_bc.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue",
                "chapter": "chapternumber"})



            ##PROCEEDINGSPAPERTABLE

            proceeding_papers = publications.query("type == 'proceedings-paper'")


            pub_joined_pp = merge(publication_ids, proceeding_papers, left_on="id", right_on="id")

            cit_joined_pp = merge(pub_joined_pp, cite1_df, left_on="id", right_on="doi")
            cit_joined_pp = cit_joined_pp[["PublicationID", "publication_year", "title", "internalID", "doi"]]

            cit_joined_pp = cit_joined_pp.drop_duplicates()

            authors_joined_pp = merge(cit_joined_pp, unione, left_on="doi", right_on="doi_single_authors")
            authors_joined_pp = authors_joined_pp.drop_duplicates()
            authors_joined_pp = authors_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "doi", "AuthorID"]]


            venue_joined_pp = merge(authors_joined_pp, venueID_df, left_on="doi", right_on="doiordinati")
            venue_joined_pp = venue_joined_pp.drop_duplicates()
            venue_joined_pp = venue_joined_pp[
                ["PublicationID", "publication_year", "title", "internalID", "AuthorID", "VenueID"]]


            ProceedingsPapersTable = venue_joined_pp.rename(columns={
                "PublicationID": "internalID",
                "internalID": "cites",
                "AuthorID": "author",
                "VenueID": "PublicationVenue"})



            ##Check if db exists and insert data using queries
            ##ADDING TO DATABASE

            with connect(self.getDbPath()) as con:
                con.commit()
            with connect(self.getDbPath()) as con:
                #If it already exists then we append rest of the data but we replace
                #for publicationsid, all publications and venue tables because we load their data by
                # queries and then add on to that data again.
                OrganizationTable.to_sql("Organization", con, if_exists="append", index=False)
                OrgId_table.to_sql("OrgID", con, if_exists="append", index=False)
                PersonTable.to_sql("Person", con, if_exists="append", index=False)
                PersonID.to_sql("PersonID", con, if_exists="append", index=False)
                Authors.to_sql("Author", con, if_exists="append", index=False)
                CitesTable.to_sql("Cites", con, if_exists="append", index=False)
                publication_ids.to_sql("PubID", con, if_exists="replace", index=False)
                VenueID.to_sql("VenueID", con, if_exists="append", index=False)
                BookTable.to_sql("Book", con, if_exists="replace", index=False)
                BookChapterTable.to_sql("BookChapter", con, if_exists="replace", index=False)
                JournalTable.to_sql("Journal", con, if_exists="replace", index=False)
                JournalArticleTable.to_sql("JournalArticle", con, if_exists="replace", index=False)
                ProceedingTable.to_sql("Proceeding", con, if_exists="replace", index=False)
                ProceedingsPapersTable.to_sql("ProceedingPaper", con, if_exists="replace", index=False)


        return True