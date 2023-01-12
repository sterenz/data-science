################################################################################
#                                                                              #
# Common Constants.                                                            #
#                                                                              #
################################################################################

#
# Data path.
#
#CSV_FILE  = './data/empty.csv' # for testing purpose.
GRAPH_CSV_FILE       = './data/graph_publications.csv'
GRAPH_JSON_FILE      = './data/graph_other_data.json'
#RELATIONAL_CSV_FILE  
#RELATIONAL_JSON_FILE

#
# Root of resources in the graph DB.
#
BASE_URL = 'https://allorapy.github.io/res/'

#
# The relational database path.
#
DB_PATH = './sqlite/relational.db'

#
# The URL of the SPARQL endpoint is the same URL of the Blazegraph.
# instance + '/sparql'.
#
ENDPOINT = 'http://127.0.0.1:9999/blazegraph/sparql'

#
# Emoji.
#
# see => https://home.unicode.org/
#
EMOJI_LED_GREEN = '\U0001F7E2'
EMOJI_LED_RED   = '\U0001F534'
EMOJI_FIRE      = '\U0001f525'
EMOJI_STARS     = '\U00002728'

################################################################################
#                                                                              #
# SPARQL Queries.                                                                     #
#                                                                              #
################################################################################

#
# Test query.
#
TEST_QUERY = """
PREFIX res: <https://allorapy.github.io/res/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT *
WHERE {
    ?s ?p ?o .
}
"""

###################################
#                                 #
# Publications published in year. #
#                                 #
###################################
PUBLICATIONS_PUBLISHED_IN_YEAR_YEAR = 2020

PUBLICATIONS_PUBLISHED_IN_YEAR = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?publication ?publicationId ?publicationYear ?publicationTitle
 WHERE {
   {
     ?publication a fabio:JournalArticle.
   }
   UNION
   {
     ?publication a fabio:BookChapter.
   }
   UNION
   {
     ?publication a fabio:ProceedingsPaper.

   }

   ?publication fabio:hasPublicationYear ?publicationYear;
                dcterms:title ?publicationTitle;
                dcterms:identifier ?publicationId.

 FILTER (?publicationYear = $YEAR)
}
 ORDER BY DESC(?publicationYear)
"""



##############################
#                            #
# Publications by Author id. #
#                            #
##############################
PUBLICATION_BY_AUTHOR_ID_AUTHOR_ID = '0000-0001-9857-1511'

PUBLICATIONS_BY_AUTHOR_ID = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>


SELECT ?publication ?publicationId ?publicationTitle ?publicationYear ?authorId ?authorName ?authorFamilyName
WHERE {
	{
      ?publication a fabio:JournalArticle.
  }
	UNION
	{
      ?publication a fabio:BookChapter.
  }
	UNION
	{
      ?publication a fabio:Proceedingspaper.
  }

   ?person a foaf:Person ;
			       dcterms:identifier ?authorId;
  			     foaf:givenName ?authorName;
  			     foaf:familyName ?authorFamilyName.

   ?publication fabio:hasPublicationYear ?publicationYear;
                dcterms:title ?publicationTitle;
                dcterms:identifier ?publicationId;
                dcterms:creator ?person.

  FILTER (?authorId = '$AUTHOR_ID')
}
"""



###########################
#                         #
# Most cited Publication. #
#                         #
###########################

MOST_CITED_PUBLICATION = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX cito:    <http://purl.org/spar/cito/>
PREFIX dcterms: <http://purl.org/dc/terms/>


SELECT ?publication ?publicationId ?publicationTitle ?publicationYear (COUNT(?cites) As ?noOfCites)
WHERE {
    { ?publication a fabio:JournalArticle. }
  UNION
    { ?publication a fabio:BookChapter. }
  UNION
    { ?publication a fabio:Proceedingspaper. }

  ?publication fabio:hasPublicationYear ?publicationYear;
               dcterms:title ?publicationTitle;
               dcterms:identifier ?publicationId.

  ?cites cito:cites ?publication.
}

GROUP BY ?publication ?publicationId ?publicationTitle ?publicationYear
ORDER BY DESC(?noOfCites)
LIMIT 1
"""


#####################
#                   #
# Most cited Venue. #
#                   #
#####################

MOST_CITED_VENUE = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX cito:    <http://purl.org/spar/cito/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?venue ?venueId ?venueTitle (COUNT(?cites) As ?noOfCites)

WHERE {
  { ?publication a fabio:JournalArticle. }
  UNION
  { ?publication a fabio:BookChapter. }
  UNION
  { ?publication a fabio:Proceedingspaper. }

  ?publication schema:isPartOf ?venue.

  ?venue dcterms:title ?venueTitle;
         dcterms:identifier ?venueId.

  ?cites cito:cites ?publication.
}

GROUP BY ?venue ?venueId ?venueTitle
ORDER BY DESC(?noOfCites)
LIMIT 1
"""



###########################
#                         #
# Venues by Publisher id. #
#                         #
###########################
VENUES_BY_PUBLISHER_ID_PUBLISHER_ID = 'crossref:78'

VENUES_BY_PUBLISHER_ID = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX wd:      <http://wikidata.org/>


SELECT ?venue ?venueId ?venueTitle ?organization ?organizationId ?organizationName

WHERE {
  ?venue dcterms:title ?venueTitle;
         dcterms:identifier ?venueId;
         dcterms:publisher ?organization.

  ?organization a schema:Organization;
                  dcterms:identifier ?organizationId;
                  schema:name ?organizationName.

  FILTER(?organizationId = '$PUBLISHER_ID')
}
"""

#########################
#                       #
# Publication in Venue. #
#                       #
#########################
PUBLICATION_IN_VENUE_VENUE_ID = 'issn:0944-1344'

PUBLICATION_IN_VENUE = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX wd:      <http://wikidata.org/>


SELECT ?venue ?venueId ?venueTitle ?publication ?publicationId ?publicationTitle ?publicationYear

WHERE {
  	{ ?publication a fabio:JournalArticle. }
  UNION
    { ?publication a fabio:BookChapter. }
  UNION
    { ?publication a fabio:Proceedingspaper. }

  ?publication fabio:hasPublicationYear ?publicationYear;
               dcterms:title ?publicationTitle;
               dcterms:identifier ?publicationId;
         	     schema:isPartOf ?venue.

  ?venue dcterms:title ?venueTitle;
         dcterms:identifier ?venueId.

  FILTER(?venueId = '$VENUE_ID')
}
"""



##############################
#                            #
# Journal articles in Issue. #
#                            #
##############################
JOURNAL_ARTICLES_IN_ISSUE_ISSUE    = '9'
JOURNAL_ARTICLES_IN_ISSUE_VOLUME   = '17'
JOURNAL_ARTICLES_IN_ISSUE_VENUE_ID = 'issn:2164-5515'

JOURNAL_ARTICLES_IN_ISSUE = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX prism:   <http://prismstandard.org/namespaces/basic/2.0/>

SELECT
	?JournalArticle
	?JournalArticleId
	?JournalArticleTitle
	?JournalArticlePublicationYear
	?issue
	?volume
	?journal
	?venueId
WHERE {
    ?JournalArticle a fabio:JournalArticle;
                      dcterms:identifier ?JournalArticleId;
                      dcterms:title ?JournalArticleTitle;
                      fabio:hasPublicationYear ?JournalArticlePublicationYear;
                      prism:issueIdentifier ?issue;
                      fabio:JournalVolume ?volume.

   ?journal a fabio:Journal;
              dcterms:identifier ?venueId.

FILTER(?issue = '$ISSUE' && ?volume = '$VOLUME' && ?venueId = '$VENUE_ID')

}
"""



###############################
#                             #
# Journal articles in Volume. #
#                             #
###############################
JOURNAL_ARTICLES_IN_VOLUME_VOLUME   = '17'
JOURNAL_ARTICLES_IN_VOLUME_VENUE_ID = 'issn:2164-5515'

JOURNAL_ARTICLES_IN_VOLUME = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX prism:   <http://prismstandard.org/namespaces/basic/2.0/>

SELECT
	?JournalArticle
	?JournalArticleId
	?JournalArticleTitle
	?JournalArticlePublicationYear
	?issue
	?volume
	?journal
	?venueId
WHERE {
    ?JournalArticle a fabio:JournalArticle;
                      dcterms:identifier ?JournalArticleId;
                      dcterms:title ?JournalArticleTitle;
                      fabio:hasPublicationYear ?JournalArticlePublicationYear;
                      prism:issueIdentifier ?issue;
                      fabio:JournalVolume ?volume.

   ?journal a fabio:Journal;
              dcterms:identifier ?venueId.

FILTER(?volume = '$VOLUME' && ?venueId = '$VENUE_ID')
}
"""



################################
#                              #
# Journal articles in Journal. #
#                              #
################################
JOURNAL_ARTICLES_IN_JOURNAL_VENUE_ID = 'issn:2164-5515'

JOURNAL_ARTICLES_IN_JOURNAL = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX prism:   <http://prismstandard.org/namespaces/basic/2.0/>

SELECT
	?JournalArticle
	?JournalArticleId
	?JournalArticleTitle
	?JournalArticlePublicationYear
	?issue
	?volume
	?journal
	?venueId
WHERE {
    ?JournalArticle a fabio:JournalArticle;
                      dcterms:identifier ?JournalArticleId;
                      dcterms:title ?JournalArticleTitle;
                      fabio:hasPublicationYear ?JournalArticlePublicationYear;
                      prism:issueIdentifier ?issue;
                      fabio:JournalVolume ?volume.

   ?journal a fabio:Journal;
              dcterms:identifier ?venueId.

FILTER(?venueId = '$VENUE_ID')
}
"""

################################
#                              #
# Proceedings by event.        #
#                              #
################################
EVENT_PARTIAL_NAME = "web"

PROCEEDINGS_BY_EVENT = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX schema:  <https://schema.org/>

SELECT ?proceedings ?proceedingsId ?proceedingsTitle ?proceedingsEvent
 WHERE {

   ?proceedings a fabio:ConferenceProceedings;
                dcterms:title ?proceedingsTitle;
                dcterms:identifier ?proceedingsId;
   				      schema:event ?proceedingsEvent.
   
   FILTER(?proceedingsEvent = '$EVENT_PARTIAL_NAME')
}  
"""

################################
#                              #
# Publication Authors.         #
#                              #
################################
PUBLICATION_AUTHORS_PUBLICATION_ID = 'doi:10.1080/21645515.2021.1910000'

PUBLICATION_AUTHORS = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?publication ?publicationId ?publicationYear ?publicationTitle ?author ?authorName ?authorFamilyName ?authorId
 WHERE {
   {
     ?publication a fabio:JournalArticle.
   }
   UNION
   {
     ?publication a fabio:BookChapter.
   }
   UNION
   {
     ?publication a fabio:ProceedingsPaper.

   }

   ?publication fabio:hasPublicationYear ?publicationYear;
                dcterms:title ?publicationTitle;
                dcterms:identifier ?publicationId;
   				      dcterms:creator ?author.

   ?author a foaf:Person; 
             foaf:givenName ?authorName;
             foaf:familyName ?authorFamilyName;
             dcterms:identifier ?authorId.
   
   FILTER(?publicationId = '$PUBLICATION_ID')
}  
"""



################################
#                              #
# Publications by Author name. #
#                              #
################################
PUBLICATIONS_BY_AUTHOR_NAME_NAME = 'paul'

PUBLICATIONS_BY_AUTHOR_NAME = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?publication ?publicationId ?publicationYear ?publicationTitle ?author ?authorName ?authorFamilyName ?authorId
 WHERE {
   {
     ?publication a fabio:JournalArticle.
   }
   UNION
   {
     ?publication a fabio:BookChapter.
   }
   UNION
   {
     ?publication a fabio:ProceedingsPaper.

   }

   ?publication fabio:hasPublicationYear ?publicationYear;
                dcterms:title ?publicationTitle;
                dcterms:identifier ?publicationId;
   				      dcterms:creator ?author.

   ?author a foaf:Person; 
             foaf:givenName ?authorName;
             foaf:familyName ?authorFamilyName;
             dcterms:identifier ?authorId.
   
   FILTER(?authorName = '$AUTHOR_NAME')
}  
"""

#######################################
#                                     #
# Distinct publisher of Publications. #
#                                     #
#######################################
DISTINCT_PUBLISHER_OF_PUBLICATIONS_PUBLICATION_ID_LIST = ['''doi:10.1080/21645515.2021.1910000''', '''doi:10.3390/ijfs9030035''']

DISTINCT_PUBLISHER_OF_PUBLICATIONS = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX wd:      <http://wikidata.org/>


SELECT ?organization ?organizationId ?organizationName

WHERE {
  	{ ?publication a fabio:JournalArticle. }
  UNION
    { ?publication a fabio:BookChapter. }
  UNION
    { ?publication a fabio:Proceedingspaper. }

  ?publication dcterms:identifier ?publicationId;
         	     schema:isPartOf ?venue.

  ?venue dcterms:publisher ?organization.  
          
  ?organization a schema:Organization;
                  dcterms:identifier ?organizationId;
                  schema:name ?organizationName.  
  
  FILTER(?publicationId IN $PUBLICATIONS_LIST) 
}
GROUP BY ?organization ?organizationId ?organizationName
ORDER BY (?publicationId)
"""

################################################################################
#                                                                              #
# Extra SPARQL Queries.                                                               #
#                                                                              #
################################################################################

###################################
#                                 #
# Get cited Publications.         #
#                                 #
###################################

GET_CITED_PUBLICATIONS = """
PREFIX res:     <https://allorapy.github.io/res/>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema:  <https://schema.org/>
PREFIX fabio:   <http://purl.org/spar/fabio/>
PREFIX cito:    <http://purl.org/spar/cito/>
PREFIX dcterms: <http://purl.org/dc/terms/>


SELECT ?publication ?publicationId ?publicationTitle ?publicationYear ?cites
WHERE {
    { ?publication a fabio:JournalArticle. }
  UNION
    { ?publication a fabio:BookChapter. }
  UNION
    { ?publication a fabio:Proceedingspaper. }

  ?publication fabio:hasPublicationYear ?publicationYear;
              dcterms:title ?publicationTitle;
              dcterms:identifier ?publicationId.

  ?cites cito:cites ?publication.
}
"""
