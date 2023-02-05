################################################################################
#                                                                              #
# App Entry Point.                                                             #
#                                                                              #
################################################################################

# # time.
# import time

# # common utilities internal.
# from common_utils import blazegraph_instance_is_active, blazegraph_instance_is_empty, create_folder, start_blazegraph_server

# from query_processors import RelationalQueryProcessor, TriplestoreQueryProcessor
# from relational_processor import RelationalDataProcessor

# from triplestore_processor import TriplestoreDataProcessor

# # generic query processor.
# from query_processors import GenericQueryProcessor

# # const.
# from const import *

# # Queries.
# from queries import *

# # os
# import os

from impl import *

#####################
#                   #
# Program.          #
#                   #
# app(): None       #
#                   #
#####################
def app():

    # Once all the classes are imported, first create the relational
    # database using the related source data
    rel_path = "relational.db"
    rel_dp = RelationalDataProcessor()
    rel_dp.setDbPath(rel_path)
    rel_dp.uploadData("data/relational_publications.csv")
    rel_dp.uploadData("data/relational_other_data.json")

    # Then, create the RDF triplestore (remember first to run the
    # Blazegraph instance) using the related source data
    grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
    grp_dp = TriplestoreDataProcessor()
    grp_dp.setEndpointUrl(grp_endpoint)
    grp_dp.uploadData("data/graph_publications.csv")
    grp_dp.uploadData("data/graph_other_data.json")

    # In the next passage, create the query processors for both
    # the databases, using the related classes
    rel_qp = RelationalQueryProcessor()
    rel_qp.setDbPath(rel_path)

    grp_qp = TriplestoreQueryProcessor()
    grp_qp.setEndpointUrl(grp_endpoint)

    # Finally, create a generic query processor for asking
    # about data
    generic = GenericQueryProcessor()
    generic.addQueryProcessor(rel_qp)
    generic.addQueryProcessor(grp_qp)

    result_q1 = generic.getPublicationsPublishedInYear(2019)
    result_q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
    result_q3 = generic.getMostCitedPublication()
    result_q4 = generic.getMostCitedVenue()
    result_q5 = generic.getVenuesByPublisherId("crossref:78") #
    result_q6 = generic.getPublicationInVenue("issn:0944-1344")
    #result_q7 = generic.getJournalArticlesInIssue("9", "17", "issn:2164-5515")
    result_q7 = generic.getJournalArticlesInIssue("4", "59", "issn:0168-7433")
    result_q8 = generic.getJournalArticlesInVolume("17", "issn:2164-5515")
    result_q9 = generic.getJournalArticlesInJournal("issn:2164-5515")
    result_q10 = generic.getProceedingsByEvent("web")
    result_q11 = generic.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")
    result_q12 = generic.getPublicationsByAuthorName("silvio")
    result_q13 = generic.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])


if __name__ == "__main__":
    try:

        app()

    except Exception as error:

        print(error)