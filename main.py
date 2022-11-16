################################################################################
#                                                                              #
# App Entry Point.                                                             #
#                                                                              #
################################################################################

# time.
import time

# common utilities internal.
from common_utils import blazegraph_instance_is_active, blazegraph_instance_is_empty, create_folder, start_blazegraph_server

from query_processors import RelationalQueryProcessor, TriplestoreQueryProcessor
from relational_processor import RelationalDataProcessor

from triplestore_processor import TriplestoreDataProcessor

# generic query processor.
from query_processors import GenericQueryProcessor

# const.
from const import *

# Queries.
from queries import *



#####################
#                   #
# Program.          #
#                   #
# app(): None       #
#                   #
#####################
def app():

    print('-- INFO: App is running. ', EMOJI_LED_GREEN )

    try:
        # Flag to know if the data has been uploaded correctly in blazegraph.
        data_has_been_uploaded = False

        #
        # First Blazegraph instance running check.
        # If it's not running, then start it.
        #
        if not blazegraph_instance_is_active():
            # Start blazegraph instance.
            start_blazegraph_server()
            
            # Wait 3 sec while blazegraph is starting up.
            time.sleep(3)

        #
        # Second Blazegraph instance running check.
        # If anyway not run, then abort.
        #
        if not blazegraph_instance_is_active():
            
            data_has_been_uploaded = False

            raise Exception('-- ERR: Blazegraph instance is not running. ', EMOJI_LED_RED )
        else:
            print('-- INFO: Balazegraph instance is running. ', EMOJI_LED_GREEN)

            try:
                ###########################
                #                         #
                # Triplestore processing. #
                #                         #
                ###########################
                if blazegraph_instance_is_empty():
                    # Init TriplestoreDataProcessor.
                    triplestore_data_processor = TriplestoreDataProcessor()

                    # Write its "enpointUrl" property.
                    triplestore_data_processor.setEnpointUrl(BASE_URL)

                    # Create a list with files to process.
                    file_list = [
                            GRAPH_CSV_FILE,
                            GRAPH_JSON_FILE
                        ]

                    #
                    # For each file in the list, process it.
                    #
                    for path in file_list:
                        if "graph" in path:
                            data_has_been_uploaded = triplestore_data_processor.uploadData(path)
                else:
                    print('-- INFO: RDF Graph has been loaded to Blazegraph db yet.')
                    pass

                ###################
                #                 #
                # Rdb processing. #
                #                 #
                ###################

                # Init RelationalDataProcessor.
                relational_data_processor = RelationalDataProcessor()

                # Set the database path.
                relational_data_processor.setDbPath(DB_PATH)

                print("-- INFO: Uploading csv data for relational...")
                data_has_been_uploaded = relational_data_processor.uploadData("data/relational_publications.csv")
                if data_has_been_uploaded:
                    print("-- INFO: Upload completed for relational csv.")

                data_has_been_uploaded = relational_data_processor.uploadData("data/relational_other_data.json")
                print("-- INFO: Uploading json data for relational...")
                if data_has_been_uploaded:
                    print("-- INFO: Upload completed for relational json.")

            except Exception as error:

                print(error)

                data_has_been_uploaded = False

                raise

            finally:
                if data_has_been_uploaded:
                    print('-- INFO: RDF Graph has been loaded to Blazegraph db successfully!')
                    print('-- INFO: RDB Tables has been loaded to sqlite db successfully!')
                else:
                    print('-- ERR: Sorry, Something went wrong...')

            ####################
            #                  #
            # Init Processors. #
            #                  #
            ####################

            #
            # In the next step, create the query processors for both databases, 
            # using the related classes.
            #

            # Triplestore.
            triplestore_query_processor = TriplestoreQueryProcessor()
            triplestore_query_processor.setEnpointUrl(ENDPOINT)

            # RDBMS.
            relational_query_processor = RelationalQueryProcessor()
            relational_query_processor.setDbPath(DB_PATH)

            # Create a list of processors.
            query_processors: list[QueryProcessor] = [triplestore_query_processor, relational_query_processor]

            # Finally, create a generic query processor for asking about data.
            generic = GenericQueryProcessor()
            query_has_been_added = generic.addQueryProcessor(query_processors)

            ###############
            #             #
            # Do Queries. #
            #             #
            ###############

            if query_has_been_added :

                create_folder("queries-results")

                do_query_publications_published_in_year(generic)

                do_publications_by_author_id(generic)

                do_most_cited_publication(generic)

                # do_most_cited_venue(generic) #TODO: FIX - SQL query

                do_venues_by_publisher_id(generic)

                # do_publication_in_venue(generic) # TODO: FIX - Reindexing only valid with uniquely valued Index objects

                do_journal_articles_in_issue(generic)

                do_journal_articles_in_volume(generic)

                # do_journal_articles_in_journal(generic) #TODO: FIX - SQL query

                # TODO: "proceedings by event" query but we dont have the event.

                # do_publication_authors(generic) # TODO: FIX - Reindexing only valid with uniquely valued Index objects

                do_publications_by_author_name(generic)

                # do_distinct_publisher_of_publications(generic) #TODO: FIX - SQL query
            
            print('-- INFO: All results were been produced!')

            if generic.cleanQueryProcessor():
                print('-- INFO: ''Generic Query Processor'' successfully cleaned.')
            else: 
                print('-- INFO: ''Generic Query Processor'' already clean.')

    except Exception as error:

        print(error)

    finally:
        print('-- INFO: App execution terminate.')



############################
#                          #
# Application Entry point. #
#                          #
############################

if __name__ == "__main__":
    app()