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
                        self.processor.graphBuilder() #TEST

                        # Deploy all triples to blazegraph.
                        self.processor.triplestoreDeploy() #TEST

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