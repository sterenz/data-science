################################################################################
#                                                                              #
# Data Model Classes.                                                          #
#                                                                              #
################################################################################


#
# IdentifiableEntity Class.
#
class IdentifiableEntity(object):
    # Constructor.
    def __init__(self, _id_list) -> None:
        self.id_list = _id_list


    # getIds(): list[string].
    def getIds(self):

        return self.id_list


#
# Person Class.
#
class Person(IdentifiableEntity):
    def __init__(self, _id_list, _givenName, _familyName) -> None:
        self.givenName  = _givenName  # string [1].
        self.familyName = _familyName # string [1].

        super().__init__(_id_list)


    # getGivenName(): string.
    def getGivenName(self):
        return self.givenName


    # getFamilyName(): string.
    def getFamilyName(self):
        return self.familyName


#
# Publication Class.
#
class Publication(IdentifiableEntity):
    def __init__(self, _id_list, _publicationYear, _title) -> None:
        self.publicationYear = _publicationYear # integer [0..1].
        self.title           = _title # string  [1].
        
        super().__init__(_id_list)



    # getPublicationYear(): integer[0..1].
    def getPublicationYear(self):
        if self.publicationYear != None:
            return self.publicationYear
        else: 
            return None



    # getTitle(): string.
    def getTitle(self):
        return self.title



    #TODO: implement!.
    # getCitedPublications(): list[Publication].
    def getCitedPublications():
        pass


    #TODO: implement!.
    # getPublicationVenue(): Venue.
    def getPublicationVenue():
        pass



    #TODO: implement!.
    # getAuthors(): set[Person].
    def getAuthors():
        pass


#
# JournalArticle Class.
#
class JournalArticle(Publication):
    def __init__(self, _id_list, _publicationYear, _title, _issue, _volume) -> None:
        self.issue = _issue # string [0..1].
        self.volume = _volume # string [0..1].

        super().__init__(_id_list, _publicationYear, _title)



    # getIssue(): string or None.
    def getIssue(self):
        match self.issue: 
            case None:
                return None
            case _:
                return self.issue



    # getVolume(): string or None.
    def getVolume(self):
        match self.volume: 
            case None:
                return None
            case _:
                return self.volume



#
# BookChapter Class.
#
class BookChapter(Publication):
    def __init__(self, _id_list, _publicationYear, _title, _chapterNumber) -> None:
        self.chapterNumber = _chapterNumber # integer [1].
        
        super().__init__(_id_list, _publicationYear, _title)



    # getChapterNumber(): integer.
    def getChapterNumber(self): 
        return self.chapterNumber



#
# ProceedingsPaper Class.
#
class ProceedingPaper(Publication):
    def __init__(self, _id_list, _publicationYear, _title) -> None:
        super().__init__(_id_list, _publicationYear, _title)



#
# Venue Class.
#
class Venue(IdentifiableEntity):
    def __init__(self, _id_list, _title) -> None:
        self.title = _title # string [1].

        super().__init__(_id_list)
    
    
    
    # getTitle(): string.
    def getTitle(self):
        return self.title



    #TODO: implement!.
    # getPublisher(): Organization.
    def getPublisher():
        pass



#
# Journal Class.
#
class Journal(Venue):
    def __init__(self, _id_list, _title) -> None:
        super().__init__(_id_list, _title)



#
# Book Class.
#
class Book(Venue):
    def __init__(self, _id_list, _title) -> None:
        super().__init__(_id_list, _title)



#
# Proceedings Class.
#
class Proceedings(Venue):
    def __init__(self, _id_list, _title, _event) -> None:
        self.event = _event # string [1].

        super().__init__(_id_list, _title)



    # getEvent(): string.
    def getEvent(self):
        return self.event



#
# Organization Class.
#
class Organization(IdentifiableEntity):
    def __init__(self, _id_list, _name) -> None:
        self.name = _name # string  [1].
        
        super().__init__(_id_list)



    # getName(): string.
    def getName(self):
        return self.name