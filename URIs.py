# All the resources (including the properties) are defined using
# the [class `URIRef`](https://rdflib.readthedocs.io/en/stable/apidocs/rdflib.html#rdflib.term.URIRef).
# The constructor of this class takes in input a string representing the IRI (or URL) of the resource in consideration.
#
# Using these classes it is possible to create all the Python objects necessary
# to create statements describing all the data to be pushed into an RDF graph.

from rdflib import Namespace
from rdflib import URIRef



################################################################################
#
# Define namespace used.
#
################################################################################
RES      = Namespace("https://allorapy.github.io/res/")
FABIO    = Namespace("http://purl.org/spar/fabio/")
PRISM    = Namespace("http://prismstandard.org/namespaces/basic/2.0/")
CITO     = Namespace("http://purl.org/spar/cito/")
DATACITE = Namespace("http://purl.org/spar/datacite/")
DCTERMS  = Namespace("http://purl.org/dc/terms/")
FOAF     = Namespace("http://xmlns.com/foaf/0.1/")
SCHEMA   = Namespace("https://schema.org/")
WD       = Namespace("http://wikidata.org/")

# my_graph.bind('fabio', FABIO)
# my_graph.bind('prism', PRISM)
# my_graph.bind('wd', WD)
# my_graph.bind('', RES)


################################################################################
#
# Main Class of resources.
#
################################################################################
IdentifiableEntity = URIRef("https://www.wikidata.org/wiki/Q35120")


################################################################################
#
# Inherited Classes.
#
################################################################################

#
# Person.
#
Person = URIRef("http://xmlns.com/foaf/0.1/Person")

#
# Publication.
#
Publication = URIRef("https://www.wikidata.org/wiki/Q732577")
#
# Publication sub-classes.
#
JournalArticle   = URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapter      = URIRef("http://purl.org/spar/fabio/BookChapter")
ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

#
# Venue.
#
Venue = URIRef("https://www.wikidata.org/wiki/Q2085381")
#
# Venue sub-classes.
#
Journal     = URIRef("http://purl.org/spar/fabio/Journal")
Book        = URIRef("http://purl.org/spar/fabio/Book")
Proceedings = URIRef("http://purl.org/spar/fabio/ConferenceProceedings")

#
# Organization.
#
Organization = URIRef("https://schema.org/Organization")


################################################################################
#
# Properties.
#
################################################################################

#
# IdentifiableEntity properties.
#
id = URIRef("http://purl.org/dc/terms/identifier")

#
# Person properties.
#
givenName  = URIRef("http://xmlns.com/foaf/0.1/givenName")
familyName = URIRef("http://xmlns.com/foaf/0.1/familyName")

#
# Publication properties.
#
title           = URIRef("http://purl.org/dc/terms/title")
publicationYear = URIRef("http://purl.org/spar/fabio/hasPublicationYear")

#
# JournalArticle sub-class properties.
#
issue  = URIRef("http://prismstandard.org/namespaces/basic/2.0/issueIdentifier")
volume = URIRef("http://purl.org/spar/fabio/JournalVolume")

#
# BookChapter sub-class properties.
#
chapterNumber = URIRef("http://purl.org/spar/fabio/hasSequenceIdentifier")

#
# ProceedingsPaper sub-class properties.
#
# null.

#
# Venue properties.
#
# title = URIRef("http://purl.org/dc/terms/title")  // Already definied in Publication.

#
# Journal sub-class properties.
#
# null.

#
# Book sub-class properties.
#
# null.

#
# Proceedings sub-class properties.
#
event = URIRef("https://schema.org/Event")

#
# Organization properties.
#
name = URIRef("https://schema.org/name")

################################################################################
#
# Relations.
#
################################################################################

# Classes relations.
hasAuthor           = URIRef("http://purl.org/dc/terms/creator")
hasCites            = URIRef("http://purl.org/spar/cito/cites") 
hasPublicationVenue = URIRef("https://schema.org/isPartOf")
hasPublisher        = URIRef("http://purl.org/dc/terms/publisher")