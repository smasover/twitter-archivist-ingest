The code in ta-parser.py extracts a specific set of fields from tweets harvested and exported by Tweet Archivist (https://www.tweetarchivist.com/).

The code corrects for harvested JSON files that are abruptly truncated (harvested file is malformed JSON); corrects for occurrences of input files that contain tweets also included in other input files (i.e., the code deduplicates a result set assembled from multiple input files); and discards data that becomes unreadable in the course of the data processing pipeline (to handle occurrences of errors that have not yet been identified and/or corrected).

The code has not been generalized. It is an artifact of a specific data-harvesting experiment. It was written by a Python novice, caveat emptor....

Copyright in the code is held by The Regents of the University of California; and is released under an ECL 2.0 License (cf. LICENCE.txt).
