import sys
import os
import json
import codecs
import time
import calendar

now = time.time()
noTime = now - now
# global variables for elapsed time tracking:
timeSpentValidatingData = noTime
totalTweetsInCleanData = 0
totalDiscardedTweetsOrFragments = 0
totalCleanDataFiles = 0
totalDiscardedDataFiles = 0
totalDiscardedDuplicateTweets = 0
listOfTweetIdsMarkedForDeduplication = []
# this dict will be keyed on filename, value will be number of tweets deleted as duplicates from the original quantity
dictOfFilesFromWhichDuplicatesRemoved = {}
listOfTweetIdsPoppedFromDeduplicatedJSON = []
listOfCleanedTweetFilesDiscardedAsUnreadableJSON = []
# some hard-coded directory names:
basePath = 'D:\\var\\tmp\\python-ta\\data\\'
rawInputDir = basePath + 'raw'
cleanTrimmedJsonDir = basePath + 'clean-trimmed'
deduplicatedJsonDir = basePath + 'deduped'
testDeduplicatedJsonDir = basePath + 'deduped-scratch'
resultsDir = basePath + 'results'
mergeDir = basePath + 'merged'
logDir = basePath + 'log'
#
# 'test set' directories
#
# cleanTrimmedJsonDir = basePath + 'clean-trimmed-test-set'
# deduplicatedJsonDir = basePath + 'deduped-scratch'
#
# log file names
markedForDedupLogFN = logDir + '\\' + 'tweetIdsMarkedForDeduplication.log'
tweetIdsPoppedFromDeduplicatedJSONLogFN = logDir + '\\' + 'tweetIdsPoppedFromDeduplicatedJSON.log'
deduplicationCountsByFileLogFN = logDir + '\\' + 'deduplicatedTweetCountByFile.log'
filesDiscardedLogFN = logDir + '\\' + 'filesDiscardedAsUnreadableJSON.log'
allTweetIdsDeduplicatedLogFN = logDir + '\\' + 'deduplicatedTweetsFullSetOfIDs.log'
statusLogFN = logDir + '\\' + 'statusMessages.log'

#
# NOTE on maxTweetsPerFile (a variable used to limit size of CLEANED data:
#   setting value at
#       1000
#   caused an exception that looked like a memory/size error when loading the file into a
#   json structure in the method parseSelectedTweetElementsFromCleanJSON(), at line: tweets = json.load(f)
#   the same raw-source file's data made it successfully through parseSelectedTweetElementsFromCleanJSON() when
#   value was set at:
#       500
#
#   - masover, 23 Oct 2013
#
# NOTE TO SELF: could reduce this value radically if a significant fraction of ingested / cleaned / trimmed files were
#               unreadable as JSON and therefore unable to be de-duplicated. theory: reduction of this value would
#               result in more files, but fewer tweets thrown away in JSON-unreadable files -- 10 Dec 2013
#
#
maxTweetsPerFile = 50
maxTweetsPerAggregatedFile = 1000

def tainhale(filename):
  global totalTweetsInCleanData
  global totalDiscardedTweetsOrFragments
  global statusLogFN

  start_time = time.time()
  f = open(filename,'rU')
  #
  # focusbegin is the place in the file to begin "inhaling" (no reason this should be other than zero, i.e., first char)
  # focussize is the number of characters to read in each "inhalation" ... set to > the largest Tweet
  #
  focusbegin = 0
  focussize =10000
  #
  # The first tweet in a Twitter Archivist file begins with the second open brace ({}:
  #    {"statuses":[{"metadata": ...
  #
  # This should occur well within the first 100 characters, hence the 'short' first read that follows
  #
  f.seek(focusbegin)
  focus = f.read(100)
  firstTweetOpenIndex = indexSecondOpenBrace(focus)
  #
  # firstTweetOpenIndex is now the index of the string in-focus (focus) from which to begin extracting a full Tweet
  #
  nextTweetOpenIndex = firstTweetOpenIndex
  cumulativeIndex = nextTweetOpenIndex
  nextTweetCloseIndex = 0
  tweetCount = 0
  tweets = []
  # in the course of the following while-loop, indexClosingBrace() returns a -1 with which to set
  #  nextTweetCloseIndex if/when no closing brace is found for the next tweet
  f.seek(nextTweetOpenIndex)
  focus = f.read(focussize)
  while (nextTweetCloseIndex >= 0):
    nextTweetOpenIndex = indexNextOpenBrace(focus)
    if nextTweetOpenIndex == -1:
      # print 'File ended with a complete tweet!'
      break
    nextTweetCloseIndex = indexClosingBrace(focus,nextTweetOpenIndex)
    if nextTweetCloseIndex != -1:
      # print 'Cumulative Index: ' + str(cumulativeIndex)
      # print 'next CLOSE, next OPEN, CLOSE - OPEN: ' + str(nextTweetCloseIndex)  + ', ' + str(nextTweetOpenIndex) + ', ' + str(nextTweetCloseIndex - nextTweetOpenIndex)
      #
      # add the length of the next tweet, plus two; the addition of two puts the start of the next focus
      #  (i.e., the f.seek statement below) at the next open brace, skipping the close brace of the current tweet and
      #  the comma that separates tweets; without this addition, the next read (into the string focus) would slowly
      #  creep backward until it began with an open brace that is NOT the start of a new tweet
      #
      cumulativeIndex += (nextTweetCloseIndex - nextTweetOpenIndex + 2)
      tweetCount += 1
      nextTweet = focus[nextTweetOpenIndex:(nextTweetCloseIndex+1)]
      # print 'Tweet #' + str(tweetCount) + ': ' + str(nextTweetOpenIndex) + ',' + str(nextTweetCloseIndex) + ' | ' + nextTweet
      #
      # the following try: block will raise an error (ValueError) if the nextTweet is malformed; this is
      #  a test to filter out capture of malformed tweets from downstream processing
      #
      if validateTweet(nextTweet):
        trimmedNextTweet = parseSelectedTweetElementsFromCleanString(nextTweet)
        tweets.append(trimmedNextTweet)
      else:
        # print '[tainhale()] Tweet #' + str(tweetCount) + ' is MALFORMED or INCOMPLETE: ' + str(nextTweetOpenIndex) + ',' + str(nextTweetCloseIndex) + ' | ' + nextTweet
        totalDiscardedTweetsOrFragments += 1
    else:
      # print 'Incomplete tweet at end of input file!'
      break
    # print 'Cumulative index = ' + str(cumulativeIndex) + '; tweetCount = ' + str(tweetCount) + '[filename = ' + filename + ']'
    f.seek(cumulativeIndex,0)
    focus = f.read(focussize)
  f.close()
  end_time = time.time()
  totalTweetsInCleanData = totalTweetsInCleanData + len(tweets)
  printAndLog('Elapsed time to parse ' + str(len(tweets)) + ' tweets: ' + str(end_time - start_time) + ' seconds.',statusLogFN)
  return tweets

def validateTweet(candidateTweet):
  global timeSpentValidatingData

  start_time = time.time()

  isValid = True
  # the following try: block will raise an error (ValueError) if the nextTweet is malformed; this is
  #  a test to filter out capture of malformed tweets from downstream processing
  try:
    candidateTweetAsJSON = json.loads(str(candidateTweet))
  except ValueError:
    isValid = False
    # print '[validateTweet()] Tweet is MALFORMED: ' + candidateTweet
  else:
    # CHECK FOR EXISTENCE OF WANTED TWEET ELEMENTS, cf.
    #  parseSelectedTweetElementsFromCleanString()
    #
    # BRITTLE CODE: this set of checks needs to be synched with parseSelectedTweetElementsFromCleanString()
    #
    # note that the following elements parsed in parseSelectedTweetElementsFromCleanString() are NOT checked
    #  here because parseSelectedTweetElementsFromCleanString() handles missing elements (value = None)
    #  already:
    #
    #    candidateTweet['geo']
    #    candidateTweet['coordinates']
    #    candidateTweet['place']
    #    candidateTweet['in_reply_to_user_id']
    #    candidateTweet['in_reply_to_screen_name']
    #    candidateTweet['in_reply_to_status_id']
    #
    #
    # ALSO: some wanted tweet elements are not checked -- see commented-out code below; this is a lazy
    #       solution to dealing with complex (regex) checks, adopted because they were not necessary
    #       to a complete run through the data collected up to 10/28/2013 at 13:01 (1:01 pm). That is
    #       the not-a-Tweet instances were discovered without these checks
    #
    #
    # this first if statement is intended to find "icky" structure-malforming user-input text, e.g., this (real) tweet text:
    #     ...   "text":":-\\","source":"<a href=\"http://twitter.com/download/android\"   ...
    # but to avoid user-constructed tweets that end up like this in the input data (i.e., comma following inline quote):
    #     ...   "text":"I said to him, \"Hello you\", then I did cartwheels",
    if ('\\","' in candidateTweet):
      isValid = False
      indexIcky = candidateTweet.find('\\","')
      if indexIcky >= 50:
        print 'Tweet: ' + candidateTweet[(indexIcky-50):]
      else:
        print 'Tweet: ' + candidateTweet[:(indexIcky+30)]
      print 'The tweet printed directly before this line has malforming escaped field-ending double-quotation-mark - tweet discarded...'
    if not (',"id":' in candidateTweet):
      isValid = False
      # print 'Tweet is missing id'
    if not (',"text":' in candidateTweet):
      isValid = False
      # print 'Tweet is missing text'
    if not (',"created_at":' in candidateTweet):
      isValid = False
      # print 'Tweet is missing created_at'
    if not (',"user":{"id":' in candidateTweet):
      isValid = False
      # print 'Tweet is missing user.id'
    # these two require special regex ... forget 'em
    #
    #  if candidateTweetAsJSON['user']['screen_name'] == None:
    #   isValid = False
    #   print 'Tweet is missing user.screen_name'
    # if candidateTweetAsJSON['user']['location'] == None:
    #   isValid = False
    #   print 'Tweet is missing user.location'
    if not (',"retweet_count":' in candidateTweet):
      isValid = False
      # print 'Tweet is missing retweet_count'
    if not (',"entities":{"hashtags":' in candidateTweet):
      isValid = False
      # print 'Tweet is missing hashtags'
    # these two require special regex ... forget 'em
    #
    # if candidateTweetAsJSON['entities']['urls'] == None:
    #   isValid = False
    #   print 'Tweet is missing urls'
    # if candidateTweetAsJSON['entities']['user_mentions'] == None:
    #   isValid = False
    #   print 'Tweet is missing user_mentions'
    end_time = time.time()
    timeSpentValidatingData = timeSpentValidatingData + (end_time - start_time)
  return isValid

def indexSecondOpenBrace(focus):
  if  focus[0] != '{':
    sys.exit('A Twitter Archivist file is expected to begin with an open brace ({). This file does not.')
  i = 1
  secondOBIndex = 0
  while i < len(focus):
    if focus[i] == '{' and secondOBIndex == 0:
      secondOBIndex = i
      break
    else:
      i += 1
  return secondOBIndex

def indexNextOpenBrace(focus):
  i = 0
  obIndex = 0
  while i < len(focus):
    if focus[i] == '{':
      obIndex = i
      break
    else:
      i += 1
    # send -1 if no opening brace was found, signaling file ended w/ complete tweet
    if obIndex == 0: obIndex = -1
  return obIndex

def indexClosingBrace(focus,obIndex):
  i = obIndex
  numOpeningBraces = 0
  numClosingBraces = 0
  closingBraceIndex = 0
  while i < len(focus):
    if focus[i] == '{':
      numOpeningBraces += 1
      #print 'Opening Braces: ' + str(numOpeningBraces)
    elif focus[i] == '}':
      numClosingBraces += 1
      #print 'Closing Braces: ' + str(numClosingBraces)
      if numOpeningBraces == numClosingBraces:
        closingBraceIndex = i
        break
    i += 1
    # send -1 if no closing brace was found, signaling file ended w/ incomplete tweet,
    #  i.e., file was truncated mid-tweet
    if closingBraceIndex == 0: closingBraceIndex = -1
  return closingBraceIndex

def saveListAsFile(list,filename):
  f = codecs.open(filename, 'aU', 'utf-8')
  for item in list:
    f.write(item + '\n')
  f.close

def saveListAsFileJSON(list,filenameBase):
  global totalCleanDataFiles
  #
  # this file runs in a loop to save multiple files with a maximum number of tweets; purpose is to avoid having
  #  to load too large a JSON file in downstream processing.
  #
  i = 0
  j = 0
  k = 0
  while (k+1) < len(list):
    filename = filenameBase[0:filenameBase.find('.json')] + '_' + str(i) + '.json'
    f = codecs.open(filename, 'aU', 'utf-8')
    f.write('{"statuses":[')
    first = True
    while (j < maxTweetsPerFile) and ((k+1) < len(list)):
      j += 1
      k += 1
      if first:
        first = False
      else:
        f.write(',')
      f.write(list[k])
    f.write(']}')
    f.close
    totalCleanDataFiles += 1
    j = 0
    i += 1

def parseSelectedTweetElementsFromCleanString(tweetAsString):
  """

  :param tweetAsString:
  :return:
  """
  tweetAsJSONString = '{"statuses":[' + str(tweetAsString) + ']}'
  fullTweetAsJSON = json.loads(tweetAsJSONString)
  trimmedTweetAsString = '{'
  # if statement below is true if the current structure tweets['statuses'][i] is a tweet; otherwise the structure is
  #  something else and should not be processed as a tweet
  if 'id' in fullTweetAsJSON['statuses'][0]:
    #
    #####################################
    # id of this tweet
    #####################################
    trimmedTweetAsString = trimmedTweetAsString + '"id":' + (str(fullTweetAsJSON['statuses'][0]['id']))
    #####################################
    # text of this tweet
    #####################################
    # cleanTweet escapes special characters in the tweet itself (user input cleanup)
    if 'text' in fullTweetAsJSON['statuses'][0]:
      cleanTweet = cleanUserInputText(fullTweetAsJSON['statuses'][0]['text'])
    else:
      cleanTweet = "EMPTY_TWEET"
      print 'Tweet ID#' + str(fullTweetAsJSON['statuses'][0]['id']) + ' is EMPTY -- no text node.'
    trimmedTweetAsString = trimmedTweetAsString + ',"text":"' + cleanTweet + '"'
    #####################################
    # timestamp of this tweet, in human-readable format (sample: Fri Aug 16 23:31:47 +0000 2013)
    #####################################
    ts = fullTweetAsJSON['statuses'][0]['created_at']
    trimmedTweetAsString = trimmedTweetAsString + ',"timestamp_human-readable":"' + ts + '"'
    #####################################
    # unix timestamp constructed for possible utility as an index in the resultant data set
    #####################################
    ts_unix = calendar.timegm(time.strptime(ts,'%a %b %d %H:%M:%S +0000 %Y'))
    trimmedTweetAsString = trimmedTweetAsString + ',"timestamp_unix":' + str(ts_unix)
    #####################################
    # uid of user who tweeted this tweet
    #####################################
    trimmedTweetAsString = trimmedTweetAsString + ',"user_id":' + str(fullTweetAsJSON['statuses'][0]['user']['id'])
    #####################################
    # screen name of user who tweeted this tweet
    #####################################
    # clean user screen name escapes special characters (user input cleanup)
    if 'screen_name' in fullTweetAsJSON['statuses'][0]['user']:
      cleanUSN = cleanUserInputText(fullTweetAsJSON['statuses'][0]['user']['screen_name'])
      trimmedTweetAsString = trimmedTweetAsString + ',"user_screen_name":"' + cleanUSN + '"'
    #####################################
    # location asserted by user who tweeted this tweet
    #####################################
    # clean user location escapes special characters (user input cleanup)
    if 'location' in fullTweetAsJSON['statuses'][0]['user']:
      cleanUL = cleanUserInputText(fullTweetAsJSON['statuses'][0]['user']['location'])
      trimmedTweetAsString = trimmedTweetAsString + ',"user_location":"' + cleanUL + '"'
    #####################################
    # geographic location from which this tweet was sent
    #####################################
    if fullTweetAsJSON['statuses'][0]['geo'] != None:
      this_geo_coordinates_as_string = '[' + (str(fullTweetAsJSON['statuses'][0]['geo']['coordinates'][0])) + ',' + (str(fullTweetAsJSON['statuses'][0]['geo']['coordinates'][1])) + ']'
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_geo":{"type":"' + fullTweetAsJSON['statuses'][0]['geo']['type'] + '","coordinates":' + this_geo_coordinates_as_string + '}'
    else:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_geo":null'
    #####################################
    # coordinates of location from which this tweet was sent
    #####################################
    if fullTweetAsJSON['statuses'][0]['coordinates'] != None:
      this_coordinates_coordinates_as_string = '[' + (str(fullTweetAsJSON['statuses'][0]['coordinates']['coordinates'][0])) + ',' + (str(fullTweetAsJSON['statuses'][0]['coordinates']['coordinates'][1])) + ']'
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_coordinates":{"type":"' + fullTweetAsJSON['statuses'][0]['coordinates']['type'] + '","coordinates":' + this_coordinates_coordinates_as_string + '}'
    else:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_coordinates":null'
    #####################################
    # place from which this tweet was sent
    #####################################
    # SAMPLE: "place":{"id":"5ef5b7f391e30aff","url":"https://api.twitter.com/1.1/geo/id/5ef5b7f391e30aff.json","place_type":"city","name":"Berkeley","full_name":"Berkeley, CA","country_code":"US","country":"United States","bounding_box":{"type":"Polygon","coordinates":[[[-122.367781,37.835727],[-122.234185,37.835727],[-122.234185,37.905824],[-122.367781,37.905824]]]},"attributes":{}}
    # SAMPLE SELECTED ELEMENTS:
    #   "place":{"id":"5ef5b7f391e30aff","url":"https://api.twitter.com/1.1/geo/id/5ef5b7f391e30aff.json","place_type":"city","name":"Berkeley","full_name":"Berkeley, CA","country_code":"US","country":"United States"}
    # thisTweetAsDict['tweet_place'] = tweets['statuses'][i]['place']
    if fullTweetAsJSON['statuses'][0]['place'] != None:
      thisTweetPlaceSelectedElementsAsDictInnards = '{"id":"' + fullTweetAsJSON['statuses'][0]['place']['id'] + '",' + \
                                                      '"url":"' + fullTweetAsJSON['statuses'][0]['place']['url'] + '",' + \
                                                      '"place_type":"' + fullTweetAsJSON['statuses'][0]['place']['place_type'] + '",' + \
                                                      '"name":"' + fullTweetAsJSON['statuses'][0]['place']['name'] + '",' + \
                                                      '"full_name":"' + fullTweetAsJSON['statuses'][0]['place']['full_name'] + '",' + \
                                                      '"country_code":"' + fullTweetAsJSON['statuses'][0]['place']['country_code'] + '",' + \
                                                      '"country":"' + fullTweetAsJSON['statuses'][0]['place']['country'] + '"' + \
                                                      '}'
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_place":' + thisTweetPlaceSelectedElementsAsDictInnards
    else:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_place":null'
    #####################################
    # uid of user to whom this tweet replies
    #####################################
    if fullTweetAsJSON['statuses'][0]['in_reply_to_user_id'] != None:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_in_reply_to_user_id":' + str(fullTweetAsJSON['statuses'][0]['in_reply_to_user_id'])
    else:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_in_reply_to_user_id":null'
    #####################################
    # screen name of user to whom this tweet replies
    #####################################
    if fullTweetAsJSON['statuses'][0]['in_reply_to_screen_name'] != None:
        trimmedTweetAsString = trimmedTweetAsString + ',"tweet_in_reply_to_screen_name":"' + fullTweetAsJSON['statuses'][0]['in_reply_to_screen_name'] + '"'
    else:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_in_reply_to_screen_name":null'
    #####################################
    # id of tweet to which this tweet replies
    #####################################
    if fullTweetAsJSON['statuses'][0]['in_reply_to_status_id'] != None:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_in_reply_to_status_id":' + str(fullTweetAsJSON['statuses'][0]['in_reply_to_status_id'])
    else:
      trimmedTweetAsString = trimmedTweetAsString + ',"tweet_in_reply_to_status_id":null'
    #####################################
    # number of times this retweet (if > 0) has been retweeted as of time of this retweet; has not been retweeted if val = 0
    #####################################
    trimmedTweetAsString = trimmedTweetAsString + ',"retweet_count":' + str(fullTweetAsJSON['statuses'][0]['retweet_count'])
    #####################################
    # this tweet's hashtags
    #####################################
    hashtagsAsText = '['
    for ht in fullTweetAsJSON['statuses'][0]['entities']['hashtags']:
      # print ht['text']
      hashtagsAsText += '"' + ht['text'] + '",'
    if hashtagsAsText != '[':
      hashtagsAsText = hashtagsAsText[:-1] + ']'
    else:
      hashtagsAsText += ']'
    trimmedTweetAsString = trimmedTweetAsString + ',"hashtags":' + hashtagsAsText
    #####################################
    # this tweet's expanded_urls
    #####################################
    expanded_urlsAsText = '['
    for url in fullTweetAsJSON['statuses'][0]['entities']['urls']:
      # print url['url']
      # print url['expanded_url']
      expanded_urlsAsText += '"' + url['expanded_url'] + '",'
    if expanded_urlsAsText != '[':
      expanded_urlsAsText = expanded_urlsAsText[:-1] + ']'
    else:
      expanded_urlsAsText += ']'
    trimmedTweetAsString = trimmedTweetAsString + ',"expanded_urls":' + expanded_urlsAsText
    #####################################
    # this tweet's User Mentions
    #####################################
    user_mentionsAsText = '['
    for um in fullTweetAsJSON['statuses'][0]['entities']['user_mentions']:
      # print um['id']
      # print um['screen_name']
      user_mentionsAsText += '{"um_id":' + str(um['id']) + ',"um_screen_name":"' + um['screen_name'] + '"},'
    if user_mentionsAsText != '[':
      user_mentionsAsText = user_mentionsAsText[:-1] + ']'
    else:
      user_mentionsAsText += ']'
    trimmedTweetAsString = trimmedTweetAsString + ',"user_mentions":' + user_mentionsAsText
    # close tweet with close-brace
    trimmedTweetAsString = trimmedTweetAsString + '}'
  return trimmedTweetAsString

def cleanUserInputText(textAsInput):
  cleanText = textAsInput
  # the following line escapes instances of backslash in user input, which is killing json.load()
  #  in cases where the escaped character is not actually escapable (cf. allowed set on front page of json.org)
  cleanText = cleanText.replace('\\','ESC-BKSLSH')
  #
  cleanText = cleanText.replace('"','ESC-QUOT')
  cleanText = cleanText.replace('\n',' ')
  cleanText = cleanText.replace('\r',' ')
  return cleanText

def writeJSON(writableFileName,listOfListOfJSONElements):
  global totalCleanDataFiles
  #
  # construct a filename for saved output from this function
  #
  # writableFileName = 'selected-elements-from-' + inputFilename
  # print writableFileName
  # print 'index of ".json" in filename minus length of filename: ' + str(writableFileName.find('.json') - len(writableFileName))
  # print 'here is the suffix of file using a negative index number: ' + writableFileName[(writableFileName.find('.json') - len(writableFileName)):]
  # filenameSuffix = writableFileName[(writableFileName.find('.json') - len(writableFileName)):]
  # if filenameSuffix != '.json':
    # print 'changing suffix...'
  #  writableFileName = writableFileName + '.json'
  # print 'filename to write: ' + writableFileName
  #
  # write extracted data into a JSON file
  #
  #
  # the "construct a filename..." code above is cruft, leftover from initial drafting of this function, in which
  #  input param was the input filename
  #
  #
  writableFile = codecs.open(writableFileName, 'w', 'utf-8')
  writableFile.write('{"statuses":[')
  i = 0
  for tweet in listOfListOfJSONElements:
    if i != 0:
      # write a comma after all but the last tweet
      writableFile.write(',')
    i += 1
    writableFile.write('{')
    j = 0
    for element in tweet:
      if j != 0:
        # write a comma after all but the first and last elements in a tweet
        writableFile.write(',')
      j += 1
      # print str(element)
      # writableFile.write(str(element))
      writableFile.write(element.encode('utf_8'))
    writableFile.write('}')
  writableFile.write(']}')
  print 'wrote file ' + writableFileName + ' containing ' + str(i) + ' tweets.'
  return

def deduplicateTweets(sourceDir,targetDir):
  # need a store of uniqueTweetIds ... as many as 300K as of 12/11/2013 -- start with a list, see if it fits (faster than file)
  # for each file in sourceDir, iterate through it and:
  #    load as json
  #      --- if succeeds, proceed
  #      --- if fails, iterate counter: totalDiscardedDataFiles += 1
  #    for each object in the json file, check whether the id field (Tweet id) already exists in uniqueTweetIds
  #      --- if yes, mark that object for FUTURE deletion (don't alter list when iterating)
  #      --- if no, add id to uniqueTweetIds and proceed
  #    at end of loop, remove objects marked for deletion (i.e., duplicate tweets)
  #    count the number of objects deleted, and for each iterate counter:  totalDiscardedDuplicateTweets += 1
  #    print a result statement: no duplicates discarded in file FILENAME, or # duplicates discarded [just during debug]
  #    write the deduplicated JSON to disk -- same filename as source file -- in the targetDirectory
  global totalDiscardedDataFiles
  global totalDiscardedDuplicateTweets
  global listOfTweetIdsMarkedForDeduplication
  global dictOfFilesFromWhichDuplicatesRemoved
  global listOfTweetIdsPoppedFromDeduplicatedJSON
  global listOfCleanedTweetFilesDiscardedAsUnreadableJSON

  uniqueTweetIds = []
  # read the filenames in the sourceDir
  cleanedFilesList = os.listdir(sourceDir)
  i = 0
  for i in xrange(len(cleanedFilesList)):
  # test with a few files:
  # for i in [0,1,2]:
    cleanedFN = sourceDir + '\\' + cleanedFilesList[i]
    # thisFile = codecs.open(cleanedFN, 'rU', 'utf-8')
    thisFile = open(cleanedFN,'rU')
    try:
      theseTweets = json.load(thisFile)
      j = 0
      popList = []
      # print 'Number of tweets in this file: ' + str(len(theseTweets['statuses']))
      # print 'Here is the iterator list: ' + str(xrange(len(theseTweets['statuses'])))
      for j in xrange(len(theseTweets['statuses'])):
        if theseTweets['statuses'][j]['id'] in uniqueTweetIds:
          # print 'Marked for deletion: ' + str(theseTweets['statuses'][j]['id']) + ' in file: ' + cleanedFilesList[i]
          popList.append(j)
          listOfTweetIdsMarkedForDeduplication.append(theseTweets['statuses'][j]['id'])
        else:
          uniqueTweetIds.append(theseTweets['statuses'][j]['id'])
        j += 1
      # now pop the marked duplicates (in popList), after reverse-sorting it so the LATER list elements are deleted first
      popList.sort()
      popList.reverse()
      # print 'This is a list of indices to be popped: ' + str(popList)
      previousQuantityOfDiscardedDupes = totalDiscardedDuplicateTweets
      for indexOfDuplicate in popList:
        # print 'This tweet to be deleted: ' + str(theseTweets['statuses'][indexOfDuplicate])
        listOfTweetIdsPoppedFromDeduplicatedJSON.append(theseTweets['statuses'][indexOfDuplicate]['id'])
        theseTweets['statuses'].pop(indexOfDuplicate)
        totalDiscardedDuplicateTweets += 1
      if not totalDiscardedDuplicateTweets <= previousQuantityOfDiscardedDupes:
        num = totalDiscardedDuplicateTweets - previousQuantityOfDiscardedDupes
        dictOfFilesFromWhichDuplicatesRemoved[cleanedFilesList[i]] = (num)
      # now serialize and save the deduplicated JSON to the targetDir
      deduplicatedFN = targetDir + '\\' + cleanedFilesList[i]
      thisFileDeduplicated = codecs.open(deduplicatedFN, 'wU', 'utf-8')
      json.dump(theseTweets,thisFileDeduplicated,ensure_ascii=False,encoding="utf-8")
      print 'File saved after removing ' + str(len(popList)) + ' duplicate tweets: ' + cleanedFilesList[i]
    except ValueError:
      print 'FAIL to json.load this file: ' + cleanedFilesList[i]
      listOfCleanedTweetFilesDiscardedAsUnreadableJSON.append(cleanedFilesList[i])
      totalDiscardedDataFiles += 1
    i += 1
  print ' ----------------------------------- '
  print 'Completed deduplication, input data contained ' + str(len(uniqueTweetIds)) + ' unique Tweet IDs.'
  print ' ----------------------------------- '
  return


def getAllTweetIds(sourceDir):
  global logDir
  # get a list of Tweet Ids from all JSON files in a directory ... ASSUMED that all files are clean JSON containing
  #  tweets with 'id' fields!
  resultsDict = iterateAndExtractSingleJSONelement(sourceDir,'id',True)
  allTweetIds = resultsDict['results']
  unreadableFilesList = resultsDict['unreadableFiles']
  unreadableFilesCount = resultsDict['unreadableFilesCount']
  print ' ----------------------------------- '
  printAndLog('Completed ingestion of Tweet IDs in directory; found ' + str(len(allTweetIds)) + ' Tweet IDs.',statusLogFN)
  printAndLog('Failed to json.load() this many files: ' + str(unreadableFilesCount),statusLogFN)
  if unreadableFilesCount != 0:
    thisFN = 'dedupedFiles-UnreadableJSON.log'
    unreadableDedupedFilesLogFN = logDir + '\\' + thisFN
    saveListAsFile(unreadableFilesList,unreadableDedupedFilesLogFN)
    printAndLog('Deduplicated files unreadable as JSON in: ' + thisFN,statusLogFN)
  print ' ----------------------------------- '
  return allTweetIds

def aggregateAllTweets(sourceDir):
  global logDir
  # get a list of all Tweets (text of) from all JSON files in a directory ... ASSUMED that all files are clean JSON containing
  #  tweets with 'text' fields!
  resultsDict = iterateAndExtractSingleJSONelement(sourceDir,'text',False)
  allTweets = resultsDict['results']
  unreadableAggFilesList = resultsDict['unreadableFiles']
  unreadableAggFilesCount = resultsDict['unreadableFilesCount']
  print ' ----------------------------------- '
  printAndLog('Completed ingestion of all Tweets in directory; found and aggregated ' + str(len(allTweets)) + ' Tweets.',statusLogFN)
  printAndLog('Failed to json.load() this many files: ' + str(unreadableAggFilesCount),statusLogFN)
  print ' ----------------------------------- '
  if unreadableAggFilesCount != 0:
    thisFN = 'dedupedFiles-UnreadableJSON-aggregationPhase.log'
    unreadableDedupedFilesLogFN = logDir + '\\' + thisFN
    saveListAsFile(unreadableAggFilesList,unreadableDedupedFilesLogFN)
    printAndLog('Deduplicated files unreadable as JSON in: ' + thisFN,statusLogFN)
  return allTweets

def iterateAndExtractSingleJSONelement(sourceDir,tweetElement,convertElementToString):
  resultsList = []
  unreadableList = []
  unreadableCount = 0
  # read the filenames in the sourceDir
  filesList = os.listdir(sourceDir)
  printAndLog('Iterating through this many files from directory' + sourceDir + ': ' + str(len(filesList)),statusLogFN)
  i = 0
  for i in xrange(len(filesList)):
  # test with a few files:
  # for i in [0,1,2]:
    cleanedFN = sourceDir + '\\' + filesList[i]
    # thisFile = codecs.open(cleanedFN, 'rU', 'utf-8')
    thisFile = open(cleanedFN,'rU')
    thisFileAsString = thisFile.read()
    try:
      theseTweets = json.loads(thisFileAsString)
      # print 'File contents (head): ' + thisFileAsString[0:100]
      # print 'File contents (tail): ' + thisFileAsString[(len(thisFileAsString)-100):]
      j = 0
      for j in xrange(len(theseTweets['statuses'])):
        #
        # 2014-01-10, 5pm
        #
        # using str() breaks attempts to use this code for strings (e.g., text element)
        #  but not using str breaks attempts to use this code for non-strings (e.g., id element)
        #
        # therefore, hacked the method signature to specify which kind of list to build ... however this
        #  can probably be solved more elegantly.....
        #
        #
        if convertElementToString:
          resultsList.append(str(theseTweets['statuses'][j][tweetElement]))
        else:
          resultsList.append(theseTweets['statuses'][j][tweetElement])
        j += 1
    except ValueError:
      print 'FAIL to json.load this file: ' + filesList[i]
      # print 'File contents (head): ' + thisFileAsString[0:100]
      # print 'File contents (tail): ' + thisFileAsString[(len(thisFileAsString)-100):]
      unreadableList.append(filesList[i])
      unreadableCount += 1
    thisFile.close()
    i += 1
  resultsDict = {}
  resultsDict['unreadableFiles'] = unreadableList
  resultsDict['unreadableFilesCount'] = unreadableCount
  resultsDict['results'] = resultsList
  return resultsDict

def mergeJSONfiles(sourceDir,targetDir):
  unreadableList = []
  unreadableCount = 0
  totalSourceFilesReadCount = 0
  totalSourceTweetsReadCount = 0
  #
  sourceJSONFilesList = os.listdir(sourceDir)
  sourceFilesStagedForReading = len(sourceJSONFilesList)
  printAndLog('Number of files to be read from source directory: ' + str(sourceFilesStagedForReading),statusLogFN)
  targetFilenameBase = 'tweets_'
  targetFilenameCounter = 0
  targetJSONStructurePrefix = '{"statuses":['
  targetJSONStructureSuffix = ']}'
  currentTargetFilename = targetFilenameBase + str(targetFilenameCounter) + '.json'
  currentTargetTweetsList = []
  # outer loop - until all files in sourceDir are ingested and written back out to merged files
  # i is counter for files in sourceDir
  i = 0
  # k is counter for tweets written to the current target (merged) file
  k = 0
  allMergedTweetsCount = 0
  for i in xrange(len(sourceJSONFilesList)):
    sourceFN = sourceDir + '\\' + sourceJSONFilesList[i]
    # thisFile = codecs.open(sourceFN, 'rU', 'utf-8')
    thisFile = open(sourceFN,'rU')
    # thisFileAsString = thisFile.read()
    try:
      totalSourceFilesReadCount += 1
      theseTweets = json.load(thisFile)
      # inner loop -- pulling JSON structures out of files in sourceDir and adding them to JSON structure to be
      #  written to a merged file
      j = 0
      for j in xrange(len(theseTweets['statuses'])):
        # here's the meat:
        #
        # o create a list of JSON 'innards' -- the tweets w/o the wrapper,
        #   where by "wrapper" I mean:
        #     {"statuses":[]}
        # o then with a list of strings that in aggregate represent maxTweetsPerAggregatedFile tweets, write 'em out
        #
        tweetAsString = getStringFromJSONTweet(theseTweets['statuses'][j])
        # currentTargetTweetsList.append(theseTweets['statuses'][j])
        currentTargetTweetsList.append(tweetAsString)
        totalSourceTweetsReadCount += 1
        k += 1
    except ValueError:
      print 'FAIL to json.load this file: ' + sourceJSONFilesList[i]
      unreadableList.append(sourceJSONFilesList[i])
      unreadableCount += 1
    thisFile.close()
    # print 'Appended this source file to merge file in progress (' + str(targetFilenameCounter) + '): ' + str(sourceFN)
    #
    # this if statement triggers a write if currentTargetTweetsList is within maxTweetsPerFile of reaching
    #  maxTweetsPerAggregatedFile ... that is, if one more input file would (likely) put the current set of tweets
    #  collected for merging in a single file 'over the top' of the max number of tweets in a merged file
    # note that the second (OR) clause causes a write if the last source file has been read in
    #
    if ((k + maxTweetsPerFile >= maxTweetsPerAggregatedFile) or
       (i+1 == sourceFilesStagedForReading)):
      #(not (i+1) in xrange(len(sourceJSONFilesList)))):
      #
      # write out target file
      #
      # print 'Writing this many tweets to merged target file (calculated from pre-counter): ' + str(k)
      # print 'Writing this many tweets to merged target file (count of currentTargetTweetsList): ' + str(len(currentTargetTweetsList))
      currentTargetFile = targetDir + '\\' + currentTargetFilename
      f = codecs.open(currentTargetFile, 'aU', 'utf-8')
      f.write(targetJSONStructurePrefix)
      first = True
      counter = 0
      while (counter + 1) <= len(currentTargetTweetsList):
        if first:
          first = False
        else:
          f.write(',')
        f.write(currentTargetTweetsList[counter])
        counter += 1
      f.write(targetJSONStructureSuffix)
      f.close()
      print 'Completed writing >>> ' + str(counter) + ' <<< tweets to this file:'  + str(currentTargetFile)
      allMergedTweetsCount = allMergedTweetsCount + counter
      # clear currentTargetTweetsList[] after writing its contents to file
      currentTargetTweetsList = []
      #
      # reset target file name and counter
      #
      targetFilenameCounter += 1
      currentTargetFilename = targetFilenameBase + str(targetFilenameCounter) + '.json'
      # reset k because target file is incremented to a new file
      k = 0
    i += 1
  printAndLog('Unflushed tweet buffer (tweets not written to file): ' + str(len(currentTargetTweetsList)),statusLogFN)
  printAndLog('Total number of UNREADABLE source files: ' + str(unreadableCount),statusLogFN)
  printAndLog('Total number of source files read: ' + str(totalSourceFilesReadCount),statusLogFN)
  printAndLog('Total number of tweets read from source files: ' + str(totalSourceTweetsReadCount),statusLogFN)
  return allMergedTweetsCount

def getStringFromJSONTweet(jsonTweet):
  # open tweet as string JSON structure
  firstElement = True
  inputJSONAsString = '{'
  # id --- long to str
  if 'id' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"id":' + str(jsonTweet['id'])
  # text
  if 'text' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"text":"' + jsonTweet['text'] + '"'
  # timestamp_human-readable
  if 'timestamp_human-readable' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"timestamp_human_readable":"' + jsonTweet['timestamp_human-readable'] + '"'
  # timestamp_unix
  if 'timestamp_unix' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"timestamp_unix":' + str(jsonTweet['timestamp_unix'])
  # user_id
  if 'user_id' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"user_id":' + str(jsonTweet['user_id'])
  # user_screen_name
  if 'user_screen_name' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"user_screen_name":"' + jsonTweet['user_screen_name'] + '"'
  # user_location
  if 'user_location' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"user_location":"' + jsonTweet['user_location'] + '"'
  # tweet_in_reply_to_status_id
  if 'tweet_in_reply_to_status_id' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['tweet_in_reply_to_status_id'] == None:
      inputJSONAsString = inputJSONAsString + '"tweet_in_reply_to_status_id":null'
    else:
      inputJSONAsString = inputJSONAsString + '"tweet_in_reply_to_status_id":' + str(jsonTweet['tweet_in_reply_to_status_id'])
  # tweet_in_reply_to_user_id
  if 'tweet_in_reply_to_user_id' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['tweet_in_reply_to_user_id'] == None:
      inputJSONAsString = inputJSONAsString + '"tweet_in_reply_to_user_id":null'
    else:
      inputJSONAsString = inputJSONAsString + '"tweet_in_reply_to_user_id":' + str(jsonTweet['tweet_in_reply_to_user_id'])
  # tweet_in_reply_to_screen_name
  if 'tweet_in_reply_to_screen_name' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['tweet_in_reply_to_screen_name'] == None:
      inputJSONAsString = inputJSONAsString + '"tweet_in_reply_to_screen_name":null'
    else:
      inputJSONAsString = inputJSONAsString + '"tweet_in_reply_to_screen_name":"' + jsonTweet['tweet_in_reply_to_screen_name'] + '"'
  # retweet_count
  if 'retweet_count' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"retweet_count":' + str(jsonTweet['retweet_count'])
  if 'tweet_geo' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['tweet_geo'] == None:
      inputJSONAsString = inputJSONAsString + '"tweet_geo":null'
    else:
      tweet_geo_type = jsonTweet['tweet_geo']['type']
      tweet_geo_coordinates = '[' +  str(jsonTweet['tweet_geo']['coordinates'][0]) + ',' + str(jsonTweet['tweet_geo']['coordinates'][1]) + ']'
      inputJSONAsString = inputJSONAsString + '"tweet_geo":{"type":"' + tweet_geo_type + '","coordinates":' + tweet_geo_coordinates + '}'
  if 'tweet_coordinates' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['tweet_coordinates'] == None:
      inputJSONAsString = inputJSONAsString + '"tweet_coordinates":null'
    else:
      tweet_coordinates_type = jsonTweet['tweet_coordinates']['type']
      tweet_coordinates_coordinates = ('[' +  str(jsonTweet['tweet_coordinates']['coordinates'][0]) + ','
                                       + str(jsonTweet['tweet_coordinates']['coordinates'][1]) + ']')
      inputJSONAsString = (inputJSONAsString + '"tweet_coordinates":{"type":"' + tweet_coordinates_type
                           + '","coordinates":' + tweet_coordinates_coordinates + '}')
  if 'tweet_place' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['tweet_place'] == None:
      inputJSONAsString = inputJSONAsString + '"tweet_place":null'
    else:
      tp_country_code = jsonTweet['tweet_place']['country_code']
      tp_url = jsonTweet['tweet_place']['url']
      tp_country = jsonTweet['tweet_place']['country']
      tp_place_type = jsonTweet['tweet_place']['place_type']
      tp_full_name = jsonTweet['tweet_place']['full_name']
      tp_id = jsonTweet['tweet_place']['id']
      tp_name = jsonTweet['tweet_place']['name']
      inputJSONAsString = (inputJSONAsString + '"tweet_place":{'
                          + '"country_code":"' + tp_country_code + '",'
                          + '"url":"' + tp_url + '",'
                          + '"country":"' + tp_country + '",'
                          + '"place_type":"' + tp_place_type + '",'
                          + '"full_name":"' + tp_full_name + '",'
                          + '"id":"' + tp_id + '",'
                          + '"name":"' + tp_name + '"'
                          + '}')
  if 'user_mentions' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    inputJSONAsString = inputJSONAsString + '"user_mentions":['
    um_count = 0
    for um in jsonTweet['user_mentions']:
      um_screen_name = jsonTweet['user_mentions'][um_count]['um_screen_name']
      um_id = jsonTweet['user_mentions'][um_count]['um_id']
      inputJSONAsString = (inputJSONAsString + '{"um_screen_name":"' + um_screen_name + '",'
                          + '"um_id":"' + str(um_id) + '"'
                          + '},')
      um_count += 1
    if um_count > 0:
      # remove final comma from set of user mentions, and add a closing bracket
      inputJSONAsString = inputJSONAsString[:-1] + ']'
    else:
      # if no user mentions, just add the closing bracket
      inputJSONAsString = inputJSONAsString + ']'
  if 'hashtags' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['hashtags'] == None:
      inputJSONAsString = inputJSONAsString + '"hashtags":[]'
    else:
      hashtagsAsText = '['
      for ht in jsonTweet['hashtags']:
        hashtagsAsText += '"' + ht + '",'
      if hashtagsAsText != '[':
        hashtagsAsText = hashtagsAsText[:-1] + ']'
      else:
        hashtagsAsText += ']'
      inputJSONAsString = inputJSONAsString + '"hashtags":' + hashtagsAsText
  if 'expanded_urls' in jsonTweet:
    if not firstElement:
      inputJSONAsString = inputJSONAsString + ','
    firstElement = False
    if jsonTweet['expanded_urls'] == None:
      inputJSONAsString = inputJSONAsString + '"expanded_urls":[]'
    else:
      eUrlsAsText = '['
      for url in jsonTweet['expanded_urls']:
        eUrlsAsText += '"' + url + '",'
      if eUrlsAsText != '[':
        eUrlsAsText = eUrlsAsText[:-1] + ']'
      else:
        eUrlsAsText += ']'
      inputJSONAsString = inputJSONAsString + '"expanded_urls":' + eUrlsAsText
  # close tweet as string JSON structure
  if inputJSONAsString[-1] == ',':
    inputJSONAsString = inputJSONAsString[:-1] + '}'
  else:
    inputJSONAsString = inputJSONAsString + '}'
  return inputJSONAsString

def printAndLog(msg,logFile):
  print msg
  f = open(logFile,'a')
  f.write(msg + '\n')
  f.close()
  return


def main():
  #
  #
  # some CONTROL variables
  ingest = True
  deduplicate = True
  testDeduplicate = True
  aggregate = True
  # ingest = False
  # deduplicate = False
  # testDeduplicate = False
  # aggregate = True

  #
  #
  # checking / user feedback for existence of expected paths
  #
  # basePathExists = os.path.isdir(basePath)
  # rawPathExists = os.path.isdir(rawInputDir)
  # cleanTrimmedJsonPathExists = os.path.isdir(cleanJsonDir)
  # deduplicatedJsonPathExists = os.path.isdir(deduplicatedJsonDir)
  # print 'base path exists: ' + str(basePathExists) + ' (path: ' + basePath + ')'
  # print 'raw path exists: ' + str(rawPathExists) + ' (path: ' + rawInputDir + ')'
  # print 'clean-trimmed JSON path exists: ' + str(cleanTrimmedJsonPathExists) + ' (path: ' + cleanTrimmedJsonDir + ')'
  # print 'deduplicated JSON path exists: ' + str(deduplicatedJsonPathExists) + ' (path: ' + deduplicatedJsonDir + ')'
  #
  #

  if ingest:
    print 'Processing files in raw input directory: ' + rawInputDir
    rawFilesList = os.listdir(rawInputDir)
    startInhaleTime = time.time()
    for item in rawFilesList:
      if os.path.isfile(rawInputDir + '\\' + item):
        print 'Processing raw input file: ' + item
        extractedTweets = tainhale(rawInputDir + '\\' + item)
        print 'Saving extracted tweets to directory: ' + cleanTrimmedJsonDir
        cleanSaveToFilename = cleanTrimmedJsonDir+ '\\' + item
        saveListAsFileJSON(extractedTweets,cleanSaveToFilename)
    endInhaleTime = time.time()
  if deduplicate:
    printAndLog('De-duplicating ' + str(totalTweetsInCleanData) + ' cleaned and trimmed tweets in ' + str(totalCleanDataFiles) + ' files from dir: ' + str(cleanTrimmedJsonDir),statusLogFN)
    startDeduplicationTime = time.time()
    deduplicateTweets(cleanTrimmedJsonDir,deduplicatedJsonDir)
    endDeduplicationTime = time.time()
    printAndLog('De-duplication complete. Results in dir: ' + str(deduplicatedJsonDir),statusLogFN)
    printAndLog('Total time to deduplicate cleaned/trimmed data: ' + str(endDeduplicationTime - startDeduplicationTime) + ' seconds.',statusLogFN)
    print 'Logging deduplication checks...'
    # here, write a number of lists and a dict collected during de-duplication to disk
    # listOfTweetIdsMarkedForDeduplication
    # listOfTweetIdsPoppedFromDeduplicatedJSON
    # dictOfFilesFromWhichDuplicatesRemoved
    # listOfCleanedTweetFilesDiscardedAsUnreadableJSON
    print 'Logging tweet IDs marked for deduplication...'
    f = open(markedForDedupLogFN,'a')
    i = 0
    for i in xrange(len(listOfTweetIdsMarkedForDeduplication)):
      f.write(str(listOfTweetIdsMarkedForDeduplication[i]) + '\n')
      i += 1
    f.close()
    print 'Logging tweet IDs popped (discarded) from JSON files due to duplication...'
    f = open(tweetIdsPoppedFromDeduplicatedJSONLogFN,'a')
    i = 0
    for i in xrange(len(listOfTweetIdsPoppedFromDeduplicatedJSON)):
      f.write(str(listOfTweetIdsPoppedFromDeduplicatedJSON[i]) + '\n')
      i += 1
    f.close()
    printAndLog('dictOfFilesFromWhichDuplicatesRemoved length: ' + str(len(dictOfFilesFromWhichDuplicatesRemoved)),statusLogFN)
    print 'Logging filenames and associated counts of tweets discarded due to duplication...'
    f = open(deduplicationCountsByFileLogFN,'a')
    for key in sorted(dictOfFilesFromWhichDuplicatesRemoved.keys()):
      f.write(str(key) + ' : ' + str(dictOfFilesFromWhichDuplicatesRemoved[key]) + '\n')
    f.close()
    print 'Logging filenames discarded due to unreadable JSON...'
    f = open(filesDiscardedLogFN,'a')
    i = 0
    for i in xrange(len(listOfCleanedTweetFilesDiscardedAsUnreadableJSON)):
      f.write(str(listOfCleanedTweetFilesDiscardedAsUnreadableJSON[i]) + '\n')
      i += 1
    f.close()
    print 'Logging deduplication checks complete.'
  if testDeduplicate:
    startTestDeduplicationTime = time.time()
    printAndLog('Building list of Tweet IDs from all files in directory: ' + str(deduplicatedJsonDir),statusLogFN)
    listOfTweetIds = getAllTweetIds(deduplicatedJsonDir)
    printAndLog('Completed: list of Tweet IDs from all files in: ' + str(deduplicatedJsonDir),statusLogFN)
    printAndLog('Checking single occurrence of tweets removed as duplicates in final set',statusLogFN)
    f = open(tweetIdsPoppedFromDeduplicatedJSONLogFN,'rU')
    missingTweetCount = 0
    missingTweets = []
    for line in f:
      duplicatedTweetId = line.rstrip()
      if not str(duplicatedTweetId) in listOfTweetIds:
        missingTweetCount += 1
        missingTweets.append(duplicatedTweetId)
        print 'Cannot find this tweet in the final set: ' + str(duplicatedTweetId)
    f.close()
    if missingTweetCount == 0:
      printAndLog('Success! All tweets removed as duplicates exist (presumably once) in final, deduplicated data set.',statusLogFN)
    else:
      printAndLog('Sorry! There are this many missing tweets: ' + str(missingTweetCount),statusLogFN)
    # deduplicateTweets(deduplicatedJsonDir,testDeduplicatedJsonDir)
    endTestDeduplicationTime = time.time()
    printAndLog('De-duplication testing complete.',statusLogFN)
  print ' ----------------------------------- '
  if aggregate:
    ###############################################################
    # aggregate deduplicated tweets into fewer, larger files
    ###############################################################
    startMergeTime = time.time()
    printAndLog('Merging files from: ' + str(deduplicatedJsonDir),statusLogFN)
    mergedTweetCount = mergeJSONfiles(deduplicatedJsonDir,mergeDir)
    printAndLog('Merged files (' + str(mergedTweetCount)  + ' tweets) located in: ' + str(mergeDir),statusLogFN)
    endMergeTime = time.time()
    ###############################################################
    # build a file containing the text of all tweets collected
    ###############################################################
    printAndLog('Building file of all Tweets from files in directory: ' + str(deduplicatedJsonDir),statusLogFN)
    listOfAllTweets = aggregateAllTweets(deduplicatedJsonDir)
    thisFN = 'AllTweets.txt'
    allTweetsFN = resultsDir + '\\' + thisFN
    saveListAsFile(listOfAllTweets,allTweetsFN)
  if ingest:
    printAndLog('Total time to ingest/clean/trim raw data: ' + str(endInhaleTime - startInhaleTime) + ' seconds.',statusLogFN)
  if testDeduplicate:
    saveListAsFile(listOfTweetIds,allTweetIdsDeduplicatedLogFN)
    printAndLog('Total time to TEST deduplicate previously de-duplicated data: ' + str(endTestDeduplicationTime - startTestDeduplicationTime) + ' seconds.',statusLogFN)
  if aggregate:
    printAndLog('Total time to MERGE de-duplicated data: ' + str(endMergeTime - startMergeTime) + ' seconds.',statusLogFN)
  printAndLog(' ----------------------------------- ',statusLogFN)
  printAndLog('Total number of cleaned/trimmed tweets: ' + str(totalTweetsInCleanData),statusLogFN)
  printAndLog(' ----------------------------------- ',statusLogFN)
  printAndLog('Total number of files containing cleaned/trimmed tweets: ' + str(totalCleanDataFiles),statusLogFN)
  printAndLog('Total time spent validating data while ingesting/cleaning/trimming: ' + str(timeSpentValidatingData) + ' seconds.',statusLogFN)
  printAndLog('Total number of tweets or tweet-fragments discarded during validation: ' + str(totalDiscardedTweetsOrFragments),statusLogFN)
  printAndLog('Total number of files discarded during de-duplication (did not open as JSON): ' + str(totalDiscardedDataFiles),statusLogFN)
  printAndLog('Total number of tweets detected as duplicates and discarded: ' + str(totalDiscardedDuplicateTweets),statusLogFN)
  printAndLog('END OF RUN',statusLogFN)

if __name__ == '__main__':
  main()