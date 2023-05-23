from numpy import average, quantile, sum as npsum, ceil
from flask import Flask, jsonify, request
from store import Store

application = Flask(__name__)
store = Store()

def generateBookStats(doc):
    return {
        'booknumber': doc['booknumber'],
        'totalwords': int(npsum([chapter['wordcount'] for chapter in doc['chapters']])),
        'totalverses': int(npsum([chapter['versecount'] for chapter in doc['chapters']])),
        'averagewords': int(average([chapter['wordcount'] for chapter in doc['chapters']])),
        'averageverses': int(average([chapter['versecount'] for chapter in doc['chapters']])),
        'quartilewords': quantile([chapter['wordcount'] for chapter in doc['chapters']], [0, 0.25, 0.5, 0.75, 1]).tolist(),
        'quartileverses': quantile([chapter['versecount'] for chapter in doc['chapters']], [0, 0.25, 0.5, 0.75, 1]).tolist()
    }

def generateChapterStats(doc):
    return {
        'chapternumber': doc['chapternumber'],
        'totalwords': int(npsum(doc['verses'])),
        'averagewords': int(average(doc['verses'])),
        'quartilewords': quantile(doc['verses'], [0, 0.25, 0.5, 0.75, 1]).tolist()
    }

def generateSubscriptionStats(wordcounts):
    return {
        'issues': wordcounts,
        'totalwords': int(npsum(wordcounts)),
        'averagewords': int(average(wordcounts)),
        'quartilewords': quantile(wordcounts, [0, 0.25, 0.5, 0.75, 1]).tolist()
    }

@application.route('/books')
def getbooks():
    docs = store.getbooks()
    stats = [generateBookStats(doc) for doc in docs]
    return jsonify(stats)

@application.route('/books/<booknumber>')
def getbook(booknumber):
    docs = store.getbook(int(booknumber))
    stats = [generateBookStats(doc) for doc in docs]
    return jsonify(stats)

@application.route('/books/<booknumber>/chapters')
def getchapters(booknumber):
    docs = store.getchapters(int(booknumber))
    stats = [generateChapterStats(doc) for doc in docs]
    return jsonify(stats)

@application.route('/books/<booknumber>/chapters/<chapternumber>')
def getchapter(booknumber, chapternumber):
    docs = store.getchapter(int(booknumber), int(chapternumber))
    stats = [generateChapterStats(doc) for doc in docs]
    return jsonify(stats)

@application.route('/subscription', methods=['POST'])
def getsubscription():
    body = request.get_json()
    verseDosage = body['verseDosage']
    isChapterSubscription = body['isChapterSubscription']
    bookPool = body['bookPool']
    currentIssue = body['currentIssue']
    docs = []
    verses = []
    if isChapterSubscription == True:
        docs = store.getchaptersubscription(bookPool, currentIssue)
        verses = docs[0]['chapters']
    else:
        docs = store.getversesubscription(bookPool, currentIssue)
        verses = docs[0]['verses']
    numissues = int(ceil(len(verses) / verseDosage))
    wordcounts = [sum(verses[i * verseDosage:(i + 1) * verseDosage]) for i in range(numissues)]
    stats = generateSubscriptionStats(wordcounts)
    return jsonify(stats)

@application.errorhandler(404)
def handlenotfound(error):
    return jsonify({ 'error': 'Route not found!' }), 404

@application.errorhandler(500)
def handleservererror(error):
    return jsonify({ 'error': 'Server error encountered!' }), 500

if __name__ == '__main__':
    application.run(host='0.0.0.0')
