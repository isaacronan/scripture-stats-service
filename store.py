from pymongo import MongoClient
from bson.son import SON
import os

class Store:
    def __init__(self):
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_username = os.getenv('DB_USERNAME')
        db_password = os.getenv('DB_PASSWORD')
        client = MongoClient(
            db_host,
            port=int(db_port),
            username=db_username,
            password=db_password,
            authSource='admin',
        )
        db = client.scripture
        self.collection = db.verses

    def getbooks(self):
        return list(self.collection.aggregate([
            { '$group': {
                '_id': { 'booknumber': '$booknumber', 'chapternumber': '$chapternumber' },
                'versecount': { '$sum': 1 },
                'wordcount': { '$sum': '$wordcount' }
            }},
            { '$group': {
                '_id': { 'booknumber': '$_id.booknumber' },
                'chapters': { '$push': {
                    'versecount': '$versecount',
                    'wordcount': '$wordcount'
                }}
            }},
            { '$project': {
                '_id': False,
                'booknumber': '$_id.booknumber',
                'chapters': True
            }},
            { '$sort': { 'booknumber': 1 }}
        ]))

    def getbook(self, booknumber):
        return list(self.collection.aggregate([
            { '$match': { 'booknumber': booknumber }},
            { '$group': {
                '_id': { 'booknumber': '$booknumber', 'chapternumber': '$chapternumber' },
                'versecount': { '$sum': 1 },
                'wordcount': { '$sum': '$wordcount' }
            }},
            { '$group': {
                '_id': { 'booknumber': '$_id.booknumber' },
                'chapters': { '$push': {
                    'versecount': '$versecount',
                    'wordcount': '$wordcount'
                }}
            }},
            { '$project': {
                '_id': False,
                'booknumber': '$_id.booknumber',
                'chapters': True
            }}
        ]))

    def getchapters(self, booknumber):
        return list(self.collection.aggregate([
            { '$match': { 'booknumber': booknumber }},
            { '$group': {
                '_id': { 'chapternumber': '$chapternumber' },
                'verses': { '$push': '$wordcount' }
            }},
            { '$project': {
                '_id': False,
                'chapternumber': '$_id.chapternumber',
                'verses': True
            }},
            { '$sort': { 'chapternumber': 1 }}
        ]))

    def getchapter(self, booknumber, chapternumber):
        return list(self.collection.aggregate([
            { '$match': { 'booknumber': booknumber, 'chapternumber': chapternumber }},
            { '$group': {
                '_id': { 'chapternumber': '$chapternumber' },
                'verses': { '$push': '$wordcount' }
            }},
            { '$project': {
                '_id': False,
                'chapternumber': '$_id.chapternumber',
                'verses': True
            }}
        ]))

    def getversesubscription(self, bookPool, currentIssue):
        currentBook = currentIssue['currentBook']
        currentChapter = currentIssue['currentChapter']
        currentVerse = currentIssue['currentVerse']
        return list(self.collection.aggregate([
            { '$match': { '$and': [
                { 'booknumber': { '$in': bookPool }},
                { '$or': [
                    { 'booknumber': { '$gt': currentBook }},
                    { 'booknumber': currentBook, 'chapternumber': { '$gt': currentChapter }},
                    { 'booknumber': currentBook, 'chapternumber': currentChapter, 'versenumber': { '$gte': currentVerse }},
                ]}
            ]}},
            { '$sort': SON([('booknumber', 1), ('chapternumber', 1), ('versenumber', 1)])},
            { '$group': {
                '_id': None,
                'verses': { '$push': '$wordcount'}
            }},
            { '$project': { '_id': False }}
        ]))

    def getchaptersubscription(self, bookPool, currentIssue):
        currentBook = currentIssue['currentBook']
        currentChapter = currentIssue['currentChapter']
        return list(self.collection.aggregate([
            { '$match': { '$and': [
                { 'booknumber': { '$in': bookPool }},
                { '$or': [
                    { 'booknumber': { '$gt': currentBook }},
                    { 'booknumber': currentBook, 'chapternumber': { '$gte': currentChapter }},
                ]}
            ]}},
            { '$sort': SON([('booknumber', 1), ('chapternumber', 1)])},
            { '$project': { '_id': False }},
            { '$group': {
                '_id': { 'booknumber': '$booknumber', 'chapternumber': '$chapternumber' },
                'verses': { '$sum': '$wordcount' }
            }},
            { '$sort': SON([('_id.booknumber', 1), ('_id.chapternumber', 1)])},
            { '$group': {
                '_id': None,
                'chapters': { '$push': '$verses' }
            }},
            { '$project': {
                '_id': False
            }}
        ]))