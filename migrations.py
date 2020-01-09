from os import environ

from argparse import ArgumentParser

from pymongo import MongoClient

from config import Development, Production

def table():
  modul = Production if 'SANIC_PRODUCTION_MODE' in environ else Development
  return MongoClient(modul.MONGO_URI)[modul.MONGO_DB][modul.MONGO_DB]

def url(obj):
  if obj["path"] == "":
    return "/"
  elif obj["path"] == "/":
    return "/{}".format(obj["slug"])
  else:
    return "{}/{}".format(obj["path"], obj["slug"])

def removeAreasAndThemes():
  thetable = table()
  thetable.update_many({"type": {"$in": ["Project", "Record"]}}, {"$unset": {"areas": "", "themes": ""}})

def addEmptyDepartment():
  thetable = table()
  thetable.update_many({"type": {"$in": ["Project", "Record"]}}, {"$set": {"department": ""}})

def addRequesters():
  from csv import reader
  from json import dumps
  with open('requester.csv', newline = '') as f:
    r = reader(f, delimiter = ';')
    requester = None
    result = {}
    next(r)
    for req, subreq in r:
      if req:
        requester = req
        result[requester] = []
      if subreq != 'NONE':
        result[requester].append(subreq)

    with open('requesters.json', 'w') as f:
      f.write(dumps(result, indent = 2))

if __name__ == "__main__":
  parser = ArgumentParser()
  parser.add_argument("action", help = "Run the specified action")
  args = parser.parse_args()
  locals()[args.action]()
