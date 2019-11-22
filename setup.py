from dataclasses import dataclass
from uuid import UUID, uuid4

from sanic import response
from sanic.exceptions import Unauthorized
from sanic.request import Request
from sanic.log import logger

from dataclasses_jsonschema import JsonSchemaMixin

from yrest.tree import Tree
from yrest.mongo import Mongo
from yrest.ysanic import MongoServer
from yrest.openapi import OpenApi
from yrest.utils import  Ok, OkResult, ErrorMessage

@dataclass
class SetupModeRequest(JsonSchemaMixin):
  code: UUID
  group: "Group"
  admin: "User"

class Setup(Mongo, Tree):
  """Allows to setup the app"""
  @classmethod
  async def setup(self, request: Request, consume: SetupModeRequest) -> OkResult:
    """Setups the app"""
    if str(consume.code) != str(request.app.setup_code):
      raise Unauthorized("Check your server's logs to get your authorization code")

    try:
      consume.group["path"] = ""
      group = request.app._models.Group(**consume.group)
      group._table = request.app._table
    except TypeError:
      raise TypeError("Invalid group")

    try:
      consume.admin["roles"] = ["admin"]
      admin = request.app._models.User(**consume.admin)
    except TypeError as e:
      raise TypeError(f"Invalid user: {e}")

    await group.create()
    await group.create_child(admin, request.app._models)

class SetupMongoServer(MongoServer, OpenApi):
  def __init__(self, root_model, models, *args, **kwargs):
    super().__init__(root_model, models, *args, **kwargs)

    self.add_route(self.dispatcher, "/", ["PUT"])

    self.setup_code = uuid4()
    logger.info(f"Setup code: {self.setup_code}")

  async def updater(self, request, path: str = None):
    try:
      data = SetupModeRequest(**request.json)
      await request.app._root_model.setup(request, data)
      return response.json(Ok())
    except TypeError as e:
      return response.json(ErrorMessage(message = e.args[0], code = 400), 400)
    except Unauthorized as e:
      return response.json(ErrorMessage(message = e.args[0], code = 401), 401)
