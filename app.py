from os import environ
from types import ModuleType
from typing import Any, Dict
from uuid import uuid4
from json import dumps, loads
from datetime import datetime

from sanic import Sanic, response
from sanic.request import Request
from sanic.exceptions import Unauthorized
from sanic.log import logger

from jinja2 import FileSystemLoader
from sanic_jinja2 import SanicJinja2

from yrest.ysanic import MongoServer
from yrest.openapi import OpenApi

from pymongo import MongoClient

from elasticsearch_async import AsyncElasticsearch

from config import Config, Development, Production

import models
from features import Aspect

from yrest.auth import AuthToken
from yrest.utils import Ok, ErrorMessage

class CloseConnection(Exception):
  pass

class Actor:
  def __init__(self, slug):
    self.slug = slug

class Server(MongoServer, OpenApi):
  connections = {}

  def __init__(self, root_model: models.Tree, models: ModuleType, **kwargs: Dict[str, Any]):
    super().__init__(root_model, models, **kwargs)

    self.register_listener(self._set_es, 'before_server_start')
    self.register_listener(self._close_es, 'before_server_stop')

  async def _set_es(self, app, loop):
    app._es = AsyncElasticsearch(hosts = app.config.get("ES_SERVERS", ["localhost"]))

  async def _close_es(self, app, loop):
    app._es.transport.close()

  async def ws_endpoint(self, request, ws):
    from websockets.exceptions import ConnectionClosed
    """Websocket channel"""

    room = None
    actor = None
    messageClass = self._models.Message
    project = None
    while True:
      try:
        data = await ws.recv()
        print(f"Received: {data}")
        if isinstance(data, str):
          data_obj = loads(data)
        if "connected" in data_obj.keys():
          actor = data_obj["connected"]
          room = data_obj["room"]
          if room not in self.connections.keys():
            self.connections[room] = {}
          messages = await messageClass.gets(self._table, path = room)
          await ws.send(dumps({"users": list(self.connections[room].keys()), "messages": [message.to_plain_dict() for message in messages]}))
          self.connections[room][actor] = ws
          project = await self._models.Project.get(self._table, url = room)
          actor = await self._models.User.get(self._table, slug = actor)
        elif "disconnected" in data_obj.keys():
          raise CloseConnection
      except (ConnectionClosed, CloseConnection):
        print(f"{actor.name} died")
        del self.connections[room][actor.slug]
        for user, conn in self.connections[room].items():
          await conn.send({"disconnected": actor.slug})
        break

      if data:
        dataObj = loads(data)
        if "message" in dataObj:
          message = messageClass(user = actor.email, date = datetime.utcnow(), message = dataObj["message"])
          message._table = self._table
          await project.create_child(message, self._models)

        for user, conn in self.connections[room].items():
          print(f"Sending: {data} to: {user}")
          await conn.send(data)

  async def auth(self, request: Request):
    result = await super().auth(request)
    backlog = request.app._models.Backlog(date = datetime.utcnow(), user = request.json["email"], runned_path = f"/auth", aspect = Aspect.AUTH)
    backlog._table = self._table
    await backlog.create()

    return result

  async def updater(self, request: Request, path: str = None):
    result = await super().updater(request, path)

    token = AuthToken.get(request.headers)
    actor = await token.get_actor(self._table, self.config["JWT_SECRET"], self._models.User)
    email = actor.email if actor else "anonymous"
    # actor = await self._models.User.get(self._table, slug = "garito")

    backlog = request.app._models.Backlog(date = datetime.utcnow(), user = email, runned_path = f"/{path}", aspect = Aspect.UPDATER)
    backlog._table = self._table
    await backlog.create()

    return result

  async def dispatcher(self, request: Request, path: str = None):
    result = await super().dispatcher(request, path)

    token = AuthToken.get(request.headers)
    actor = await token.get_actor(self._table, self.config["JWT_SECRET"], self._models.User)
    email = actor.email if actor else "anonymous"
    # actor = await self._models.User.get(self._table, slug = "garito")

    backlog = request.app._models.Backlog(date = datetime.utcnow(), user = email, runned_path = f"/{path}" if path else "/", aspect = Aspect.DISPATCHER)
    backlog._table = self._table
    await backlog.create()

    return result

  async def factory(self, request: Request, model, path: str = None):
    result = await super().factory(request, model, path)

    token = AuthToken.get(request.headers)
    actor = await token.get_actor(self._table, self.config["JWT_SECRET"], self._models.User)
    email = actor.email if actor else "anonymous"
    # actor = await self._models.User.get(self._table, slug = "garito")

    backlog = request.app._models.Backlog(date = datetime.utcnow(), user = email, runned_path = f"/{path}", aspect = Aspect.FACTORY)
    backlog._table = self._table
    await backlog.create()

    return result

  async def remover(self, request: Request, path: str = None):
    result = await super().remover(request, path)

    token = AuthToken.get(request.headers)
    actor = await token.get_actor(self._table, self.config["JWT_SECRET"], self._models.User)
    email = actor.email if actor else "anonymous"
    # actor = await self._models.User.get(self._table, slug = "garito")

    backlog = request.app._models.Backlog(date = datetime.utcnow(), user = email, runned_path = f"/{path}", aspect = Aspect.REMOVER)
    backlog._table = self._table
    await backlog.create()

    return result

# class Server(MongoServer, OpenApi):
#   async def ask_invitation(self, request, **kwargs):
#     logger.info(f"{kwargs['invitation'].email} has asked for an invitation")

#     app_name = request.app.extensions["jinja2"].env.globals["app_name"]
#     subject = f"Your {app_name}'s invitation"
#     to = kwargs['invitation'].email

#     html = await self.extensions["jinja2"].render_string_async("ask_invitation.html", request, **kwargs)

#     attachments = ["./imgs/logo.gif"]

#     await self.send_email(to, subject, html = html, attachments = attachments)

#     admins = await request.app._models.User.gets(self._table, roles = "admin")
#     subject = f"{to} has asked for an invitation"

#     html = await self.extensions["jinja2"].render_string_async("invitation_asked.html", request, **kwargs)
#     for admin in admins:
#       to = f"{admin.name} <{admin.email}>"
#       await self.send_email(to, subject, html = html, attachments = attachments)

#   async def new_user(self, request, **kwargs):
#     logger.info(f"{kwargs['user'].name} has registered an account")

#     subject = f"We have a new user: {kwargs['user'].name}"
#     html = await self.extensions["jinja2"].render_string_async("new_user.html", request, **kwargs)
#     attachments = ["./imgs/logo.gif"]

#     admins = await request.app._models.User.gets(self._table, roles = "admin")
#     for admin in admins:
#       to = f"{admin.name} <{admin.email}>"
#       await self.send_email(to, subject, html = html, attachments = attachments)

#   async def forgot_password(self, request, **kwargs):
#     actors_name = kwargs['actor'].name

#     logger.info(f"{actors_name} has forgotten the sign in password")

#     app_name = request.app.extensions["jinja2"].env.globals["app_name"]
#     subject = f"Reset your {app_name}'s password"
#     to = f"{actors_name} <{kwargs['actor'].email}>"

#     html = await self.extensions["jinja2"].render_string_async("forgot_password.html", request, **kwargs)

#     attachments = ["./imgs/logo.gif"]

#     return await self.send_email(to, subject, html = html, attachments = attachments)

def create_app(config: Config = Development) -> MongoServer:
  if needs_setup(config):
    from setup import SetupMongoServer, Setup
    app = SetupMongoServer(Setup, models, strict_slashes = True)
  else:
    app = Server(models.Group, models, strict_slashes = True)

  # app = SetupMongoServer(Setup, models, strict_slashes = True) if needs_setup(config) else Server(models.Group, models, strict_slashes = True)
  app.config.from_object(config)

  SanicJinja2(app, loader = FileSystemLoader("./templates"), pkg_name = "yrest.ysanic", enable_async = True)
  app_globals = {
    "server_name": app.config["SERVER_NAME"], "app_url": app.config["APP_URL"], "app_name": app.config['OA_INFO']['title'], "app_description": app.config['OA_INFO']['description']
  }
  app.extensions["jinja2"].env.globals.update(app_globals)

  if app.config.get("DEBUG", False):
    app.register_middleware(app._allow_origin, "response")

  return app

def needs_setup(config: Config) -> bool:
  client = MongoClient(config.MONGO_URI)
  table = client[config.MONGO_DB][getattr(config, "MONGO_TABLE", config.MONGO_DB)]
  root = table.find_one({"path": ""})

  return not bool(root)

if __name__ == "__main__":
  app = create_app(Production) if 'SANIC_PRODUCTION_MODE' in environ else create_app()

  app.run(host = app.config.get("HOST", "localhost"), port = app.config.get("PORT", 8000), auto_reload = app.config.get("DEBUG", False))
