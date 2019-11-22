from typing import Any, Union, Optional, Dict, Tuple
from dataclasses import dataclass

from sanic.request import Request

from dataclasses_jsonschema import JsonSchemaMixin

from yrest.tree import Tree
from yrest.mongo import Mongo
from yrest.ysanic import yJSONEncoder
from yrest.auth import Auth, IsAuth

from features import HasUsers, DefinesSecurity, HasDescription, HasName, CanBeRemoved, CanBeRemovedWithFiles, UsedBySystemOnly, SystemNeedsIt, HasRoles, HasContext, HasEmail, CanBeAuthenticated, HasProjects, HasRecords, HasCode, HasPhases, ShouldBeRegistrable, HasDeadline, HasAddress, HasAreas, HasThemes, HasTags, HasStakeholders, HasFiles, IsSearchable, HasMessages, HasMessage, IsTemporalyMarked, FromUser, ShouldBeFinished, HasBacklog, HasPath, HasAspect, ShouldEmitNewsAggregations, AggregatesFiles, AggregatesMessages
from parameters import UpdatePermissionRequest
from yrest.utils import  OkResult, can_crash, ErrorMessage

class SystemNeedsItException(Exception):
  pass

@dataclass
class Group(JsonSchemaMixin, Mongo, Tree, IsAuth, AggregatesMessages, AggregatesFiles, ShouldEmitNewsAggregations, HasBacklog, IsSearchable, HasUsers, DefinesSecurity, HasRecords, HasProjects, HasDescription, HasName):
  async def index(self, request: Request) -> OkResult:
    """Returns the group's data"""
    ancests = await self.ancestors(request.app._models)
    ancestors = [ancestor.to_plain_dict() for ancestor in ancests] if ancests else []
    return {"object": self, "ancestors": ancestors}

  async def get_children(self, request: Request) ->OkResult:
    """Returns the list of projects and records"""
    children = await self._table.find({"$or": [{"type": "Project"}, {"type": "Record"}], "path": self.get_url()}).sort([("record", 1)]).to_list(None)
    result = {}
    for child in children:
      childObj = getattr(request.app._models, child["type"])(**child)
      data = childObj.to_plain_dict()
      data["phaseStats"] = await childObj.phases_stats(request)
      data["fileStats"] = await childObj.files_amount(request)

      result[childObj.slug] = data

    return result

  async def get_tags(self, request: Request) -> OkResult:
    """Returns the list of tags used in the system"""
    tags = await self._table.distinct("tags")
    return tags

  async def get_themes(self, request: Request) -> OkResult:
    """Returns the list of themes used in the system"""
    themes = await self._table.distinct("themes")
    return themes

  async def get_areas(self, request: Request) -> OkResult:
    """Returns the list of areas used in the system"""
    areas = await self._table.distinct("areas")
    return areas

@dataclass
class Role(JsonSchemaMixin, Mongo, Tree, CanBeRemoved, UsedBySystemOnly, SystemNeedsIt, HasDescription, HasName):
  __x_schema__ = {"form": ["name", "description"]}

  @can_crash(SystemNeedsItException, code = 405)
  async def remove(self, request: Request, actor: "User") -> OkResult:
    """Removes the non system role"""
    if self.system:
      raise SystemNeedsItException("Can't remove a system role")

    result = await request.app._generic_remover(request, self, actor)

    role_prefix = f"^{self.slug}@"
    await self._table.update_many({"roles": {"$regex": role_prefix}}, {"$pull": {"roles": {"$regex": role_prefix}}})

    return result

@dataclass
class Permission(JsonSchemaMixin, Mongo, Tree, HasRoles, HasContext, HasName):
  def __sluger__(self, values: Optional[Dict[str, Any]] = None, fields: bool = False) -> Union[Tuple, str]:
    if fields:
      return ("context", "name")
    elif values:
      return f"{values.get('context', self.context)}_{values.get('name', self.name)}"
    else:
      return f"{self.context}_{self.name}"

  async def update(self, request: Request, consume: UpdatePermissionRequest) -> OkResult:
    """Updates the permission's roles"""
    await super().update(request.app._models, **consume.to_dict())
    return self

@dataclass
class User(JsonSchemaMixin, Mongo, Tree, CanBeAuthenticated, HasEmail, HasName):
  """Represents the user"""

  __indexer__ = "email"
  __exclude__ = ["password"]

  def __post_init__(self):
    super().__post_init__()

    self.password = Auth.secure(self.password)

  async def index(self, request: Request) -> OkResult:
    """Returns the user's data"""
    ancestors = [ancestor.to_plain_dict() for ancestor in await self.ancestors(request.app._models)]
    return {"object": self.to_plain_dict(), "ancestors": ancestors}

@dataclass
class Project(JsonSchemaMixin, Mongo, Tree, CanBeRemovedWithFiles, ShouldEmitNewsAggregations, HasMessages, HasBacklog, HasFiles, HasStakeholders, HasPhases, HasTags, HasThemes, HasAreas, HasAddress, HasDeadline, ShouldBeRegistrable, HasCode, HasDescription, HasName):
  """Project"""
  __x_schema__ = {"form": ["name", "description", "code", "record", "deadline", "address", "areas", "themes", "tags"]}
  _encoder = yJSONEncoder

  async def index(self, request: Request) -> OkResult:
    """Returns the project's data"""
    ancestors = [ancestor.to_plain_dict() for ancestor in await self.ancestors(request.app._models)]
    files = await self.files_amount(request)
    return {"object": self.to_plain_dict(), "ancestors": ancestors, "files": files}

@dataclass
class Record(JsonSchemaMixin, Mongo, Tree, CanBeRemovedWithFiles, ShouldEmitNewsAggregations, HasMessages, HasBacklog, HasFiles, HasStakeholders, HasPhases, HasTags, HasThemes, HasAreas, HasAddress, HasDeadline, ShouldBeRegistrable, HasCode, HasDescription, HasName):
  """Record"""
  __x_schema__ = {"form": ["name", "description", "code", "record", "deadline", "address", "areas", "themes", "tags"]}
  _encoder = yJSONEncoder

  async def index(self, request: Request) -> OkResult:
    """Returns the record's data"""
    ancestors = [ancestor.to_plain_dict() for ancestor in await self.ancestors(request.app._models)]
    files = await self.files_amount(request)
    return {"object": self.to_plain_dict(), "ancestors": ancestors, "files": files}

@dataclass
class Phase(JsonSchemaMixin, Mongo, Tree, CanBeRemoved, ShouldBeFinished, HasName):
  """Project's phase"""

@dataclass
class Message(JsonSchemaMixin, Mongo, Tree, HasMessage, IsTemporalyMarked, FromUser):
  """Chat message"""

  __indexer__ = "date"

  def __sluger__(self, values = None, fields: bool = False) -> Union[Tuple, str]:
    if fields:
      return ("date",)
    elif values:
      return values.get('date', self.date)
    else:
      return self.date.isoformat()

@dataclass
class Backlog(JsonSchemaMixin, Mongo, Tree, HasAspect, HasPath, FromUser, IsTemporalyMarked):
  """Backlog entry"""

  __indexer__ = "date"

  def __sluger__(self, values = None, fields: bool = False) -> Union[Tuple, str]:
    if fields:
      return ("date",)
    elif values:
      return values.get('date', self.date)
    else:
      return self.date.isoformat()
