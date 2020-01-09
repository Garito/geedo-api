from pathlib import PurePath
from typing import Any, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from sanic.request import Request
from sanic.exceptions import Unauthorized, NotFound

from dataclasses_jsonschema import JsonSchemaMeta, JsonSchemaMixin, ValidationError

from yrest.tree import Tree, Email, Password, Phone
from yrest.mongo import Mongo
from yrest.ysanic import ySanic
from yrest.auth import check_password_hash
from yrest.utils import Ok, OkResult, OkListResult, ErrorMessage, get_parents_urls

from parameters import TransferRoleRequest, DelegationRequest, ChangePasswordRequest, UploadFilesRequest, SearchRequest, GetFileRequest

@dataclass
class HasInvitations:
  invitations: List[str] = field(default_factory = list, metadata = {"model": "Invitation"})

  async def get_invitations(self, request: Request) -> OkListResult:
    """Returns the list of invitations"""
    result = await self.children([request.app._models.Invitation])
    return {child.slug: child.to_plain_dict() for child in result["invitations"]}

@dataclass
class HasUsers:
  users: List[str] = field(default_factory =  list, metadata = {"model": "User"})

  async def get_users(self, request: Request) -> OkListResult:
    """Returns the list of users"""
    result = await self.children([request.app._models.User])
    return {child.slug: child.to_plain_dict() for child in result["users"]}

@dataclass
class DefinesSecurity:
  roles: List[str] = field(default_factory = list, metadata = {"model": "Role"})
  permissions: List[str] = field(default_factory = list, metadata = {"model": "Permission"})

  async def _rebuild_sec(self, app: ySanic):
    """Allows to maintain the security inline with the code"""
    await self._rebuild_perms(app)
    await self._rebuild_roles(app)

  async def _rebuild_perms(self, app: ySanic):
    """Allows to sync source code with its permissions objects in the database"""
    src_perms = set()
    for model, data in app._introspection.items():
      for member in app._introspection[model].keys():
        if member != "factories":
          src_perms.add(f"{model}/{member}")

      if "factories" in data.keys():
        for factory in data["factories"]:
          src_perms.add(f"{model}/create_{factory.lower()}")

    perms = await self.children([app._models.Permission])
    perms = {f"{perm.context}/{perm.name}": perm for perm in perms["permissions"]}
    perms_set = set(perms.keys())

    for perm in (src_perms - perms_set):
      parts = perm.split("/")
      name = parts.pop()
      context = "/".join(parts)
      roles = [] if perm in app.config["OPEN_ENDPOINTS"] else ["admin"]
      perm_obj = app._models.Permission(name = name, context = context, roles = roles)
      await self.create_child(perm_obj, app._models)

    for perm in (perms_set - src_perms):
      parts = perm.split("/")
      name = parts.pop()
      context = "/".join(parts)
      perm_obj = await app._models.Permission.get(self._table, path = self.get_url(), name = name, context = context)
      await perm_obj.delete(app._models)

  async def _rebuild_roles(self, app: ySanic):
    """Allows to sync system roles"""
    roles = await self.children([app._models.Role])
    system_roles = app.config["SYSTEM_ROLES"].copy()

    for role in roles["roles"]:
      if role.name in system_roles.keys():
        system_roles.pop(role.name)

    for name, data in system_roles.items():
      role = app._models.Role(name = name, description = data["description"], system = True, system_only = data.get("system_only", False))
      await self.create_child(role, app._models)

  async def rebuild_sec(self, request: Request) -> Ok:
    """Allows to rebuild the permissions and roles"""
    self._rebuild_sec(request.app)

  async def get_roles(self, request: Request) -> OkListResult:
    """Returns the list of roles"""
    result = await self.children([request.app._models.Role])
    return {child.slug: child.to_plain_dict() for child in result["roles"]}

  async def get_permissions(self, request: Request) -> OkListResult:
    """Returns the list of permissions"""
    result = await self.children([request.app._models.Permission])
    return {f"{child.context}/{child.name}": child.to_plain_dict() for child in result["permissions"]}

@dataclass
class HasDescription:
  description: str = field(metadata = JsonSchemaMeta(maxLength = 5000, extensions = {"label": "Description"}))

@dataclass
class HasName:
  name: str = field(metadata = JsonSchemaMeta(extensions = {"label": "Name"}))

@dataclass
class HasPhone:
  phone: Phone

@dataclass
class HasNIF:
  nif: str = field(metadata = JsonSchemaMeta(extensions = {"label": "NIF"}))

@dataclass
class SystemNeedsIt:
  system: bool = False

@dataclass
class UsedBySystemOnly:
  system_only: bool = False

@dataclass
class CanBeRemoved:
  async def remove(self, request: Request, actor: "User") -> OkResult:
    """Removes the paper"""
    result = await request.app._generic_remover(request, self, actor)

    perm_url = f"@{self.get_url()}$"
    await self._table.update_many({"roles": {"$regex": perm_url}}, {"$pull": {"roles": {"$regex": perm_url}}})

    return result

@dataclass
class CanBeRemovedWithFiles(CanBeRemoved):
  async def remove(self, request: Request, actor: "User") -> OkResult:
    """Remove the paper and its files"""
    async for file in request.app._gridfs.find({"filename": {"$regex": f"^{self.get_url()}"}}):
      await request.app._gridfs.delete(file._id)

    result = await super().remove(request, actor)

    return result

@dataclass
class HasContext:
  context: str

@dataclass
class HasRoles:
  roles: List[str] = field(default_factory = list)

  async def allows(self, actor, context) -> bool:
    return not self.roles or (actor and set(actor.get_roles(context)) & set(self.roles))

@dataclass
class HasEmail:
  email: Email = field(metadata = JsonSchemaMeta(extensions = {"label": "Email"}))

@dataclass
class CanBeAuthenticated:
  password: Password = None
  roles: List[str] = field(default_factory = list)

  def get_roles(self, context):
    roles = self.roles.copy()
    if self == context:
      roles.append("owner")

    urls = get_parents_urls(context.get_url())
    if urls:
      for url in urls:
        for role in self.roles:
          if role.endswith(url):
            roles.append(role.split('@')[0])
            break

    return roles

  async def change_password(self, request: Request, consume: ChangePasswordRequest) -> Ok:
    """Allows to change the actor's password"""
    if not check_password_hash(self.password, consume.old):
      return ErrorMessage("Bad old password", 401)

    await self.update(request.app._models, password = consume.new)

@dataclass
class HasProjects:
  projects: List[str] = field(default_factory = list, metadata = {"model": "Project"})

  async def get_projects(self, request: Request, actor) -> OkResult:
    """Returns the list of projects"""
    children = await self.children([request.app._models.Project], sort = {"Project": {"$sort": {"record": 1}}})
    result = {}
    for child in children["projects"]:
      obj = child.to_plain_dict()
      child._table = self._table
      url = child.get_url()
      obj["stats"] = {}
      obj["stats"]["files"] = len(await request.app._gridfs.find({"filename": {"$regex": f"^{url}"}}).to_list(None))
      obj["stats"]["messages"] = len(await request.app._table.find({"type": "Message", "path": {"$regex": f"^{url}"}}).to_list(None))
      obj["stats"]["activity"] = len(await request.app._table.find({"type": "Backlog", "runned_path": {"$regex": f"^{url}"}}).to_list(None))
      obj["stats"]["phases"] = await child.phases_stats(request)
      result[child.slug] = obj

    return result

@dataclass
class HasRecords:
  records: List[str] = field(default_factory = list, metadata = {"model": "Record"})

  async def get_records(self, request: Request) -> OkResult:
    """Returns the list of records"""
    from sanic.log import logger
    children = await self.children([request.app._models.Record], sort = {"Record": {"$sort": {"record": 1}}})
    result = {}
    for child in children["records"]:
      obj = child.to_plain_dict()
      child._table = self._table
      url = child.get_url()
      obj["stats"] = {}
      obj["stats"]["files"] = len(await request.app._gridfs.find({"filename": {"$regex": f"^{url}"}}).to_list(None))
      obj["stats"]["messages"] = len(await request.app._table.find({"type": "Message", "path": {"$regex": f"^{url}"}}).to_list(None))
      obj["stats"]["activity"] = len(await request.app._table.find({"type": "Backlog", "runned_path": {"$regex": f"^{url}"}}).to_list(None))
      obj["stats"]["phases"] = await child.phases_stats(request)
      result[child.slug] = obj

    return result

@dataclass
class HasCode:
  code: str = field(metadata = JsonSchemaMeta(extensions = {"label": "Code"}))

@dataclass
class HasPhases:
  phases: List[str] = field(default_factory = list, metadata = {"model": "Phase"})

  async def get_phases(self, request: Request) -> OkResult:
    """Return the project's phases"""
    result = await self.children([request.app._models.Phase])
    return {child.slug: child.to_plain_dict() for child in result["phases"]}

  async def phases_stats(self, request: Request) -> OkResult:
    """Returns the total and finished phases"""
    match = {"$match": {"type": "Phase", "path": self.get_url()}}
    group = {"$group": {"_id": None, "total": {"$sum": 1}, "finished": {"$sum": {"$cond": ["$finished", 1, 0]}}}}
    stats = await request.app._table.aggregate([match, group]).to_list(None)
    if len(stats):
      return {"total": stats[0]["total"], "finished": stats[0]["finished"]}
    else:
      return {"total": 0, "finished": 0}

@dataclass
class ShouldBeRegistrable:
  record: datetime = field(metadata = JsonSchemaMeta(extensions = {"label": "Record"}))

@dataclass
class HasDeadline:
  deadline: datetime = field(metadata = JsonSchemaMeta(extensions = {"label": "Deadline"}))

@dataclass
class HasAddress:
  address: str = field(metadata = JsonSchemaMeta(extensions = {"label": "Address", "format": "GeoAddress"}))

  async def get_near(self, request: Request) -> OkResult:
    """Returns projects and records near by the address"""
    pass

@dataclass
class HasTags:
  tags: List[str] = field(default_factory = list, metadata = JsonSchemaMeta(extensions = {"label": "Tags", "options": {"opId": "Root/get_tags", "taggable": True}}))

# @dataclass
# class HasThemes:
#   themes: List[str] = field(default_factory = list, metadata = JsonSchemaMeta(extensions = {"label": "Themes", "options": {"opId": "Root/get_themes", "taggable": True}}))

# @dataclass
# class HasAreas:
#   areas: List[str] = field(default_factory = list, metadata = JsonSchemaMeta(extensions = {"label": "Areas", "options": {"opId": "Root/get_areas", "taggable": True}}))

@dataclass
class HasStakeholders:
  async def get_stakeholders(self, request: Request) -> OkResult:
    """Returns the project's stakeholders"""
    my_url = self.get_url()
    stakeholders = await request.app._models.User.gets(self._table, roles = {"$regex": f"@{my_url}$"})
    result = {}
    for stakeholder in stakeholders:
      for rolepath in stakeholder.roles:
        if rolepath.endswith(f'@{my_url}'):
          role = rolepath.split('@')[0]
          if role not in result.keys():
            result[role] = []
          result[role].append(stakeholder)

    return result

  async def transfer_role(self, request: Request, actor: "User", consume: TransferRoleRequest) -> OkListResult:
    """Transfer a role from the actor to the provided user"""
    url = self.get_url()
    role = f"{consume.role}@{url}"
    owner = await request.app._models.User.get(self._table, email = consume.owner)
    if role not in owner.roles:
      raise Unauthorized(f"{owner.name} has not {consume.role} @ {url}")

    newOwner = await request.app._models.User.get(self._table, email = consume.newOwner)
    if role in newOwner.roles:
      raise ValidationError(f"{newOwner.name} has already {consume.role} @ {url}")

    newOwner_roles = newOwner.roles
    newOwner_roles.append(role)
    owner_roles = owner.roles
    owner_roles.pop(owner_roles.index(role))

    async with await self._table.database.client.start_session() as s:
      async with s.start_transaction():
        await newOwner.update(request.app._models, roles = newOwner_roles)
        await owner.update(request.app._models, roles = owner_roles)

    return {"newOwner": newOwner_roles, "exOwner": owner_roles}

  async def give_role(self, request: Request, consume: DelegationRequest) -> OkResult:
    """Gives a role to the user in this position"""
    user = await request.app._models.User.get(self._table, email = consume.email)
    if not user:
      raise NotFound("The user can't be found")

    roles = user.roles
    roles.append(f"{consume.role}@{self.get_url()}")
    await user.update(request.app._models, roles = roles)

    return roles

  async def withdraw_role(self, request: Request, consume: DelegationRequest) -> OkListResult:
    """Removes a role from the user in this position"""
    user = await request.app._models.User.get(self._table, email = consume.email)
    if not user:
      raise NotFound("The user can't be found")

    roles = user.roles
    roles.remove(f"{consume.role}@{self.get_url()}")
    await user.update(request.app._models, roles = roles)

    return roles

@dataclass
class HasFiles:
  async def get_files(self, request: Request) -> OkResult:
    """Returns the files' url list"""
    stream = None
    content_type = None
    files = {}
    async for file in request.app._gridfs.find({"filename": {"$regex": f"^{self.get_url()}"}}):
      content_type = file.metadata["contentType"]
      stream = await file.read()
      files[file.name] = {"stream": stream, "content_type": content_type}

    return files

  async def upload_files(self, request: Request, consume: UploadFilesRequest) -> OkResult:
    """Allows to upload multiple files"""
    url = self.get_url()
    result = {}
    for file in consume.files:
      file_url = f"{url}/{file['name']}"
      metadata = {"contentType": file["content_type"], "parent": url}
      await request.app._gridfs.upload_from_stream(filename = file_url, source = file["data"].encode("UTF-8"), metadata = metadata)
      result[file_url] = {"content_type": file["content_type"], "stream": file["data"]}
    return result

  async def files_amount(self, request: Request) -> int:
    """Returns the number of files"""
    files = await request.app._gridfs.find({"filename": {"$regex": f"^{self.get_url()}"}}).to_list(None)
    return len(files)

@dataclass
class AggregatesFiles:
  async def files_by_project(self, request: Request) -> OkResult:
    """Returns the files by project"""
    stream = None
    content_type = None
    files = {}

    async for file in request.app._gridfs.find({"filename": {"$regex": f"^{self.get_url()}"}}).sort("uploadDate", -1):
      content_type = file.metadata["contentType"]
      stream = await file.read()
      if file.metadata["parent"] not in files.keys():
        parent = await self._table.find_one(self.__class__._decompose_url(file.metadata["parent"]))
        parentObj = getattr(request.app._models, parent["type"])(**parent)
        files[file.metadata["parent"]] = {"obj": parentObj.to_plain_dict(), "files": {}}
      files[file.metadata["parent"]]["files"][file.name] = {"stream": stream, "content_type": content_type}

    return files

@dataclass
class IsSearchable:
  async def search(self, request: Request, consume: SearchRequest) -> OkResult:
    """Allows to search for contents"""
    if consume.search:
      search_query = {
        "multi_match": {
          "fields": ["name", "description", "code", "address", "areas", "themes", "tags", "filename" ],
          "query": consume.search,
          "fuzziness": "AUTO"
        }
      }
    else:
      search_query = None

    if consume.start_date and consume.end_date:
      date_query = {
        "range": {
          "record": {"gte": consume.start_date + 'T00:00:00', "lte": consume.end_date + 'T23:59:59'}
        }
      }
    elif consume.start_date:
      date_query = {
        "range": {
          "record": {"gte": consume.start_date + 'T00:00:00', "lte": consume.start_date + 'T23:59:59'}
        }
      }
    else:
      date_query = None

    if search_query and date_query:
      query = {
        "query": {
          "bool": {
            "must": search_query,
            "filter": [ date_query ]
          }
        }
      }
    elif search_query:
      query = {
        "query": search_query
      }
    elif date_query:
      query = {
        "query": date_query
      }

    result = await request.app._es.search(index = "gd.gd,gd.fs.files", body = query)
    return result["hits"]

  async def search_file(self, request: Request, consume: GetFileRequest) -> OkResult:
    """Returns the file by its name"""
    from sanic.log import logger
    async for file in request.app._gridfs.find({"filename": consume.filename}):
      parentUrl = PurePath(file.metadata["parent"])
      parentDoc = await request.app._table.find_one({"path": str(parentUrl.parent), "slug": parentUrl.name})
      parent = getattr(request.app._models, parentDoc["type"])(**parentDoc)
      stream = await file.read()
      return {"filename": file.filename, "content_type": file.metadata["contentType"], "stream": stream, "parent": parent.to_plain_dict()}
    else:
      return None
@dataclass
class FromUser:
  user: Email

@dataclass
class IsTemporalyMarked:
  date: datetime

@dataclass
class HasMessages:
  messages: List[str] = field(default_factory = list, metadata = {"model": "Message"})

  async def get_messages(self, request: Request) -> OkListResult:
    """Returns the list of messages"""
    result = await self.children([request.app._models.Message])
    return [child.to_plain_dict() for child in result["messages"]]
    # return {child.slug: child.to_plain_dict() for child in result["messages"]}

@dataclass
class AggregatesMessages:
  async def msgs_by_project(self, request: Request) -> OkResult:
    """Returns the messages aggregate by project"""
    msgs = await request.app._models.Message.gets(self._table, path = {"$regex": f"^{self.get_url()}"}, sort = [("date", -1)])
    result = {}
    for msg in msgs:
      if msg.path not in result.keys():
        parent = await self._table.find_one(self.__class__._decompose_url(msg.path))
        parentObj = getattr(request.app._models, parent["type"])(**parent)
        result[msg.path] = {"obj": parentObj.to_plain_dict(), "msgs": {}}
      result[msg.path]["msgs"][msg.get_url()] = msg
    return result

@dataclass
class HasMessage:
  message: str

@dataclass
class ShouldBeFinished:
  finished: datetime = None

  async def finish(self, request: Request, actor: "User") ->OkResult:
    """Mark as finished"""
    await self.update(request.app._models, finished = datetime.utcnow())

    roles = actor.roles
    roles.append(f"finisher@{self.get_url()}")
    await actor.update(request.app._models, roles = roles)

    return {"finished": self.finished.isoformat(), "finisher": actor.slug}

@dataclass
class HasBacklog:
  async def get_logs(self, request: Request) -> OkListResult:
    """Returns the backlog's entries"""
    docs = await self._table.find({"type": "Backlog", "runned_path": {"$regex": f"^{self.get_url()}"}}).sort("date", -1).to_list(250)
    backlog = request.app._models.Backlog
    result = [backlog(**doc) for doc in docs]
    return result

@dataclass
class HasPath:
  runned_path: str = ''

class Aspect(Enum):
  AUTH = "auth"
  UPDATER = "updater"
  DISPATCHER = "dispatcher"
  FACTORY = "factory"
  REMOVER = "remover"

@dataclass
class HasAspect:
  aspect: Aspect = Aspect.DISPATCHER

class ShouldEmitNewsAggregations:
  async def get_news(self, request: Request, actor) -> OkResult:
    """Returns the aggregation of the new activity"""
    last = await request.app._models.Backlog.get(self._table, aspect = "auth", user =  actor.email, sort = [("date", -1)])

    if last:
      url = self.get_url()
      # files = await request.app._gridfs.find({"filename": {"$regex": f"^{url}"}, "uploadDate": {"$gte": last.date}}).to_list(None)
      files = await request.app._gridfs.find({"filename": {"$regex": f"^{url}"}}).to_list(None)
      messages = await request.app._table.find({"type": "Message", "path": {"$regex": f"^{url}"}, "date": {"$gte": last.date}}).to_list(None)
      activity = await request.app._table.find({"type": "Backlog", "runned_path": {"$regex": f"^{url}"}, "date": {"$gte": last.date}}).to_list(None)
    else:
      files = []
      messages = []
      activity = []

    return {"files": len(files), "messages": len(messages), "activity": len(activity)}

@dataclass
class UpdateRequest(JsonSchemaMixin, HasTags, HasAddress, HasDeadline, ShouldBeRegistrable, HasCode, HasDescription):
  pass

@dataclass
class CanBeUpdated:
  pass

@dataclass
class IsCancelable:
  canceled: datetime = None

  async def cancel(self, request: Request) -> OkResult:
    """Cancels the paper"""
    data = {"canceled": datetime.utcnow()}
    await super(Mongo, self).update(request.app._models, **data)
    return data

  async def reopen(self, request: Request):
    """Reopens the paper"""
    await super(Mongo, self).update(request.app._models, canceled = None)

@dataclass
class HasRequester:
  requester: str = field(default = None, metadata = {"model": "Requester"})

@dataclass
class HasRequesterType:
  reqType: str = None

@dataclass
class HasRequesterSubtype:
  subtype: str = None

@dataclass
class HasDepartment:
  department: str = field(metadata = JsonSchemaMeta(extensions = {"label": "Department", "options": {"opId": "Root/get_departments", "model": "Department"}}))

@dataclass
class ShouldBeResolved:
  resolution: str = field(default = '', metadata = JsonSchemaMeta(maxLength = 5000, extensions = {"label": "Resolution"}))
