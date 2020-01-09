from datetime import datetime
from typing import Any, List
from dataclasses import dataclass, field

from dataclasses_jsonschema import JsonSchemaMixin, JsonSchemaMeta

from yrest.tree import Email, Password
from yrest.auth import generate_password_hash

@dataclass
class UpdatePermissionRequest(JsonSchemaMixin):
  roles: List[str] = field(metadata = JsonSchemaMeta(title = "Select no roles give access to everyone", extensions = {"label": False, "placeholder": "Select the roles"}))

@dataclass
class TransferRoleRequest(JsonSchemaMixin):
  owner: Email
  newOwner: Email
  role: str

@dataclass
class DelegationRequest(JsonSchemaMixin):
  email: Email
  role: str

@dataclass
class ChangePasswordRequest(JsonSchemaMixin):
  old: Password = field(metadata = JsonSchemaMeta(extensions = {"label": "Old password"}))
  new: Password = field(metadata = JsonSchemaMeta(extensions = {"label": "New password"}))

  def __post_init__(self):
    if self.old == self.new:
      raise TypeError("The new password and the old one must be different")
    self.new = generate_password_hash(self.new)

@dataclass
class UploadFilesRequest(JsonSchemaMixin):
  files: Any

@dataclass
class SearchRequest(JsonSchemaMixin):
  search: str = None
  start_date: datetime = None
  end_date: datetime = None

@dataclass
class GetFileRequest(JsonSchemaMixin):
  filename: str
