
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream


class Projects(Stream):
  name = "projects"
  replication_key = "modified_at"
  replication_method = 'INCREMENTAL'
  fields = [
    "name",
    "gid",
    "owner",
    "current_status",
    "custom_fields",
    "default_view",
    "due_date",
    "due_on",
    "html_notes",
    "is_template",
    "created_at",
    "modified_at",
    "start_on",
    "archived",
    "public",
    "members",
    "followers",
    "color",
    "notes",
    "icon",
    "permalink_url",
    "workspace",
    "team"
  ]


  def get_objects(self):
    iter = 0
    bookmark = self.get_bookmark()
    session_bookmark = bookmark
    opt_fields = ",".join(self.fields)
    for workspace in Context.asana.client.workspaces.find_all():
      for project in Context.asana.client.projects.find_all(workspace=workspace["gid"], opt_fields=opt_fields):
        session_bookmark = self.get_updated_session_bookmark(session_bookmark, project[self.replication_key])
        iter += 1
        if iter > 5:
          Context.asana.refresh_access_token()
        if self.is_bookmark_old(project[self.replication_key]):
          yield project
    self.update_bookmark(session_bookmark)


Context.stream_objects["projects"] = Projects
