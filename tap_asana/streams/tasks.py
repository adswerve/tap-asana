from singer import utils
import singer
from tap_asana.streams.base import invalid_token_handler
from tap_asana.context import Context
from tap_asana.streams.base import Stream
from oauthlib.oauth2 import TokenExpiredError
import time



class Tasks(Stream):
    name = 'tasks'
    replication_key = "modified_at"
    replication_method = 'INCREMENTAL'
    fields = [
        "gid",
        "resource_type",
        "name",
        "approval_status",
        "assignee_status",
        "completed",
        "completed_at",
        "completed_by",
        "created_at",
        "dependencies",
        "dependents",
        "due_at",
        "due_on",
        "external",
        "hearted",
        "hearts",
        "html_notes",
        "is_rendered_as_seperator",
        "liked",
        "likes",
        "memberships",
        "modified_at",
        "notes",
        "num_hearts",
        "num_likes",
        "num_subtasks",
        "resource_subtype",
        "start_on",
        "assignee",
        "custom_fields",
        "followers",
        "parent",
        "permalink_url",
        "projects",
        "tags",
        "workspace"
    ]
    start_time = None
    task_history = {}

    def get_objects(self):
        LOGGER = singer.get_logger()
        LOGGER.info("ATTENTION: Starting Tasks Sync")

        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
        opt_fields = ",".join(self.fields)

        # Refreshing token at the start of tasks
        Context.asana.refresh_access_token()
        self.start_time = time.time()

        # Put all project ids into a list
        workspaces = self.call_api("workspaces")
        all_projects_gid = []
        for workspace in workspaces:
            projects = self.call_api("projects", workspace=workspace["gid"], archived=False)
            for project in projects:
                all_projects_gid.append(project["gid"])

        # For every Project ID, get all tasks and subtasks
        for p_gid in all_projects_gid:
            tasks = self.call_api("tasks", project=p_gid, opt_fields=opt_fields)

            task_dict = {}
            for task in tasks:
                self.timer_check()  # check if need to refresh token

                if task["gid"] in self.task_history.keys():  # if task has been check already
                    continue
                self.task_history[task["gid"]] = True
                task_dict[task["gid"]] = task["num_subtasks"]

                session_bookmark = self.get_updated_session_bookmark(session_bookmark, task[self.replication_key])
                if self.is_bookmark_old(task[self.replication_key]):
                    yield task

            all_subtasks_ids = []
            # If a project has tasks, look for subtasks
            if len(task_dict) > 0:
                self.get_all_tasks(task_dict, all_subtasks_ids)
            if len(all_subtasks_ids) > 0:
                for task_id in all_subtasks_ids:
                    self.timer_check()
                    try:
                        subtask = Context.asana.client.tasks.find_by_id(task_id)
                        if subtask["gid"] not in self.task_history.keys():
                            self.task_history[subtask["gid"]] = True
                            session_bookmark = self.get_updated_session_bookmark(session_bookmark, subtask[self.replication_key])
                            if self.is_bookmark_old(subtask[self.replication_key]):
                                self.task_history[subtask["gid"]] = True
                                yield subtask
                    except Exception as e:
                        LOGGER.info("Skipping a subtask, exception occurred")
                        LOGGER.info(e)
                        Context.asana.refresh_access_token()

        self.update_bookmark(session_bookmark)

    def get_all_tasks(self, task_dict, all_subtasks_ids):
        opt_fields = ",".join(self.fields)
        self.timer_check()
        for gid in task_dict:
            if int(task_dict[gid]) == 0:
                continue
            temp_subtasks = {}
            subtasks = Context.asana.client.tasks.subtasks(gid, opt_fields=opt_fields)
            for subtask in subtasks:
                all_subtasks_ids.append(subtask["gid"])  # add subtask id to the full id list
                temp_subtasks[subtask["gid"]] = subtask["num_subtasks"]  # add subtask id to a list for this specific task

            # If there are any subtasks for this given task, call the function recursively and check for nested subtasks
            if len(temp_subtasks) > 0:
                self.get_all_tasks(temp_subtasks, all_subtasks_ids)

    def timer_check(self):
        LOGGER = singer.get_logger()
        if (time.time() - self.start_time) > 1800:
            LOGGER.info("ATTENTION: 30 min passed, refreshing token")
            Context.asana.refresh_access_token()
            self.start_time = time.time()

Context.stream_objects['tasks'] = Tasks
