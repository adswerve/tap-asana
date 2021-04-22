import singer
from singer import utils
from tap_asana.context import Context
from tap_asana.streams.base import Stream
import time


class Stories(Stream):
    name = "stories"
    replication_key = "created_at"
    replication_method = 'INCREMENTAL'
    fields = [
        "gid",
        "resource_type",
        "created_at",
        "created_by",
        "resource_subtype",
        "text",
        "html_text",
        "is_pinned",
        "assignee",
        "dependency",
        "duplicate_of",
        "duplicated_from",
        "follower",
        "hearted",
        "hearts",
        "is_edited",
        "liked",
        "likes",
        "new_approval_status",
        "new_dates",
        "new_enum_value",
        "new_name",
        "new_number_value",
        "new_resource_subtype",
        "new_section",
        "new_text_value",
        "num_hearts",
        "num_likes",
        "num_subtasks",
        "old_approval_status",
        "old_dates",
        "old_enum_value",
        "old_name",
        "old_number_value",
        "old_resource_subtype",
        "old_section",
        "old_text_value",
        "preview",
        "project",
        "source",
        "story",
        "tag",
        "target",
        "task"
    ]
    start_time = None
    task_history = {}

    def get_objects(self):
        LOGGER = singer.get_logger()
        LOGGER.info("ATTENTION: Starting Stories Sync")

        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        opt_fields = ",".join(self.fields)

        # Refreshing token at the start of stories
        Context.asana.refresh_access_token()
        self.start_time = time.time()

        # Put all project ids into a list
        workspaces = self.call_api("workspaces")
        all_projects_gid = []
        for workspace in workspaces:
            projects = self.call_api("projects",
                                     workspace=workspace["gid"])  # only getting updates for non archived projects *****ADD BACK IN
            for project in projects:
                if int(project['gid']) < 1100634302647302:  # remove
                    continue
                all_projects_gid.append(project["gid"])

        LOGGER.info("Num projects to finish backfill:")  # remove
        LOGGER.info(len(all_projects_gid))  # remove

        # For every Project ID, get all tasks and subtasks, yield their stories
        for p_gid in all_projects_gid:

            LOGGER.info(p_gid)
            tasks = self.call_api("tasks", project=p_gid, opt_fields=opt_fields)

            task_dict = {}
            for task in tasks:
                self.timer_check()  # check if need to refresh token

                if task["gid"] in self.task_history.keys():  # if task has been check already
                    continue
                self.task_history[task["gid"]] = True
                task_dict[task["gid"]] = task["num_subtasks"]

                if self.is_bookmark_old(task["modified_at"]):  # if task has been modified, check for stories
                    for story in Context.asana.client.stories.get_stories_for_task(task_gid=task["gid"],
                                                                                   opt_fields=opt_fields):
                        session_bookmark = self.get_updated_session_bookmark(session_bookmark,
                                                                             story[self.replication_key])
                        if self.is_bookmark_old(story[self.replication_key]):
                            yield story

            all_subtasks_ids = []
            # If a project has tasks, look for subtasks
            if len(task_dict) > 0:
                self.get_all_tasks(task_dict, all_subtasks_ids)

            # If we found any subtasks for the given tasks, yield their stories
            if len(all_subtasks_ids) > 0:
                for task_id in all_subtasks_ids:
                    self.timer_check()
                    try:
                        subtask = Context.asana.client.tasks.find_by_id(task_id)
                        if subtask["gid"] not in self.task_history.keys():
                            self.task_history[task["gid"]] = True
                            if self.is_bookmark_old(subtask["modified_at"]):
                                for story in Context.asana.client.stories.get_stories_for_task(task_gid=subtask["gid"],
                                                                                               opt_fields=opt_fields):
                                    session_bookmark = self.get_updated_session_bookmark(session_bookmark,
                                                                                         story[self.replication_key])
                                    if self.is_bookmark_old(story[self.replication_key]):
                                        yield story

                    except Exception as e:
                        LOGGER.info("Skipping a subtask's stories, exception occurred")
                        LOGGER.info(e)
                        Context.asana.refresh_access_token()

        self.update_bookmark(session_bookmark)

    def get_all_tasks(self, task_dict, all_subtasks_ids):
        opt_fields = ",".join(self.fields)
        self.timer_check()
        for gid in task_dict.keys():
            # check if the task has subtasks
            if int(task_dict[gid]) == 0:
                continue
            temp_subtasks = {}
            subtasks = Context.asana.client.tasks.subtasks(gid, opt_fields=opt_fields)
            for subtask in subtasks:
                all_subtasks_ids.append(subtask["gid"])  # add subtask id to the full id list
                temp_subtasks[subtask["gid"]] = subtask["num_subtasks"]  # add id mapped to num_subtasks into dict

            # If there are any subtasks for this given task, call the function recursively and check for nested subtasks
            if len(temp_subtasks) > 0:
                self.get_all_tasks(temp_subtasks, all_subtasks_ids)

    def timer_check(self):
        LOGGER = singer.get_logger()
        if (time.time() - self.start_time) > 1800:
            LOGGER.info("ATTENTION: 30 min passed, refreshing token")
            Context.asana.refresh_access_token()
            self.start_time = time.time()


Context.stream_objects['stories'] = Stories
