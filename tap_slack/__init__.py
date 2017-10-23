#!/usr/bin/env python3

import datetime
from datetime import timezone
import time

import os

import backoff
import requests
import singer

from singer import utils

CONFIG = {}
STATE = {}
written_schemas = set()

LOGGER = singer.get_logger()
session = requests.Session()
sync_start = datetime.datetime.utcnow().timestamp()

def write_schema(entity):
    if entity not in written_schemas:
        rel_path = "schemas/{}.json".format(entity)
        abs_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), rel_path)
        schema = utils.load_json(abs_path)
        singer.write_schema(entity, schema, ["id"])
        written_schemas.add(entity)

def get_next_cursor(body):
    return body.get("response_metadata", {}).get("next_cursor")

def get_start(key):
    start_date_str = STATE.get(key, CONFIG['start_date'])
    return utils.strptime(start_date_str).replace(tzinfo=timezone.utc).timestamp()

def update_bookmark(key, value):
    existing_bookmark = get_start(key)
    new_bookmark = max(existing_bookmark, value)
    LOGGER.info("update_bookmark, {}, {}, {}, {}, {}".format(key, value, existing_bookmark, new_bookmark, new_bookmark > existing_bookmark))
    if new_bookmark > existing_bookmark:
        STATE[key] = utils.strftime(datetime.datetime.fromtimestamp(new_bookmark).astimezone(timezone.utc))
        singer.write_state(STATE)

class Method(object):
    def __init__(self):
        self.params = {}

    # TODO: implement retry
    def request(self):
        url = "https://slack.com/api/{}".format(self.endpoint)
        params = {
            "token": CONFIG["token"]
        }
        req = requests.Request("GET", url, params={**self.params, **params}).prepare()
        LOGGER.info("GET {}".format(req.url))
        resp = session.send(req)

        if resp.status_code == 429:
            sleep_for_secs = float(resp.headers["Retry-After"])
            LOGGER.info("Got 429 response, waiting {}s before retrying. (Raw header: {})".format(sleep_for_secs, resp.headers["Retry-After"]))
            time.sleep(sleep_for_secs)
            return Method.request(self)

        if resp.status_code != 200:
            LOGGER.error("GET {}: [{} - {}]".format(req.url, resp.status_code, resp.content))
        resp.raise_for_status()

        body = resp.json()
        if not body["ok"]:
            raise Error("GET {}: response is not OK [{}]".format(req.url, resp.content))

        return body

class ChannelsMethod(Method):
    def __init__(self, cursor=None):
        super().__init__()
        self.endpoint = "conversations.list"
        self.params = {
            "cursor": cursor,
            "types": "public_channel,private_channel,mpim,im"
        }

    def request(self):
        body = super().request()
        LOGGER.info("Got {} channels".format(len(body["channels"])))

        additional_requests = []

        for channel in body["channels"]:
            if channel["is_im"]:
                write_schema("im")
                singer.write_record("im", channel)
                additional_requests.append(ChannelHistoryMethod(channel["id"]))
            else:
                additional_requests.append(ChannelWithMembersMethod(channel))

        cursor = get_next_cursor(body)
        if cursor:
            additional_requests.append(ChannelsMethod(cursor))

        return additional_requests

class ChannelWithMembersMethod(Method):
    def __init__(self, channel, additional_members=[], cursor=None):
        super().__init__()
        self.channel = channel
        self.additional_members = additional_members
        self.endpoint = "conversations.members"
        self.params = {
            "channel": channel["id"],
            "cursor": cursor
        }

    def request(self):
        body = super().request()
        members = self.additional_members + body["members"]

        additional_requests = []
        cursor = get_next_cursor(body)
        if cursor:
            additional_requests.append(ChannelWithMembersMethod(self.channel, additional_members=members, cursor=cursor))
        else:
            channel = {**self.channel, **{"members": members}}
            write_schema("conversation")
            singer.write_record("conversation", channel)
            additional_requests.append(ChannelHistoryMethod(channel["id"]))

        return additional_requests

class ChannelHistoryMethod(Method):
    def __init__(self, channel_id, oldest=None, cursor=None):
        super().__init__()
        self.bookmark_key = "conversation_history:{}".format(channel_id)
        self.oldest = oldest or get_start(self.bookmark_key)
        self.channel_id = channel_id
        self.endpoint = "conversations.history"
        self.params = {
            "channel": channel_id,
            "cursor": cursor,
            # oldest parameter is exclusive, so query from 1s earlier
            "oldest": self.oldest - 1
        }

    def request(self):
        body = super().request()
        LOGGER.info("got {} messages".format(len(body["messages"])))

        # Messages are returned newest-to-oldest. Reverse this so that the
        # emitted state is never newer than any messages that have not yet been
        # emitted.
        body["messages"].reverse()

        for message in body["messages"]:
            ts = float(message["ts"])
            additional_props = {
                "id": "{}_{}".format(self.channel_id, int(ts * 1000000)),
                "channelId": self.channel_id,
                "ts": ts
            }
            transformed_message = {**message, **additional_props}
            if ts >= self.oldest:
                write_schema("message")
                singer.write_record("message", transformed_message)
                update_bookmark(self.bookmark_key, ts)

        additional_requests = []
        cursor = get_next_cursor(body)
        if cursor:
            additional_requests.append(ChannelHistoryMethod(self.channel_id, oldest=self.oldest, cursor=cursor))

        return additional_requests

class EmojiMethod(Method):
    def __init__(self):
        super().__init__()
        self.endpoint = "emoji.list"
        self.params = {}

    def request(self):
        body = super().request()
        names = list(body["emoji"].keys())
        LOGGER.info("Got {} emoji".format(len(names)))

        for name in names:
            write_schema("emoji")
            singer.write_record("emoji", {
                "id": name,
                "name": name,
                "url": body["emoji"][name]
            })

# NOTE: ordering is unknown. Assume that response is unordered
class FilesMethod(Method):
    def __init__(self, ts_from=None, page=1):
        LOGGER.info("s {}".format(get_start("files")))
        ts_from = ts_from or get_start("files")
        super().__init__()
        self.endpoint = "files.list"
        self.page = 1
        self.ts_from = ts_from
        # Iterate in 1 week chunks
        self.ts_to = min(sync_start, ts_from + 7 * 24 * 60 * 60)
        self.params = {
            "page": self.page,
            "ts_from": self.ts_from,
            "ts_to": self.ts_to
        }

    def request(self):
        body = super().request()

        LOGGER.info("Got {} files".format(len(body["files"])))
        max_bookmark = 0

        for file in body["files"]:
            write_schema("file")
            singer.write_record("file", file)
            max_bookmark = max(max_bookmark, file["created"])

        additional_requests = []
        write_boomark = True # don't want to do this until we finish the chunk
        if body["paging"]["page"] < body["paging"]["pages"]:
            additional_requests.append(FilesMethod(self.ts_from, page=self.page+1))
            write_boomark = False
        elif self.ts_to < sync_start:
            additional_requests.append(FilesMethod(self.ts_to))

        if write_boomark and len(body["files"]) > 0:
            update_bookmark("files", max_bookmark)

        return additional_requests

class TeamInfoMethod(Method):
    def __init__(self):
        super().__init__()
        self.endpoint = "team.info"

    def request(self):
        body = super().request()
        write_schema("team")
        singer.write_record("team", body["team"])

class UserGroupsMethod(Method):
    def __init__(self):
        super().__init__()
        self.endpoint = "usergroups.list"

    def request(self):
        body = super().request()
        LOGGER.info("Got {} usergroups".format(len(body["usergroups"])))

        additional_requests = []

        for usergroup in body["usergroups"]:
            additional_requests.append(UserGroupWithUsersMethod(usergroup))

        return additional_requests

class UserGroupWithUsersMethod(Method):
    def __init__(self, usergroup):
        super().__init__()
        self.endpoint = "usergroups.users.list"
        self.usergroup = usergroup
        self.params = {
            "usergroup": usergroup["id"]
        }

    def request(self):
        body = super().request()
        write_schema("usergroup")
        singer.write_record("usergroup", {**self.usergroup, **{"users": body["users"]}})

class UsersMethod(Method):
    def __init__(self, cursor=None):
        super().__init__()
        self.endpoint = "users.list"
        self.params = {
            "cursor": cursor,
            "include_locale": True
        }

    def request(self):
        body = super().request()
        LOGGER.info("Got {} users".format(len(body["members"])))

        additional_requests = []

        for user in body["members"]:
            write_schema("user")
            singer.write_record("user", user)

        cursor = get_next_cursor(body)
        if cursor:
            additional_requests.append(UsersMethod(cursor))

        return additional_requests

def do_sync():
    LOGGER.info("Authenticating")
    LOGGER.info(CONFIG)

    queue = []
    queue.append(ChannelsMethod())
    queue.append(EmojiMethod())
    queue.append(FilesMethod())
    queue.append(TeamInfoMethod())
    queue.append(UserGroupsMethod())
    queue.append(UsersMethod())

    while len(queue) > 0:
        method = queue.pop()
        LOGGER.info("Processing {} ({}), {} requests pending".format(method.endpoint, method.params, len(queue)))
        additional_requests = method.request()
        if additional_requests:
            queue = additional_requests + queue

    LOGGER.info("Completed sync")

def main_impl():
    args = utils.parse_args(["token", "start_date"])
    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == '__main__':
    main()
