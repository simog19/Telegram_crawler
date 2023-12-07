##### DISCLAIMER:
##### CODE MODULARITY TO IMPROVE
##### CODE DOCUMENTATION - WORK IN PROGRESS

import multiprocessing
import asyncio
import random
from datetime import datetime
import re
import pymongo
import telethon
from telethon import TelegramClient
# from telethon.tl.functions.bots import GetBotInfoRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Channel, Dialog

n_processes = 2

api_ids = ['', '']
api_hashes = ['', '']
clients = []


def update_edges(edges_collection, entity_id, group_id):
    if edges_collection.find_one({"dest": entity_id}):
        edges_collection.update_one({"dest": entity_id}, {"$push": {"sources": group_id}})
    else:
        edges_collection.insert_one({"dest": entity_id, "sources": []})
        edges_collection.update_one({"dest": entity_id}, {"$push": {"sources": group_id}})


async def process_link(link_ext, id, client, l, result):
    new_links = set()
    gathered = []
    gathered_ext = []
    async for dialog in client.iter_dialogs():  # itering on all entries chat/groups/channel/bot
        if dialog.entity.id == id:
            new_links = new_links.union(await gather_links(dialog, client, gathered, gathered_ext, result))

            result['new_links'] = list_to_json(new_links)
            result['link_gathered'] = gathered
            result['group_id'] = id
            result['link'] = link_ext
            result['data'] = await collect_data(dialog, link_ext, client, result['p_id'])


async def gather_links(dialog: Dialog, client, gathered, gathered_ext, result):
    l = set()
    try:
        # TODO: per link esterni cambiare regex oppure provare due cicli for: 1 per ricerca link esterni  e uno per ricerca link interni
        async for m in client.iter_messages(dialog.entity.id, search="t.me/", limit=1000000):
            try:
                # provare su un messaggio con due o piu link - findall non funziona a quanto pare
                # REGEX for external link = re.search(
                # '/(https:\/\/www\.|http:\/\/www\.|https:\/\/|http:\/\/)?[a-zA-Z]{2,}(\.[a-zA-Z]{2,})(\.[a-zA-Z]{2,})?\/[a-zA-Z0-9]{2,}|((https:\/\/www\.|http:\/\/www\.|https:\/\/|http:\/\/)?[a-zA-Z]{2,}(\.[a-zA-Z]{2,})(\.[a-zA-Z]{2,})?)|(https:\/\/www\.|http:\/\/www\.|https:\/\/|http:\/\/)?[a-zA-Z0-9]{2,}\.[a-zA-Z0-9]{2,}\.[a-zA-Z0-9]{2,}(\.[a-zA-Z0-9]{2,})?/',
                # m.text).group()
                link = re.search('(https\:\/\/)?t\.me\/\+?[a-zA-Z0-9\.\&\/\?\:@\-_=#]*', m.text)
                if link is not None:
                    link = link.group()
                else:
                    continue
                # check if its is the link to a message
                if is_message(link) is True:
                    continue
                elif is_proxy(link) is True:
                    # print("[WORKER n."+str(result['p_id'])+"] DISCARDED: this is a proxy link: " + link)
                    continue
                else:  # if not add it to our set
                    l.add(link)
                    skip = False
                    # removing duplicates
                    for s in gathered:
                        if s["link_hash"] == str(link) and s["group_id"] == dialog.entity.id:
                            skip = True
                            break
                    if skip is False:
                        s = {
                            "link_hash": link,
                            "message": m.to_dict(),
                            "group_id": dialog.entity.id,
                            "group_name": dialog.title,
                            "date": datetime.now()  # to understand when it has been gathered
                        }
                        gathered.append(s)
                    else:
                        pass

            except AttributeError as e:
                print(
                    str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]---[!] from gather_links:" + str(e))
                pass
        print(str(datetime.now()) + " - [WORKER n." + str(
            result['p_id']) + "]---[] Links collected succesfully in: " + str(dialog.entity.id))
    except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
        print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]---[!] from gather_links:" + str(e))
        return l
    except TypeError as e:
        print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]---[!] from gather_links:" + str(e))
    return l


def is_message(link):
    # Define a regular expression pattern to match Telegram message links
    pattern = r't.me/.*?/(\d+)'

    # Use re.search to find the pattern in the link
    match = re.search(pattern, link)

    # Check if a match is found
    if match:
        # Extract the message ID from the match
        message_id = match.group(1)
        return True
    else:
        return False


def is_proxy(link):
    # Define a regular expression pattern to match Telegram message links
    pattern = r't.me/proxy\?'

    # Use re.search to find the pattern in the link
    match = re.search(pattern, link)

    # Check if a match is found
    if match:
        return True
    else:
        return False


async def collect_data(dialog: Dialog, link, client, pid):
    # Collect data of the group: name, messages, list of members
    group = dialog.entity
    d = {}
    if type(group) == telethon.tl.types.Channel or type(group) == telethon.tl.types.Chat:
        members = []
        messages = []
        try:
            if group.id != None and group.broadcast != True:
                async for m in client.iter_participants(dialog.id, limit=2000):
                    members.append(m.to_dict())
            if group.id != None:
                # change limit according to how many messages have to be saved
                async for m in client.iter_messages(dialog.id,
                                                    limit=500):  # TODO mettere max_age di 2 mesi, inserire parametri in iter messages
                    messages.append(m.to_dict())  # take the entire object m, instead of m.message
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
            print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] Data collection failed: " + str(e))

        username = group.username
        # print(username)
        d = {
            "id": str(group.id),
            "name": group.title,
            "username": username,
            "link_hash": link,
            "date": str(group.date),
            "date_inserted": datetime.now(),
            "is_scam": str(group.scam),
            "members": members,
            "messages": messages
        }

    print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[] Data collected succesfully in: " + str(
        dialog.entity.id))
    return d


async def join_group_by_hash(link_ext, link_hash, client, result):
    pid = result['p_id']
    try:
        g = await client(ImportChatInviteRequest(link_hash))
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[+] [HASH] Joined in: " + str(
            g.chats[0].title) + " " + str(link_hash))
    except telethon.errors.rpcerrorlist.InviteHashExpiredError as e:
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        print(str(datetime.now()) + " - [WORKER n." + str(
            pid) + "]---Trying with join_group_public_by_link (JoinChannelRequest)")
        g = await join_group_public_by_link(link_ext, link_hash, client, pid, result)
        return g
    except telethon.errors.rpcerrorlist.UserAlreadyParticipantError as e:
        result[
            'code'] = "JOIN_FAILED"
        result['error_messages'] = str(e)
        result['time'] = datetime.now()
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        return None
    except telethon.errors.rpcerrorlist.PeerIdInvalidError as e:
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        result['code'] = "JOIN_FAILED"
        result['error_messages'] = str(e)
        result['time'] = datetime.now()
        return None
    except telethon.errors.rpcerrorlist.FloodWaitError as e:
        wait_l = [int(word) for word in str(e).split() if word.isdigit()]
        wait = ''
        for digit in wait_l:
            wait += str(digit)
        print(str(datetime.now()) + " - [WORKER n." + str(
            pid) + "]---[!] Flood Error: Waiting for " + wait + " seconds - " + str(datetime.now()))
        await asyncio.sleep(int(wait) + 10)
        g = await join_group_by_hash(link_ext, link_hash, client, result)
        return g
    except telethon.errors.rpcerrorlist.InviteRequestSentError as e:
        result['code'] = "REQUEST_SENT"
        result['error_messages'] = str(e)
        result['time'] = datetime.now()
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        return None
    except BaseException as e:
        result['code'] = "JOIN_FAILED"
        result['error_messages'] = str(e)
        result['time'] = datetime.now()
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!]: " + str(datetime.now()) + str(e))
        return None

    return g


async def join_group_public_by_link(link_ext, link, client, pid, result):
    try:
        g = await client(JoinChannelRequest(link))
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[+] [PUBLIC] Joined in: " + str(
            g.chats[0].title) + " " + str(link))

    except telethon.errors.ChannelInvalidError as e:
        result['code'] = "JOIN_FAILED"
        result['error_messages'] = str(e)
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        return None
    except telethon.errors.ChannelPrivateError as e:
        result['code'] = "JOIN_FAILED"
        result['error_messages'] = str(e)
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        return None
    except telethon.errors.FloodWaitError as e:
        wait_l = [int(word) for word in str(e).split() if word.isdigit()]
        wait = ''
        for digit in wait_l:
            wait += str(digit)
        print(str(datetime.now()) + " - [WORKER n." + str(
            pid) + "]---[!] Flood Error: Waiting for " + wait + " seconds -: " + str(datetime.now()))
        await asyncio.sleep(int(wait) + 10)
        g = await join_group_public_by_link(link_ext, link, client, pid, result)
        return g
    except BaseException as e:
        result['code'] = "JOIN_FAILED"
        result['error_messages'] = str(e)
        print(str(datetime.now()) + " - [WORKER n." + str(pid) + "]---[!] " + str(e))
        return None

    return g


async def leave_group(e, client, result):  # missing link_ext param
    try:

        id = e['id']
        await client.delete_dialog(id)
        result['code'] = "LEAVE_SUCCESS"
        result["time"] = datetime.now()

    except BaseException as err:
        print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]---[!] Leaving [" + str(
            e['link_hash']) + "] Failed: " + str(err))
        result['code'] = "LEAVE_FAILED"
        result['error_messages'] = str(err)
        result['time'] = e['time_joined']


def list_to_json(links_list):
    n = []
    for el in links_list:
        n.append({"link_hash": str(el), "process_id": None, "state": "tbp"})
    return n


async def bot_interaction(link_extended, param, l, entity, client, result):
    print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "] The link under evaluation is a BOT")
    # send message
    message = "/start " + param
    # list_hash = []
    await client.send_message(l, message)
    print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]: ---Sent the following message:" + message)
    print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]: ---Waiting 10s for a reply...")
    await asyncio.sleep(10)  # waiting for reply
    messages = []
    messages_dict = []
    async for m in client.iter_messages(entity.id, limit=10):
        messages_dict.append(m.to_dict())  # take the entire object m, instead of m.message
        messages.append(m)

    # check if inside the messages there is some telegram link (e.g. after clicking a button and receiving a new message)
    new_links = set()
    # print("[WORKER n."+str(result['p_id'])+"]---[] Looking for links in the messages...")
    for m in messages:
        link = re.search('(https\:\/\/)?t\.me\/\+?[a-zA-Z0-9\.\&\/\?\:@\-_=#]*', m.text)
        if link is not None:
            link = link.group()
            s = {
                "link_hash": link,
                "message": m.to_dict(),
                "group_id": entity.id,
                "group_name": entity.first_name
            }
            result['link_gathered'].append(s)
            new_links.union(link)
    result['code'] = "BOT_RESULT" # superfluo
    result['link'] = link_extended
    result['group_id'] = entity.id
    result["new_links"] = list_to_json(new_links)
    result['messages_bot'] = messages_dict


async def evaluate_link(client, l, result):
    # checking if a parameter is present in the link
    if l.rfind('?') != -1:  # adding parameter
        param = l[l.rfind('=') + 1:]
        # print("[DEBUG]: " + param)
        link_extended = l
        l = l[0: + l.rfind('?')]  # keep the link without param
        try:
            entity = await client.get_entity(l)
            # print(" ---[DEBUG]: get entity on parameter research successful")
            if type(entity) == telethon.tl.types.User:
                if entity.bot:
                    result['code'] = "BOT_RESULT"
                    await bot_interaction(link_extended, param, l, entity, client, result)
                else:
                    result['code'] = "JOIN_FAILED"
            else:
                result['code'] = "JOIN_FAILED"

        except BaseException as e:
            print(str(datetime.now()) + " - [WORKER n." + str(
                result['p_id']) + "]---[!Base Error on get entity_1]: " + str(e))
            result['code'] = "JOIN_FAILED"
    elif l[len(l) - 3:] == 'bot' or l[len(l) - 3:] == 'BOT' or l[len(l) - 3:] == 'Bot':  # check if "bot" are the last 3 letters of the link
        param = ""
        link_extended = l  # mi serve per la gather nel caso il bot non avesse parametri
        try:
            entity = await client.get_entity(l)
            # print(" ---[DEBUG]: get entity_2 successful")
            result['code'] = "BOT_RESULT"
            await bot_interaction(link_extended, param, l, entity, client, result)
        except BaseException as e:
            print(str(datetime.now()) + " - [WORKER n." + str(
                result['p_id']) + "]---[!Base Error on get entity_2]: " + str(e))
            result['code'] = "JOIN_FAILED"
    else:
        # join the group and process the link
        link_extended = l
        l = l[l.rfind('/') + 1:]
        l = l[l.rfind('+') + 1:]
        try:
            # print("[DEBUG]: " + str(l))
            update = await join_group_by_hash(link_extended, l, client, result)
            if update is not None:
                result['code'] = "JOIN_SUCCESS"
                entity_id = update.chats[0].id
                result['group_id'] = entity_id
                result['time'] = datetime.now()
                await process_link(link_extended, entity_id, client, l, result)

        except BaseException as e:
            # result of join failed built in join_group_by_hash
            print(str(datetime.now()) + " - [WORKER n." + str(result['p_id']) + "]:" + str(e))


# Define a function for slave processes to work on URLs
async def crawl_worker(client, task_queue, result_queue, i, process_queue):  # questa cosa fa due task
    print(str(datetime.now()) + " - [WORKER n." + str(
        i) + "]---------------***********START TIME*************------------- ")
    while True:
        task = task_queue.get()
        # print(task)
        if task['name'] == str("TRY_JOIN"):
            list_data = task['data']
            l = list_data[0]
            # print("[WORKER n." + str(i) + "-debug] link:" + l)
            result = {
                "p_id": i,
                "link": l,
                "group_id": "",
                "code": "",
                "data": None, # superfluo
                "messages_bot": [],
                "link_gathered": [],
                "new_links": [],
                "error_messages": "",
                "time": None
            }
            print(
                str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------[EVAL_LINK]: Evaluating the link: " + str(
                    l))
            await evaluate_link(client, l, result)
            # put result in queue
            result["time"] = datetime.now()
            print(str(datetime.now()) + " - [WORKER n." + str(
                i) + "]: ---------[RESULT - TRY_JOIN]: Putting in queue result for the link: " + str(l))
            # print("[WORKER n." + str(i) + "][RESULT]:"+str(result))
            result_queue.put(result)

            if i < 2:
                wait_time = random.randint(100, 120)
                print(str(datetime.now()) + " - [WORKER n." + str(i) + "] waiting " + str(wait_time) + " seconds... ")
                # wait for n sec before joining a group
                await asyncio.sleep(wait_time)
            else:
                wait_time = random.randint(220, 240)
                print(
                    str(datetime.now()) + " - [WORKER n." + str(i) + "] waiting " + str(wait_time) + " seconds... ")
                # wait for n sec before joining a group
                await asyncio.sleep(wait_time)
            # put id in process_queue, passing also process queue
            print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------Putting id_worker in free queue...")
            process_queue.put(i)
        if task['name'] == str("LEAVE"):
            # leave group and build result
            for e in task['data']:
                result = {
                    "p_id": i,
                    "code": "",
                    "link": "",
                    "id": "",
                    "error_messages": "",
                    "time": None
                }
                print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------Leaving group: " + str(
                    e['link_hash']) + " - " + str(e['id']))
                wait_time = random.randint(0, 5)
                print(str(datetime.now()) + " - [WORKER n." + str(i) + "] waiting " + str(
                    wait_time) + " seconds before leaving a group... ")
                await asyncio.sleep(wait_time)
                await leave_group(e, client, result)
                result["id"] = e['id']
                result['link'] = e['link_hash']
                print(str(datetime.now()) + " - [WORKER n." + str(
                    i) + "]: ---------[RESULT - LEAVE TASK]: Putting in queue result for the id: " + str(e['id']))
                result["time"] = datetime.now()
                result_queue.put(result)

            print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------Putting id worker in free queue...")
            process_queue.put(i)
            # task['name'] = ""
        if task['name'] == str("CHECK_WAIT"):
            for el in task['data']:
                result = {
                    "p_id": i,
                    "link": "",
                    "group_id": "",
                    "code": "",
                    "data": None,
                    "messages_bot": [],
                    "link_gathered": [],
                    "new_links": [],
                    "error_messages": "",
                    "time": None
                }

                print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------checking request to: " + str(
                    el['link_hash']))
                try:
                    # print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------checking request to: " + str(e['link_hash']))
                    await client(JoinChannelRequest(el["link_hash"]))
                    result['code'] = "REQUEST_ACCEPTED"
                    entity = await client.get_entity(el["link_hash"])
                    await process_link(el["link_hash"], entity.id, client, el["link_hash"], result)
                    print(str(datetime.now()) + " - [WORKER n." + str(
                        i) + "]: ---------[RESULT - CHECK_WAIT - ACCEPTED]: Putting in queue result for: " + str(
                        el['link_hash']))
                    result["link"] = el['link_hash']
                    result["time"] = datetime.now()
                    result_queue.put(result)
                except telethon.errors.UserAlreadyParticipantError as e:
                    result['code'] = "REQUEST_ACCEPTED"
                    # print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------request accepted")
                    entity = await client.get_entity(el["link_hash"])
                    await process_link(el["link_hash"], entity.id, client, el["link_hash"], result)
                    print(str(datetime.now()) + " - [WORKER n." + str(
                        i) + "]: ---------[RESULT - CHECK_WAIT - ACCEPTED]: Putting in queue result for: " + str(
                        el['link_hash']))
                    result["link"] = el['link_hash']
                    result["time"] = datetime.now()
                    result_queue.put(result)
                except Exception as e:
                    result['code'] = "STILL_WAITING"
                    # print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------request still pending")
                    result['error_messages'] = str(e)
                    print(str(datetime.now()) + " - [WORKER n." + str(
                        i) + "]: ---------[RESULT - CHECK_WAIT - STILL WAIT]: Putting in queue result for: " + str(
                        el['link_hash']))
                    result["link"] = el['link_hash']
                    result["time"] = datetime.now()
                    result_queue.put(result)

                if i < 2:
                    wait_time = random.randint(100, 120)
                    print(
                        str(datetime.now()) + " - [WORKER n." + str(i) + "] waiting " + str(wait_time) + " seconds... ")
                    # wait for n sec before joining a group
                    await asyncio.sleep(wait_time)
                else:
                    wait_time = random.randint(220, 240)
                    print(
                        str(datetime.now()) + " - [WORKER n." + str(i) + "] waiting " + str(wait_time) + " seconds... ")
                    # wait for n sec before joining a group
                    await asyncio.sleep(wait_time)

            print(str(datetime.now()) + " - [WORKER n." + str(i) + "]: ---------Putting id_worker in free queue...")
            process_queue.put(i)


async def main(client, task_queue, result_queue, pid, process_queue):
    me = await client.get_me()
    username = me.username

    print(str(datetime.now()) + " - [MASTER]: ------------Launching worker n. " + str(pid) + " - " + str(
        username) + " - " + str(me.phone))
    await crawl_worker(client, task_queue, result_queue, pid, process_queue)


def launch_clients(api_id, api_hash, pid, task_queue, result_queue, process_queue):
    client = TelegramClient('Simo_session' + str(pid), api_id, api_hash)

    with client:
        client.loop.run_until_complete(main(client, task_queue, result_queue, pid, process_queue))
        client.disconnect()


def initializing_mongodb():
    print(str(datetime.now()) + " - [MASTER]: Opening mongodb connection...")
    # myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myclient = pymongo.MongoClient("mongodb://myUserAdmin:********@***.***.***.**:*****/?authMechanism=DEFAULT")
    mydb = myclient["MyCrawler"]
    tbp_collection = mydb["tbp"]  # 0
    groups_collection = mydb["groups"]  # 1
    done_collection = mydb["done"]  # 2
    wait_collection = mydb["wait"]  # 3
    gather_collection = mydb["gathered"]  # 4
    edges_collection = mydb["edges"]  # 5
    #gather_ext_collection = mydb["gathered_ext"]  # 6
    analytics_collection = mydb["analytics"]  # 7
    bot_collection = mydb["bot_collection"]  # 8
    leave_collection = mydb["leave"]  # 9

    collections = (tbp_collection, groups_collection, done_collection, wait_collection, gather_collection,
                   edges_collection, None, analytics_collection, bot_collection, leave_collection)
    return collections


def check_threshold(timestamp, threshold):
    now = datetime.now()
    time_difference = now - timestamp
    hours_difference = time_difference.total_seconds() / 3600
    # print("[MASTER - DEBUG]: check_threshold: "+str(hours_difference))

    if hours_difference >= threshold:
        return True
    else:
        return False


def get_result(result_queue, db, leave_queues, wait_queues):
    # size = result_queue.qsize()
    # print(str(datetime.now()) + " - [MASTER][RESULT]: " +str(size)+" results present in the queue...")
    while not result_queue.empty():
        result = result_queue.get()
        code = result["code"]
        # print(str(datetime.now()) + " - [MASTER][RESULT "+str(size)+"]: Result of the worker [" + str(result['p_id']) + "] for the link ["+result['link']+"] got from queue with the following code: " + code)
        print(str(datetime.now()) + " - [MASTER][RESULT]: Result of the worker [" + str(
            result['p_id']) + "] for the link [" + result['link'] + "] got from queue with the following code: " + code)
        # size = result_queue.qsize()

        if code == "JOIN_SUCCESS":
            try:
                db[4].insert_many(result['link_gathered'])
            except:
                print(
                    str(datetime.now()) + " - [MASTER][RESULT]: [] Links with metadata inserted correctly in gathered")
            try:
                db[0].insert_many(result['new_links'])
                print(str(datetime.now()) + " - [MASTER][RESULT]: [] New links  inserted correctly in tbp_collection")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Not found any new link in this result.")
            try:
                if db[1].find_one({"id": str(result["group_id"])}) is None:
                    db[1].insert_one(result['data'])
                    print(str(datetime.now()) + " - [MASTER][RESULT]: [] data inserted correctly in groups_collection")
                    db[7].update_one({"_id": 0}, {"$inc": {"collect_counter": 1}})
                else:
                    print(
                        str(datetime.now()) + " - [MASTER][RESULT]: [!] data to insert already present in group_collection")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in groups_collection.insert_one()")

            try:
                i = result['p_id']
                item = {
                    "link_hash": result["link"],
                    "id": result["group_id"],
                    "time_joined": result["time"]
                }
                db[9].update_one({"_id": i}, {"$push": {"queue": item}})
                leave_queues[i].put(item)
                print(str(datetime.now()) + " - [MASTER][RESULT]: item inserted correctly in leave_queue")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in leave_collection.update_one()")

            db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "inside"}})
            db[2].update_one({"link_hash": result['link']}, {"$set": {"time": result['time']}})

            # update graph
            for el in db[4].find({"link_hash": result['link']}):
                update_edges(db[5], result['group_id'], el["group_id"])

        elif code == "JOIN_FAILED":
            db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "join_failed"}})
            db[2].update_one({"link_hash": result['link']}, {"$set": {"time": result['time']}})

        elif code == "BOT_RESULT":
            try:
                db[4].insert_many(result['link_gathered'])
                print(
                    str(datetime.now()) + " - [MASTER][RESULT]: [] Links with metadata inserted correctly in gathered")
            except:
                print(
                    str(datetime.now()) + " - [MASTER][RESULT]: [!] Not inserted any link with metadata in gather_collection.")

            try:
                db[0].insert_many(result['new_links'])
            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Not found any new link in this channel/group.")

            try:
                d = {
                    'process_id': str(result['p_id']),
                    'id': str(result['group_id']),
                    'link_hash': result['link'],
                    'messages': result['messages_bot'],
                    'time': result['time']
                }
                if db[8].find_one({"id": str(result["group_id"])}) is None:
                    db[8].insert_one(d)
                    print(str(datetime.now()) + " - [MASTER][RESULT]: [] data inserted correctly in bot_collection")
                    db[7].update_one({"_id": 3}, {"$inc": {"bot_counter": 1}})
                    db[7].update_one({"_id": 0}, {"$inc": {"collect_counter": 1}})
                else:
                    print(
                        str(datetime.now()) + " - [MASTER][RESULT]: [!] data to insert already present in bot_collection")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in bot_collection.insert_one()")

            try:
                i = result['p_id']
                item = {
                    "link_hash": result["link"],
                    "id": result["group_id"],
                    "time_joined": result["time"]
                }
                db[9].update_one({"_id": i}, {"$push": {"queue": item}})
                leave_queues[i].put(item)
                print(str(datetime.now()) + " - [MASTER][RESULT]: item inserted correctly in leave_queue")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in leave_collection.insert_one()")

            db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "inside"}})
            db[2].update_one({"link_hash": result['link']}, {"$set": {"time": result['time']}})

            for el in db[4].find({"link_hash": result['link']}):
                update_edges(db[5], result['group_id'], el["group_id"])
            # print()

        elif code == "REQUEST_SENT":

            try:
                i = result['p_id']
                item = {
                    "link_hash": result["link"],
                    "time_request": result["time"]
                }
                db[3].update_one({"_id": i}, {"$push": {"queue": item}})
                wait_queues[i].put(item)
                print(str(datetime.now()) + " - [MASTER][RESULT]: item inserted correctly in wait_queue")
                db[7].update_one({"_id": 4}, {"$inc": {"request_counter": 1}})
                db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "waiting"}})
            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in wait_collection.insert_one()")

        elif code == "LEAVE_SUCCESS":
            db[9].update_one({"_id": result["p_id"]}, {"$pull": {"queue": {"id": result['id']}}})
            db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "done"}})
            db[2].update_one({"link_hash": result['link']}, {"$set": {"time": result['time']}})

        elif code == "LEAVE_FAILED":
            db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "leave_failed"}})

            try:
                i = result['p_id']
                item = {
                    "link_hash": result["link"],
                    "id": result["group_id"],
                    "time_joined": datetime.now()
                }
                db[9].update_one({"_id": result["p_id"]}, {"$pull": {"queue": {"id": result['id']}}})
                db[9].update_one({"_id": i}, {"$push": {"queue": item}})
                leave_queues[i].put(item)
                print(str(datetime.now()) + " - [MASTER][RESULT]: item inserted correctly in leave_queue")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in leave_collection.update_one()")


        elif code == "REQUEST_ACCEPTED":
            db[7].update_one({"_id": 5}, {"$inc": {"request_accepted": 1}})

            try:
                db[4].insert_many(result['link_gathered'])
                print(
                    str(datetime.now()) + " - [MASTER][RESULT]: [] Links with metadata inserted correctly in gathered")
            except:
                print(
                    str(datetime.now()) + " - [MASTER][RESULT]: [!] Not inserted any link with metadata in gather_collection.")
            try:
                db[0].insert_many(result['new_links'])
                print(str(datetime.now()) + " - [MASTER][RESULT]: [] New links  inserted correctly in tbp_collection")
            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Not found any new link in this result.")

            try:
                if db[1].find_one({"id": str(result["group_id"])}) is None:
                    db[1].insert_one(result['data'])
                    print(str(datetime.now()) + " - [MASTER][RESULT]: [] data inserted correctly groups_collection")
                    db[7].update_one({"_id": 0}, {"$inc": {"collect_counter": 1}})
                else:
                    print(
                        str(datetime.now()) + " - [MASTER][RESULT]: [!] data to insert already present in group_collection")
            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in groups_collection.insert_one()")

            try:
                i = result['p_id']
                item = {
                    "link_hash": result["link"],
                    "id": result["group_id"],
                    "time_joined": result["time"]
                }
                db[9].update_one({"_id": i}, {"$push": {"queue": item}})
                leave_queues[i].put(item)
                print(str(datetime.now()) + " - [MASTER][RESULT]: item inserted correctly in leave_queue")

            except:
                print(str(datetime.now()) + " - [MASTER][RESULT]: [!] Error in leave_collection.insert_one()")

            # change state link from waiting to inside
            db[2].update_one({"link_hash": result['link']}, {"$set": {"state": "inside"}})
            db[2].update_one({"link_hash": result['link']}, {"$set": {"time": result['time']}})
            db[3].update_one({"_id": result["p_id"]}, {"$pull": {"queue": {"link_hash": result['link']}}})

            # update graph
            for el in db[4].find({"link_hash": result['link']}):
                update_edges(db[5], result['group_id'], el["group_id"])

        elif code == "STILL_WAITING":
            i = result['p_id']
            item = {
                "link_hash": result["link"],
                "time_request": datetime.now()
            }
            wait_queues[i].put(item)


        else:
            print(str(datetime.now()) + " - [MASTER][RESULT]: Result without code for the link: " + result[
                "link"] + " - " + str(result['p_id']))
            print(
                str(datetime.now()) + " - [MASTER][RESULT]: " + result["error_messages"] + " - " + str(result['time']))
            continue


if __name__ == '__main__':
    # Opening database connection and grouping collections
    try:
        db = initializing_mongodb()
        print(str(datetime.now()) + " - ... connection with db opened successfully!")
    except Exception as e:
        print(str(datetime.now()) + " - [!] Error in opening database connection:" + str(e))
        exit(-1)

    # Create a multiprocessing Queue for tasks and results
    # link_queue = multiprocessing.Queue()
    print(str(datetime.now()) + " - [MASTER]: Creating Data structures")
    result_queue = multiprocessing.Queue()
    leave_queues = [None] * n_processes
    wait_queues = [None] * n_processes
    process_queue = multiprocessing.Queue()
    tasks = [None] * n_processes
    threshold_leave = 24  # 24
    # last_leave = datetime.now()
    threshold_request = 24  # 5
    # last_check_wait = datetime.now()

    print(str(datetime.now()) + " - [MASTER]: Creating leave queues, wait queues, processes queue, task list")

    # Retrieve stuff in processing, if any
    temp_state = "processing"
    for x in db[2].find({"state": temp_state}):
        x['state'] = "tbp"
        x['time'] = ""
        x['process_id'] = ""
        db[2].delete_one({"link_hash": x["link_hash"]})
        db[7].update_one({"_id": 1}, {"$inc": {"total_counter": -1}})
        if db[0].find_one({"link_hash": x["link_hash"]}) is None:
            db[0].insert_one(x)

    # Create and start n worker processes
    processes = []

    print(str(datetime.now()) + " - [MASTER]: Launching workers... ")
    for i in range(n_processes):
        leave_queues[i] = multiprocessing.Queue()
        element = db[9].find_one({"_id": i})

        for el in element["queue"]:
            if el["id"] == "" or el["link_hash"] == "":
                continue
            else:
                leave_queues[i].put(el)

        wait_queues[i] = multiprocessing.Queue()
        element = db[3].find_one({"_id": i})
        for el in element["queue"]:
            wait_queues[i].put(el)

        process_queue.put(i)
        tasks[i] = multiprocessing.Queue()
        process = multiprocessing.Process(target=launch_clients,
                                          args=(api_ids[i], api_hashes[i], i, tasks[i], result_queue, process_queue))
        process.start()
        processes.append(process)


    x = db[0].find_one()
    while x is not None:
        if not process_queue.empty():
            i = process_queue.get()
            print(str(datetime.now()) + " - [MASTER]: Taking a free worker from the queue...: [" + str(i) + "]")

            # Assign Task 2
            # 2 significa un LEAVE_TASK ogni due ore
            task = {
                'name': '',
                'data': []
            }
            # last_leave = datetime.now()
            temp_queue = multiprocessing.Queue()
            while not leave_queues[i].empty():
                element = leave_queues[i].get()
                timestamp = element["time_joined"]
                # check temporal threshold for leaving
                if check_threshold(timestamp, threshold_leave):
                    # tasks[i]['data'].append(element)
                    task['data'].append(element)
                else:
                    temp_queue.put(element)
            leave_queues[i] = temp_queue
            if len(task['data']) > 0:
                print(str(datetime.now()) + " - [MASTER]: assigning LEAVE TASK to process: " + str(i))
                # tasks[i]['name'] = str("LEAVE")
                task['name'] = str("LEAVE")
                tasks[i].put(task)
                if not result_queue.empty():
                    print(
                        str(datetime.now()) + " - [MASTER][RESULT]: getting result after having assigned LEAVE TASK to process: " + str(
                            i))
                    get_result(result_queue, db, leave_queues, wait_queues)

            # Assign Task 3
            else:
                task = {
                    'name': '',
                    'data': []
                }
                temp_queue = multiprocessing.Queue()
                while not wait_queues[i].empty():
                    element = wait_queues[i].get()
                    timestamp = element["time_request"]
                    # check temporal threshold for request
                    if check_threshold(timestamp, threshold_request):
                        # tasks[i]['data'].append(element)
                        task['data'].append(element)
                    else:
                        temp_queue.put(element)
                wait_queues[i] = temp_queue
                if len(task['data']) > 0:
                    print(str(datetime.now()) + " - [MASTER]: assigning CHECK_WAIT to process: " + str(i))
                    task['name'] = str("CHECK_WAIT")
                    tasks[i].put(task)
                    if not result_queue.empty():
                        print(
                            str(datetime.now()) + " - [MASTER][RESULT]: getting result after having assigned LEAVE TASK to process: " + str(
                                i))
                        get_result(result_queue, db, leave_queues, wait_queues)

                # Assign Task 1
                else:
                    task = {
                        'name': '',
                        'data': []
                    }
                    # trying to put a while true, con break se riesci ad assegnare
                    x = db[0].find_one()
                    print(str(datetime.now()) + " - [MASTER]: Taking a link from tbp_collection in db...: " + str(
                        x["link_hash"]))
                    #  [DEBUG] print(str(x))
                    # check whether the link is in done collection or not
                    if db[2].find_one({"link_hash": x[
                        "link_hash"]}) is not None:  # db contains done_collection in position number 2
                        # if yes delete from tbp collection on db
                        print(
                            str(datetime.now()) + " - [MASTER]: Link already in done, removing element from tbp_collection...: " + str(
                                x["link_hash"]))
                        db[0].delete_many({"link_hash": x["link_hash"]})

                        process_queue.put(i)

                    else:
                        db[7].update_one({"_id": 1}, {"$inc": {"total_counter": 1}})
                        task['data'].append(x["link_hash"])
                        print(str(datetime.now()) + " - [MASTER]: Assigning TRY JOIN task to worker: " + str(i))
                        task['name'] = "TRY_JOIN"
                        tasks[i].put(task)
                        # change its status from tbp to processing and add timestamp of status change both in memory and db
                        x["state"] = "processing"
                        x["time"] = datetime.now()
                        # change procces_id to i (the process to which it was assigned)
                        x["process_id"] = i
                        # remove from db from tbp_collection
                        db[0].delete_many({'link_hash': x["link_hash"]})
                        try:
                            if db[2].find_one({"link_hash": x["link_hash"]}) is None:
                                db[2].insert_one(x)
                        except:
                            print(str(datetime.now()) + " - [MASTER]: Error in inserting " + str(
                                x['link_hash']) + " in done_collection")

                        # do get result
                        if not result_queue.empty():
                            print(
                                str(datetime.now()) + " - [MASTER][RESULT]: getting result after having assigned TRY_JOIN TASK to worker: " + str(
                                    i))
                            get_result(result_queue, db, leave_queues, wait_queues)
        elif not result_queue.empty():
            print(str(datetime.now()) + " - [MASTER][RESULT]: No free worker available. Taking result from queue result.")
            get_result(result_queue, db, leave_queues, wait_queues)

    # Wait for all slave processes to finish
    print(str(datetime.now()) + " - [MASTER]: link list empty, ending master process")
    for process in processes:
        process.join()
