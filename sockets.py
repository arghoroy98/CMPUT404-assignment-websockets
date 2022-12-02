#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2013-2014 Abram Hindle
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import flask
from flask import Flask, request
from flask_sockets import Sockets
import gevent
from gevent import queue
import time
import json
import os

app = Flask(__name__)
sockets = Sockets(app)
app.debug = True

clients = list()

class Client:                               #Reference(Lines 31 to 39): https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py
    def __init__(self): 
        self.queue = queue.Queue()

    def put(self, v):
        self.queue.put_nowait(v)

    def get(self):
        return self.queue.get()


class World:
    def __init__(self):
        self.clear()
        # we've got listeners now!
        self.listeners = list()
        
    def add_set_listener(self, listener):
        self.listeners.append( listener )

    def update(self, entity, key, value):
        entry = self.space.get(entity,dict())
        entry[key] = value
        self.space[entity] = entry
        self.update_listeners( entity )

    def set(self, entity, data):
        self.space[entity] = data
        self.update_listeners( entity )

    def update_listeners(self, entity):
        '''update the set listeners'''
        for listener in self.listeners:
            listener(entity, self.get(entity))

    def clear(self):
        self.space = dict()

    def get(self, entity):
        return self.space.get(entity,dict())
    
    def world(self):
        return self.space

myWorld = World()        

#REFERENCED CODE
def send_all(msg):                      #Reference(Lines 78 to 83): https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py          
    for client in clients:
        client.put(msg)

def send_all_json(obj):
    send_all(json.dumps(obj))
#REFERENCED CODE BLOCK END

def set_listener( entity, data ):
    ''' do something with the update ! '''
    current_object = {}
    current_object[entity] = data
    send_all_json(current_object)

myWorld.add_set_listener( set_listener )
        
@app.route('/')
def hello():
    '''Return something coherent here.. perhaps redirect to /static/index.html '''
    return flask.redirect("/static/index.html")


def read_ws(ws,client):                                                         #Reference(Line 100 to 112): https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py
    '''A greenlet function that reads from the websocket and updates the world'''
    try:
        while True:
            msg = ws.receive()
            if (msg != None):
                packet = json.loads(msg)
                for key,value in packet.items():
                    myWorld.set(key, value)
            else:
                break
    except:
        pass

@sockets.route('/subscribe')
def subscribe_socket(ws):                                                       #Reference(Line 115 to 137): https://github.com/abramhindle/WebSocketsExamples/blob/master/broadcaster.py
    '''Fufill the websocket URL of /subscribe, every update notify the
       websocket and read updates from the websocket '''
    client = Client()
    clients.append(client)
    g = gevent.spawn( read_ws, ws, client )
    try:
        # So this is a one time thing where I will send the entire world to the server
        for key,value in myWorld.world().items():
            my_object = {key:value}
            print(my_object)
            ws.send(json.dumps(my_object))
        while True:
            msg = client.get()
            print(msg)
            if (msg):
                print("Message received")
            ws.send(msg)
    except Exception as e:# WebSocketError as e:
        print(e)
    finally:
        clients.remove(client)
        gevent.kill(g)


# I give this to you, this is how you get the raw body/data portion of a post in flask
# this should come with flask but whatever, it's not my project.
def flask_post_json():
    '''Ah the joys of frameworks! They do so much work for you
       that they get in the way of sane operation!'''
    if (request.json != None):
        return request.json
    elif (request.data != None and request.data.decode("utf8") != u''):
        return json.loads(request.data.decode("utf8"))
    else:
        return json.loads(request.form.keys()[0])

@app.route("/entity/<entity>", methods=['POST','PUT'])
def update(entity):
    current_entry = flask_post_json()
    for key,value in current_entry.items():
        myWorld.set(entity, key, value)

    return myWorld.get(entity)
    

@app.route("/world", methods=['POST','GET'])    
def world():
    '''you should probably return the world here'''
    current_world = myWorld.world()
    return current_world

@app.route("/entity/<entity>")    
def get_entity(entity):
    '''This is the GET version of the entity interface, return a representation of the entity'''
    current_entity = myWorld.get(entity)
    return current_entity


@app.route("/clear", methods=['POST','GET'])
def clear():
    '''Clear the world out!'''
    myWorld.clear()
    return None



if __name__ == "__main__":
    ''' This doesn't work well anymore:
        pip install gunicorn
        and run
        gunicorn -k flask_sockets.worker sockets:app
    '''
    app.run()
