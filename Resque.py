#!/usr/bin/env python

import redis

class SimpleResqueClient:
    def __init__(self, host = 'localhost', port = 6379, db = 0, prefix = ''):
        self.redis  = redis.Redis(host, port, db)
        self.prefix = prefix

    def key(self, k):
        return self.prefix + ':' + k

    def queues(self):
        return self.redis.smembers(self.key('queues'))

    def queued(self, queue):
        return self.redis.llen(self.key('queue:' + queue))

class Resque:
    def __init__(self, agent_config, logger, raw_config):
        params = {}

        for k, v in raw_config["Main"].items():
            if k[:7] == "resque_":
                params[k[7:]] = v

        self.client = SimpleResqueClient(**params)

    def run(self):
        return dict((name, self.client.queued(name)) for name in self.client.queues())

#This code is for debugging and unit testing
if __name__ == '__main__':
    raw_config = {"Main": {}}
    raw_config["Main"]["resque_host"]   = "localhost"
    raw_config["Main"]["resque_prefix"] = "some/prefix"
    raw_config["Main"]["resque_port"]   = 6379
    raw_config["Main"]["resque_db"]     = 0

    resque = Resque(None, None, raw_config)
    print resque.run()
