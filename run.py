#!/usr/bin/python
from multiprocessing import Process, Manager
from progressbar import ProgressBar
import os
import time
import copy

import datetime
from requests.exceptions import HTTPError, SSLError

from pprint import pprint
from sumologic import SumoLogic
import yaml


MAX_QUEUE_SIZE = 20


def log(msg):
    pass


def timing(f):
    def wrap(*args):
        start = time.time()
        ret = f(*args)
        end = time.time()
        result = str(datetime.timedelta(milliseconds=1000*(end-start)))[:7]
        print(f'program took {result}')

        return ret
    return wrap


class ProgressBarWrapper:

    def __init__(self, progressbar):
        self.progressbar = progressbar.start()
        self.counter = 0

    def add(self, value=1):
        self.counter += 1
        self.progressbar.update(self.counter)


def toDatetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp / 1000)


class ConfigInterpreter:
    def __init__(self):
        with open("config.yaml", "r") as _file:
            config = yaml.safe_load(_file.read())
        self.durationOfQuery = self.parseTime(**config['durationOfQuery'])
        self.timeStep = self.parseTime(**config['timeStep'])
        self.startTimeDelta = self.parseTime(**config['startTimeDelta'])
        self.lastStartTime = self.parseTime(**config.get('lastStartTime', {}))
        self.runCount = int(self.startTimeDelta / self.timeStep)
        self.orgs_config = config['orgs']
        self.orgs_to_use = config['use_orgs']
        self.config = config
        self.lastStartTime = int(time.time() * 1000) - self.lastStartTime
        self.firstStartTime = self.lastStartTime - self.startTimeDelta
        self.output_file = config['output_file']

        with open(config['query'], "r") as query_file:
            self.query = query_file.read()

    @classmethod
    def parseTime(cls, **kwargs):
        if kwargs == {}:
            return 0
        return int(datetime.timedelta(**kwargs).total_seconds() * 1000)

    @classmethod
    def sumoFromDict(cls, _dict):
        return SumoLogic(_dict["access_id"], _dict["access_key"], _dict["endpoint"])

    def createFromConfig(self):
        progressbar = ProgressBarWrapper(ProgressBar(2*self.runCount*len(self.orgs_to_use)).start())
        orgs = {}
        for key, _dict in self.orgs_config.items():
            if key in self.orgs_to_use:
                sqm = SumoQueryManager(
                        self.durationOfQuery, self.timeStep, self.startTimeDelta,
                        self.lastStartTime, self.query, self.sumoFromDict(_dict)
                )
                orgs[key] =  DeploymentManager(key, self.runCount, sqm, progressbar)
        return Sumoltaneously(orgs)

    def getInfo(self):
        result = []
        result.append(f'lastStartTime: {toDatetime(self.lastStartTime)}')
        result.append(f'firstStartTime: {toDatetime(self.firstStartTime)}')
        result.append(f'query:\n {self.query}')
        return result


class SumoQueryManager:

    def __init__(self, durationOfQuery, timeStep, startTimeDelta, lastStartTime, query, sumo):
        self.sumo = sumo
        self.query = query
        self.durationOfQuery = durationOfQuery
        self.timeStep = timeStep
        self.lastStartTime = lastStartTime
        self.firstStartTime = self.lastStartTime - startTimeDelta

    def timeRange(self, iteration):
        toTime = self.lastStartTime - iteration * self.timeStep
        fromTime = toTime - self.durationOfQuery
        return fromTime, toTime

    def startQuery(self, iteration):
        fromTime, toTime = self.timeRange(iteration)
        return self.sumo.search_job(self.query, fromTime=fromTime, toTime=toTime, timeZone="CET", byReceiptTime=False)

    def check(self, waiting):
        with Manager() as manager:
            result = manager.dict()
            def _check(result):
                try:
                    result.update(self.sumo.search_job_status(waiting))
                except SSLError:
                    pass

            action_process = Process(target=_check, args=(result,))
            action_process.start()
            action_process.join(timeout=3)
            return dict(result)

    def timeRangeResultDatetime(self, iteration):
        toTime = self.lastStartTime - iteration * self.timeStep
        fromTime = toTime - self.timeStep
        return toDatetime(fromTime), toDatetime(toTime)


class DeploymentManager:

    def __init__(self, deployment, runCount, sumoQueryManager, progressbar):
        self.deployment = deployment
        self.iterationsToRun = set(range(0, runCount))
        self.totalIterations = len(self.iterationsToRun)
        self.sumoQueryManager = sumoQueryManager
        self.waitingForResults = {}
        self.readyResults = {}
        self.progressbar = progressbar

    def checkPerformed(self):
        # check already started runs
        iterationsToBeRemoved=[]
        for iteration, waiting in self.waitingForResults.items():
            try:
                status = self.sumoQueryManager.check(waiting)
            except HTTPError as error:
                if (error.response.status_code == 429):
                    log(f"[{self.deployment}] Failed to save iteration {iteration}")
                    self.iterationsToRun.add(iteration)
                    exit(-1)
            else:
                if status.get('state') == 'DONE GATHERING RESULTS':
                    log(f"[{self.deployment}] Iteration {iteration} ready, saving results")
                    self.readyResults[self.sumoQueryManager.timeRangeResultDatetime(iteration)] = status
                    iterationsToBeRemoved.append(iteration)
                    self.progressbar.add(1)

        for iteration in iterationsToBeRemoved:
            del(self.waitingForResults[iteration])

    def startNewQueries(self):
        while self.iterationsToRun and len(self.waitingForResults) < MAX_QUEUE_SIZE:
            iteration = self.iterationsToRun.pop()
            try:
                self.waitingForResults[iteration] = self.sumoQueryManager.startQuery(iteration)
            except HTTPError as error:
                if (error.response.status_code == 429):
                    self.iterationsToRun.add(iteration)
                    break
                else:
                    raise error
            self.progressbar.add(1)
            log(f"[{self.deployment}] Sending iteration number {iteration}")

    def nextIteration(self):
        return self.iterationsToRun or self.waitingForResults


class Sumoltaneously:

    def __init__(self, deployments):
        self.deployments = deployments
        self.readyDeploymentsResults = {}

    def executeQueries(self):
        end = False
        while not end:
            end = True
            for dep in self.deployments.values():
                if dep.nextIteration():
                    dep.checkPerformed()
                    dep.startNewQueries()
                    end = False
            time.sleep(1)

        self.readyDeploymentsResults = {
            key: value.readyResults
            for key, value in self.deployments.items()
        }


def flatTimeranges(trList):
    result = []
    lastTr = None
    for tr in trList:
        if lastTr is None:
            lastTr = list(tr)
            continue
        if lastTr[1] == tr[0]:
            lastTr[1] = tr[1]
            continue
        result.append(lastTr)
        lastTr = list(tr)
    if lastTr is not None:
        result.append(lastTr)
    return result


def getResults(sumoltaneously, config):
    result = []
    for org, orgResults in sumoltaneously.readyDeploymentsResults.items():
        result.append(f'{org}:')

        timeranges = sorted([k for k, v in orgResults.items() if v['recordCount'] > 0])
        for row in flatTimeranges(timeranges):
            result.append(f'{row[0].strftime("%m/%d/%Y %H:%M")} - {row[1].strftime("%m/%d/%Y %H:%M")}')

    result += config.getInfo()
    return '\n'.join(result)


@timing
def run():
    config = ConfigInterpreter()
    sumoltaneously = config.createFromConfig()
    sumoltaneously.executeQueries()
    # with open("output.txt", "w") as file_:
    #     pprint(sumoltaneously.readyDeploymentsResults, stream=file_)

    results = getResults(sumoltaneously, config)
    with open("results", "w") as file_:
        file_.write(results)
        print(results)


if __name__ == "__main__":
    run()
