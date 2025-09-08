import copy
import random as rd
import csv
import math
import os
# import pandas as pd
# import numpy as np
import concurrent.futures

def resetParams():
    global walking_speed  # m/s
    global routeTable
    global roadLengthTable
    global arrivalTime
    global departuresProfile
    global arrivalProfile
    global carpark_grouping
    global carpark_population
    walking_speed = 1  # m/s
    routeTable = {}
    roadLengthTable = {}
    arrivalTime = {}
    departuresProfile = {}
    arrivalProfile = {}
    carpark_grouping = {}
    carpark_population = {}

def readRouteTable(metro):
    #Route table initialization
    with open(f'UptownInputs/roadCalcs_routeTable_{metro}.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            profileId = row[0]
            popSplit = float(row[2])
            route = list(filter(lambda x:x!='',row[3:]))
            if profileId not in routeTable:
                routeTable[profileId] = []
            routeTable[profileId].append(
                {
                    'popSplit': popSplit,
                    'route': route
                }
            )

def readRoadLengthTable():
#Route length initialization
    with open('UptownInputs/roadCalcs_roadLengthTable.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            roadProfile = row[0]
            length = float(row[1])
            roadLengthTable[roadProfile] = {
                'length': length,
                'walking_time': round(length / walking_speed) # Calculate walking time in seconds
            }

def readCarParkGrouping():
#group parking initialization
    with open('UptownInputs/roadCalcs_carpark_grouping.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            roadProfile = row[0]
            group = row[2]
            carpark_grouping[roadProfile] = group

def populationFormatter(pop):
    try:
        return 0 if pop == '' else int(pop.replace(',', ''))
    except:
        return 0
        
def readCarParkPopulation():
#carpark population initialization
    with open('UptownInputs/roadCalcs_carpark_population.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            roadProfile = row[0]
            population = populationFormatter(row[1])
            carpark_population[roadProfile] = population
        print(carpark_population)
def readArrivalTime(scenario):
#population and arrival initialization    
    with open(f'UptownInputs/roadCalcs_arrivalTime_{scenario}.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            profileId = row[0]
            time = float(row[3])
            population = populationFormatter(row[2])
            arrivalTime[profileId] = {
                'population': population,
                'time': time
            }

def readArrivalDepartureProfile(scenario):
#arrival profile
    with open(f'UptownInputs/roadCalcs_arrivalProfile_departures_{scenario}.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)
        headers = data[0]
        arrivalData = data[1:]

        def pctFormatter(pct):
            try:
                return abs(float(pct))
            except:
                return abs(float(str(pct.split('%')[0])))/100

        for row in arrivalData:
            profileId = row[0]
            timeProfile = row[2:]
            departuresProfile[profileId] = {}
            for idx,pct in enumerate(timeProfile):
                departuresProfile[profileId][(idx*3600)] = 0 if pct == '' else pctFormatter(pct)


def readArrivalArrivalProfile(scenario):
#arrival profile
    with open(f'UptownInputs/roadCalcs_arrivalProfile_arrivals_{scenario}.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)
        headers = data[0]
        arrivalData = data[1:]

        def pctFormatter(pct):
            try:
                return abs(float(pct))
            except:
                return abs(float(str(pct.split('%')[0])))/100

        for row in arrivalData:
            profileId = row[0]
            timeProfile = row[2:]
            arrivalProfile[profileId] = {}
            for idx,pct in enumerate(timeProfile):
                arrivalProfile[profileId][(idx*3600)] = 0 if pct == '' else pctFormatter(pct)

def removeCarparkPopulationFromArrivalTime():
    groupTotal = {}
    #get total
    for profileId in arrivalTime:
        group = carpark_grouping[profileId] if profileId in carpark_grouping else None
        groupTotal[group] = groupTotal.get(group,0) + arrivalTime[profileId]['population'] if group else 0

    for profileId in arrivalTime:
        group = carpark_grouping[profileId] if profileId in carpark_grouping else None
        popToRemove = math.floor(arrivalTime[profileId]['population']/groupTotal[group] * carpark_population[group]) if group and group in groupTotal and groupTotal[group]>0 else 0
        arrivalTime[profileId]['population'] -= popToRemove
    
def round_down(num, divisor):
    return num - (num%divisor)

class person:
    def __init__(self,personIndex,timeAtNode,pathList,nodeDetail,inNewSpace):
        self.idx = personIndex
        self.curNode = pathList[0]
        self.curNodeIdx = 0
        self.timeAtCurNode = timeAtNode
        self.nodeList = pathList
        self.node_detail = nodeDetail 
        self.timeAtNextNode = timeAtNode + self.calculateTimeToNextNode()
        self.agentInNewSpace = inNewSpace

    def __str__(self):
        return f"{self.idx} {self.curNode} {self.timeAtCurNode} {self.timeAtNextNode}"

    def calculateTimeToNextNode(self):
        # print(self.node_detail)
        timeToSpendAtCurNode = self.node_detail[self.curNode]['walking_time']
        return timeToSpendAtCurNode

    def updateAgentLocation(self,curTime):
        self.agentInNewSpace = False
        if curTime == self.timeAtNextNode:
            #Reached Last Step, remove details
            if self.curNode == self.nodeList[-1]:
                self.curNode = 'end'
                self.timeAtNextNode = -1
                return
    
            self.curNodeIdx+=1
            self.curNode = self.nodeList[self.curNodeIdx]
            self.timeAtCurNode = curTime
            self.timeAtNextNode = curTime + self.calculateTimeToNextNode()
            self.agentInNewSpace = True
            return
        
def runScenario(metro,scenario):
    startingTime = 0*3600 #7pm in seconds
    #simulate arrival time of people coming in a lift and moving through from one to the next and calculate occupancy of each node over time
    simulationTime = 86400+startingTime #seconds
    #Escalators
    peoplePerHour = 1500
    flowRateBucket = 60
    flowRateWidthTest = 66 # people per minute per metre
    #keep track of time when people arrive at a node, where they are and when they should leave.

    arrivalRatePerSecond = peoplePerHour / 3600
    numberOfDeparturesGenerated = {}
    numberOfArrivalsGenerated = {}
    numberOfPeople = {}
    peopleList = []
    peopleEnd = 0
    nodeOccupancies = {}
    nodePeopleCount = {}
    nodeFlowRate = {}
    nodeWidthFlowRequired = {}
    for profile in routeTable:
        numberOfDeparturesGenerated[profile] = {}
        numberOfArrivalsGenerated[profile] = {}
        for routeId,routes in enumerate(routeTable[profile]):
            numberOfDeparturesGenerated[profile][routeId] = 0
            numberOfArrivalsGenerated[profile][routeId] = 0



    for curTime in range(startingTime,simulationTime,1):
        timeBucket = round_down(curTime, 3600)
        if curTime not in nodeOccupancies:
            nodeOccupancies[curTime] = {}
            nodePeopleCount[curTime] = {}
            nodeFlowRate[curTime] = {}
            nodeWidthFlowRequired[curTime] = {}
            nodeOccupancies[curTime]['end'] = 0
            nodePeopleCount[curTime]['end'] = 0
            nodeFlowRate[curTime]['end'] = 0
            nodeWidthFlowRequired[curTime]['end'] = 0


            for key in roadLengthTable:
                nodeOccupancies[curTime][key] = 0
                nodePeopleCount[curTime][key] = nodePeopleCount[curTime-1][key] if curTime > startingTime else 0
                nodeFlowRate[curTime][key] = nodePeopleCount[curTime-1][key] - nodePeopleCount[curTime-(flowRateBucket+1)][key] if curTime > startingTime+flowRateBucket+1 else 0
                nodeWidthFlowRequired[curTime][key] = nodeFlowRate[curTime-1][key]/flowRateWidthTest if curTime > startingTime else 0

        #loop through profiles ID and generate the number of people
        for profile in departuresProfile:
            if profile not in routeTable:
                continue
            for routeId, routes in enumerate(routeTable[profile]):
                profileNodePaths = routes['route']
                if len(profileNodePaths) == 0:
                    continue
                reversedProfileNode = routes['route'][::-1]
                remainingArrivalPct = 0

                #departures over 15 mins
                numberOfDepartures = round(departuresProfile[profile][timeBucket]*routes['popSplit']*arrivalTime[profile]['population'])
                hourArrivalProfile = arrivalProfile[profile][timeBucket]
                
                nodeDetails = roadLengthTable


                #40% arrive in the first 15 mins of the hour, the remainder over the next 45 mins
                if hourArrivalProfile > 0.4:
                    remainingArrivalPct = hourArrivalProfile - 0.4
                    hourArrivalProfile = 0.4


                if curTime < timeBucket+arrivalTime[profile]['time']:
                    numberOfArrivals = round(hourArrivalProfile*routes['popSplit']*arrivalTime[profile]['population'])
                    rateOfDeparturesPeople = (numberOfDepartures/arrivalTime[profile]['time'])
                    numberOfDeparturesGenerated[profile][routeId] += rateOfDeparturesPeople

                    rateOfArrivalsPeople = (numberOfArrivals/arrivalTime[profile]['time'])
                    numberOfArrivalsGenerated[profile][routeId] += rateOfArrivalsPeople

                    personDeparturesAmount = math.floor(numberOfDeparturesGenerated[profile][routeId])
                    for personIndex in range(int(personDeparturesAmount)):
                        peopleList.append(person(len(peopleList),curTime,profileNodePaths,nodeDetails,True))

                    personArrivalAmount = math.floor(numberOfArrivalsGenerated[profile][routeId])
                    for personIndex in range(int(personArrivalAmount)):
                        peopleList.append(person(len(peopleList),curTime,reversedProfileNode,nodeDetails,True))




                if remainingArrivalPct!=0 and curTime >= timeBucket+arrivalTime[profile]['time']:
                    numberOfArrivals = round(remainingArrivalPct*routes['popSplit']*arrivalTime[profile]['population'])
                    remainingRateOfArrivalsPeople = (numberOfArrivals/(3600-arrivalTime[profile]['time']))
                    numberOfArrivalsGenerated[profile][routeId] += remainingRateOfArrivalsPeople

                    personArrivalAmount = math.floor(numberOfArrivalsGenerated[profile][routeId])
                    for personIndex in range(int(personArrivalAmount)):
                        peopleList.append(person(len(peopleList),curTime,reversedProfileNode,nodeDetails,True))


                numberOfDeparturesGenerated[profile][routeId]-=math.floor(numberOfDeparturesGenerated[profile][routeId])
                numberOfArrivalsGenerated[profile][routeId]-=math.floor(numberOfArrivalsGenerated[profile][routeId])


        newPeopleList = []
        for agentId, agent in enumerate(peopleList):
            agentInNewSpace = agent.agentInNewSpace
            if agentInNewSpace:
                nodePeopleCount[curTime][agent.curNode] += 1

            agent.updateAgentLocation(curTime)
            agentLocation = agent.curNode

            if agentLocation == 'end':
                peopleEnd += 1
                continue

            nodeOccupancies[curTime][agentLocation] += 1
            newPeopleList.append(agent)
        nodeOccupancies[curTime]['end'] = peopleEnd
        peopleList = newPeopleList

# print(nodeOccupancies)

# Get headers from one of the inner dictionaries
    fieldnames = ['time'] + list(next(iter(nodeOccupancies.values())).keys())

    with open(f'UptownOutputs/{metro}/roadCalcs_OccupancyOutput_{scenario}.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for time, node_values in nodeOccupancies.items():
            row = {'time': time}
            row.update(node_values)
            writer.writerow(row)

    with open(f'UptownOutputs/{metro}/roadCalcs_PeopleCountOutput_{scenario}.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for time, node_values in nodePeopleCount.items():
            row = {'time': time}
            row.update(node_values)
            writer.writerow(row)

    with open(f'UptownOutputs/{metro}/roadCalcs_FlowRateOutput_{scenario}.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for time, node_values in nodeFlowRate.items():
            row = {'time': time}
            row.update(node_values)
            writer.writerow(row)
    with open(f'UptownOutputs/{metro}/roadCalcs_WidthFlowRequiredOutput_{scenario}.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for time, node_values in nodeWidthFlowRequired.items():
            row = {'time': time}
            row.update(node_values)
            writer.writerow(row)

def runSingleMain():
    # scenarioList = ['00','01','01M','02','02B','02BM','02M','03','04','04B','04BM','05','05B','A','AA','asianG','BBCC','BC','D','DM','esport','FIFA','megaSport','megaSportM','Olympics','OlympicsT','Opening','OpeningM']
    scenarioList = ['OlympicsT']
    # metroList = ['pre','post']
    metroList = ['pre']
    for metro in metroList:
        for scenario in scenarioList:
            print(f"Running scenario {scenario} for metro {metro}")
            resetParams()
            readRouteTable(metro)
            readRoadLengthTable()
            readArrivalTime(scenario)
            readArrivalDepartureProfile(scenario)
            readArrivalArrivalProfile(scenario)

            readCarParkGrouping()
            readCarParkPopulation()
            removeCarparkPopulationFromArrivalTime()

            runScenario(metro,scenario)

def run_full_scenario(metro, scenario):
    print(f"Running scenario {scenario} for metro {metro}")
    resetParams()
    readRouteTable(metro)
    readRoadLengthTable()
    readArrivalTime(scenario)
    readArrivalDepartureProfile(scenario)
    readArrivalArrivalProfile(scenario)

    readCarParkGrouping()
    readCarParkPopulation()
    removeCarparkPopulationFromArrivalTime()

    runScenario(metro,scenario)

if __name__ == "__main__":
    scenarioList = ['00','01','01M','02','02B','02BM','02M','03','04','04B','04BM','05','05B','A','AA','asianG','BBCC','BC','D','DM','esport','FIFA','megaSport','megaSportM','Olympics','OlympicsT','Opening','OpeningM','OpeningS1','OpeningS1M','OpeningS2','OpeningS1M','OpeningS3','OpeningS2M']
    # scenarioList = ['04B']
    metroList = ['pre','post']
    # metroList = ['pre']
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for metro in metroList:
            for scenario in scenarioList:
                print(f"Submitting scenario {scenario} for metro {metro}")
                futures.append(executor.submit(run_full_scenario, metro, scenario))
        for future in concurrent.futures.as_completed(futures):
            future.result()