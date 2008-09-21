#
# Copyright 2007-2008 Polar Rose <http://www.polarrose.com> and Stefan
#  Arentz <stefan@arentz.nl>
#
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
# limitation under the License.
#

from datetime import datetime
import decimal, hmac, logging, sha, base64, urllib
from xml.etree import ElementTree

from twisted.application import internet, service
from twisted.internet import defer, task
from twisted.web import client
from twisted.web.error import Error
from twisted.python import log

__all__ = [ 'SimpleDatabaseService' ]

class Item(object):
    def __init__(self, name, attributes):
        self.name = name
        self.attributes = attributes

class ErrorResponse(object):
    def __init__(self, tree):
        self.requestId = tree.findtext('RequestID')
        self.boxUsage = decimal.Decimal(0) # XXX Figure out how to calculate this
        self.success = False
        self.errors = {}
        for e in tree.findall('Errors/Error'):
            code = e.findtext('Code')
            message = e.findtext('Message')
            self.errors[code] = message
    def __repr__(self):
        return '<ErrorResponse requestId: "%s" errors: %s>' % (self.requestId, self.errors)

class BaseResponse(object):
    def __init__(self, tree):
        self.success = True
        self.requestId = tree.findtext('{http://sdb.amazonaws.com/doc/2007-11-07/}ResponseMetadata/{http://sdb.amazonaws.com/doc/2007-11-07/}RequestId')
        self.boxUsage = decimal.Decimal(tree.findtext('{http://sdb.amazonaws.com/doc/2007-11-07/}ResponseMetadata/{http://sdb.amazonaws.com/doc/2007-11-07/}BoxUsage'))
        
class CreateDomainResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<CreateDomainResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class DeleteDomainResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<DeleteDomainResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class ListDomainsResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.domains = []
        r = tree.find("{http://sdb.amazonaws.com/doc/2007-11-07/}ListDomainsResult")
        for e in r.findall("{http://sdb.amazonaws.com/doc/2007-11-07/}DomainName"):
            self.domains.append(e.text)
        self.nextToken = r.findtext("{http://sdb.amazonaws.com/doc/2007-11-07/}NextToken")
    def __repr__(self):
        return '<ListDomainsResponse statusCode: "%s" requestId: "%s" domains: %s>' % (self.statusCode, self.requestId, self.domains)

class PutAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<PutAttributesResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class DeleteAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<DeleteAttributesResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class GetAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.attributes = {}
        r = tree.find("{http://sdb.amazonaws.com/doc/2007-11-07/}GetAttributesResult")        
        for e in r.findall('{http://sdb.amazonaws.com/doc/2007-11-07/}Attribute'):
            name = e.findtext('{http://sdb.amazonaws.com/doc/2007-11-07/}Name')
            value = e.findtext('{http://sdb.amazonaws.com/doc/2007-11-07/}Value')
            if self.attributes.has_key(str(name)):
                if type(self.attributes[str(name)]) != list:
                    self.attributes[str(name)] = [ self.attributes[str(name)]  ]
                self.attributes[str(name)].append(value)
            else:
                self.attributes[str(name)] = value
    def __repr__(self):
        return '<GetAttributesResponse requestId: "%s" attributes: %s>' % (self.requestId, self.attributes)

class QueryResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.items = []
        r = tree.find("{http://sdb.amazonaws.com/doc/2007-11-07/}QueryResult")
        for e in r.findall("{http://sdb.amazonaws.com/doc/2007-11-07/}ItemName"):
            self.items.append(e.text)
        self.nextToken = r.findtext('{http://sdb.amazonaws.com/doc/2007-11-07/}NextToken')
    def __repr__(self):
        return '<QueryResponse requestId: "%s" items: %s nextToken: %s>' % (self.requestId, self.items, self.nextToken)

class FetchResponse(object):
    def __init__(self, queryResponse):
        self.success = True
        self.requestId = queryResponse.requestId
        self.boxUsage = queryResponse.boxUsage
        self.nextToken = queryResponse.nextToken
        self.items = {}
    def __repr__(self):
        return '<FetchResponse requestId: "%s" items: %s nextToken: %s>' % (self.requestId, self.items, self.nextToken)

RESPONSE_OBJECTS = {
    '{http://sdb.amazonaws.com/doc/2007-11-07/}CreateDomainResponse': CreateDomainResponse,
    '{http://sdb.amazonaws.com/doc/2007-11-07/}DeleteDomainResponse': DeleteDomainResponse,
    '{http://sdb.amazonaws.com/doc/2007-11-07/}ListDomainsResponse': ListDomainsResponse,
    '{http://sdb.amazonaws.com/doc/2007-11-07/}PutAttributesResponse': PutAttributesResponse,
    '{http://sdb.amazonaws.com/doc/2007-11-07/}DeleteAttributesResponse': DeleteAttributesResponse,
    '{http://sdb.amazonaws.com/doc/2007-11-07/}GetAttributesResponse': GetAttributesResponse,
    '{http://sdb.amazonaws.com/doc/2007-11-07/}QueryResponse': QueryResponse,
    'Response': ErrorResponse
}

class SimpleDatabaseService(object):

    SDB_API_VERSION = "2007-11-07"
    SDB_SERVICE_ENDPOINT = "http://sdb.amazonaws.com"

    SDB_ACCEPTABLE_ERRORS = [400]

    totalBoxUsage = decimal.Decimal(0)

    def __init__(self, key = None, secret = None, debug = False):
        self.key = key
        self.secret = secret
        self.debug = debug
        self.logger = logging.getLogger('polarrose.amazon.SimpleDatabaseService')
    #

    def createDomain(self, domainName):
        url = self._createRequestUrl("CreateDomain", {'DomainName': domainName})
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    def deleteDomain(self, domainName):
        url = self._createRequestUrl("DeleteDomain", {'DomainName': domainName})
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    def listDomains(self, maxNumberOfDomains = 100, nextToken = None):
        parameters = {}
        if maxNumberOfDomains:
            parameters['MaxNumberOfDomains'] = maxNumberOfDomains
        if nextToken:
            parameters['NextToken'] = nextToken
        url = self._createRequestUrl("ListDomains", parameters)
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    #

    def putAttributes(self, domainName, itemName, attributes, replace = None):
        url = self._createRequestUrl("PutAttributes", {'DomainName': domainName, 'ItemName': itemName },
           self._attributesToParameters(attributes, replace))
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    def deleteAttributes(self, domainName, itemName, attributes = {}):
        url = self._createRequestUrl("DeleteAttributes", {'DomainName': domainName, 'ItemName': itemName},
           self._deletedAttributesToParameters(attributes))
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    def getAttributes(self, domainName, itemName, attributes = []):
        attributeNames = dict([("AttributeName.%s" % (n+1), v) for n,v in enumerate(attributes)])
        url = self._createRequestUrl("GetAttributes", {'DomainName': domainName, 'ItemName': itemName}, attributeNames)
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)        
    
    #

    def query(self, domainName, queryExpression = None, maxNumberOfItems = 100, nextToken = None):
        parameters = {'DomainName': domainName, 'QueryExpression': queryExpression,
                      'MaxNumberOfItems': maxNumberOfItems}
        if nextToken:
            parameters['NextToken'] = nextToken
        url = self._createRequestUrl("Query", parameters)
        if self.debug:
            self.logger.debug(url)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    #

    def _fetchCollectorCallback(self, responses, itemNames, fetchResponse):
        for itemName, item in zip(itemNames, responses):
            if item[0]:
                fetchResponse.items[itemName] = Item(itemName, item[1])
            else:
                fetchResponse.items[itemName] = item[1]
        return fetchResponse

    def _fetchQueryCallback(self, response, domainName, attributes):
        if not response.success:
            return response
        deferred = defer.DeferredList([self.getAttributes(domainName, item, attributes) for item in response.items],
            consumeErrors = True)
        return deferred.addCallback(self._fetchCollectorCallback, response.items, FetchResponse(response))

    def fetch(self, domainName, expression = "", attributes = [], maxNumberOfItems = 100, nextToken = None):
        deferred = self.query(domainName, expression, maxNumberOfItems, nextToken)
        return deferred.addCallback(self._fetchQueryCallback, domainName, attributes)

    #

#    def _executeParallelTasks(self, iterable, count, callable, *args, **named):
#        coop = task.Cooperator()
#        work = (callable(elem, *args, **named) for elem in iterable)
#        return defer.DeferredList([coop.coiterate(work) for i in xrange(count)])
#
#    def _handleFetchItem(self, response, responses):
#        responses.append(response)
#        return response
#
#    def _fetchItem(self, (itemName), domainName, responses):
#        return self.getAttributes(domainName, itemName).addCallback(self._handleFetchItem, responses)
#
#    def _handleExecuteParallelFetchItems(self, response, responses):
#        return responses
#
#    def _handleQuerySuccess(self, response, count, domainName, responses):
#        deferred = self._executeParallelTasks(response.items, count, self._fetchItem, domainName, responses)
#        return deferred.addCallback(self._handleExecuteParallelFetchItems, responses)
#
#    def parallelFetch(self, domainName, expression, attributes = [], maxNumberOfItems = 100, count = 2):
#        deferred = self.query(domainName, expression, maxNumberOfItems)
#        return deferred.addCallback(self._handleQuerySuccess, count, domainName, [])

    #

    def _commonCallback(self, response):
        if self.debug:
            self.logger.debug(response)
        tree = ElementTree.fromstring(response)
        response = RESPONSE_OBJECTS[tree.tag](tree)
        self.totalBoxUsage += response.boxUsage
        return response
    
    def _commonErrback(self, failure):
        if self.debug:
            self.logger.debug(str(failure))
        if int(failure.value.status) in self.SDB_ACCEPTABLE_ERRORS:
            tree = ElementTree.fromstring(failure.value.response)
            return RESPONSE_OBJECTS[tree.tag](tree)
        else:
            return failure

    def _generateSignature(self, params):
        signature = ""
        for key in sorted(params.keys(), key=str.lower):
            signature = signature + key + str(params[key])
        return base64.encodestring(hmac.new(self.secret, signature, sha).digest()).strip()

    def _createRequestUrl(self, action, *params):
        parameters = {}
        for p in params: parameters.update(p)
        parameters['Action'] = action
        parameters['Version'] = self.SDB_API_VERSION
        parameters['AWSAccessKeyId'] = self.key
        parameters['SignatureVersion'] = "1"
        parameters['Timestamp'] = datetime.utcnow().isoformat()[0:19]+"+00:00"
        parameters['Signature'] = self._generateSignature(parameters)
        return "%s?%s" % (self.SDB_SERVICE_ENDPOINT, urllib.urlencode(parameters))

    def _attributesToParameters(self, attributes, replace = None):
        parameters = {}
        n = 1
        for k,v in attributes.items():
            if replace and k in replace:
                parameters["Attribute.%d.Replace" % n] = "true"
            if type(v) in (list, tuple):
                for i in v:
                    parameters["Attribute.%d.Name" % n] = str(k)
                    parameters["Attribute.%d.Value" % n] = str(i)
                    n = n + 1                    
            else:
                parameters["Attribute.%d.Name" % n] = str(k)
                parameters["Attribute.%d.Value" % n] = str(v)
                n = n + 1
        return parameters

    def _deletedAttributesToParameters(self, attributes):
        if type(attributes) in (list,tuple):
            return self._deletedAttributesToParametersList(attributes)
        else:
            return self._deletedAttributesToParametersDictionary(attributes)            

    def _deletedAttributesToParametersList(self, attributes):
        parameters = {}
        n = 1
        for v in attributes:
            parameters["Attribute.%d.Name" % n] = str(v)
            n = n + 1
        return parameters
    
    def _deletedAttributesToParametersDictionary(self, attributes):
        parameters = {}
        n = 1
        for k,v in attributes.items():
            if type(v) in (list, tuple):
                for i in v:
                    parameters["Attribute.%d.Name" % n] = str(k)
                    parameters["Attribute.%d.Value" % n] = str(i)
                    n = n + 1                    
            else:
                parameters["Attribute.%d.Name" % n] = str(k)
                if v:
                    parameters["Attribute.%d.Value" % n] = str(v)
                n = n + 1
        return parameters
    
