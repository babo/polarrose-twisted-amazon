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

__all__ = [ 'SimpleDatabaseService' ]

SDB_API_VERSION = "2007-11-07"
SDB_SERVICE_ENDPOINT = "http://sdb.amazonaws.com/"
SDB_XPATH='{%sdoc/%s/}' % (SDB_SERVICE_ENDPOINT, SDB_API_VERSION)

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
        self.requestId = tree.findtext(SDB_XPATH+'ResponseMetadata/'+SDB_XPATH+'RequestId')
        self.boxUsage = decimal.Decimal(tree.findtext(SDB_XPATH+'ResponseMetadata/'+SDB_XPATH+'BoxUsage'))

class CreateDomainResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<CreateDomainResponse requestId: "%s">' % (self.requestId)

class DeleteDomainResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<DeleteDomainResponse requestId: "%s">' % (self.requestId)

class ListDomainsResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.domains = []
        r = tree.find(SDB_XPATH+'ListDomainsResult')
        for e in r.findall(SDB_XPATH+'DomainName'):
            self.domains.append(e.text)
        self.nextToken = r.findtext(SDB_XPATH+'NextToken')
    def __repr__(self):
        return '<ListDomainsResponse requestId: "%s" domains: %s>' % (self.requestId, self.domains)

class PutAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<PutAttributesResponse requestId: "%s">' % (self.requestId)

class DeleteAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<DeleteAttributesResponse requestId: "%s">' % (self.requestId)

class GetAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.attributes = {}
        r = tree.find(SDB_XPATH+'GetAttributesResult')
        for e in r.findall(SDB_XPATH+'Attribute'):
            name = e.findtext(SDB_XPATH+'Name')
            value = e.findtext(SDB_XPATH+'Value')
            if self.attributes.has_key(str(name)):
                if type(self.attributes[str(name)]) != list:
                    self.attributes[str(name)] = [ self.attributes[str(name)]  ]
                self.attributes[str(name)].append(value)
            else:
                self.attributes[str(name)] = value
    def __repr__(self):
        return '<GetAttributesResponse requestId: "%s" attributes: %s>' % (self.requestId, self.attributes)

class QueryWithAttributesResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.items = {}
        r = tree.find(SDB_XPATH+'QueryWithAttributesResult')
        for e in r.findall(SDB_XPATH+'Item'):
            name = e.findtext(SDB_XPATH+'Name')
            attrs = {}
            for attr in e.findall(SDB_XPATH+'Attribute'):
                k = attr.findtext(SDB_XPATH+'Name')
                v = attr.findtext(SDB_XPATH+'Value')
                if attrs.has_key(k):
                    prev = attrs[k]
                    if type(prev) == list:
                        attrs[k].append(v)
                    else:
                        attrs[k] = [prev, v]
                else:
                    attrs[k] = v
            self.items[name] = attrs
        self.nextToken = r.findtext(SDB_XPATH+'NextToken')
    def __repr__(self):
        return '<QueryResponse requestId: "%s" items: %s nextToken: %s>' % (self.requestId, self.items, self.nextToken)

class QueryResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.items = []
        r = tree.find(SDB_XPATH+'QueryResult')
        for e in r.findall(SDB_XPATH+'ItemName'):
            self.items.append(e.text)
        self.nextToken = r.findtext(SDB_XPATH+'NextToken')
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

class SimpleDatabaseService(object):

    SDB_ACCEPTABLE_ERRORS = [400]

    totalBoxUsage = decimal.Decimal(0)

    RESPONSE_OBJECTS = {
        SDB_XPATH+'CreateDomainResponse': CreateDomainResponse,
        SDB_XPATH+'DeleteDomainResponse': DeleteDomainResponse,
        SDB_XPATH+'ListDomainsResponse': ListDomainsResponse,
        SDB_XPATH+'PutAttributesResponse': PutAttributesResponse,
        SDB_XPATH+'DeleteAttributesResponse': DeleteAttributesResponse,
        SDB_XPATH+'GetAttributesResponse': GetAttributesResponse,
        SDB_XPATH+'QueryWithAttributesResponse': QueryWithAttributesResponse,
        SDB_XPATH+'QueryResponse': QueryResponse,
        'Response': ErrorResponse
    }

    def __init__(self, key = None, secret = None, debug = False, process=None):
        self.key = key
        self.secret = secret
        self.debug = debug
        self.process = process or (lambda x: x)
        self.logger = logging.getLogger('polarrose.amazon.SimpleDatabaseService')

    def createDomain(self, domainName):
        url = self._createRequestUrl("CreateDomain", DomainName=domainName)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def deleteDomain(self, domainName):
        url = self._createRequestUrl("DeleteDomain", DomainName=domainName)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def listDomains(self, maxNumberOfDomains = 100, nextToken = None):
        url = self._createRequestUrl("ListDomains", MaxNumberOfDomains=maxNumberOfDomains, NextToken=nextToken)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def putAttributes(self, domainName, itemName, attributes, replace = None):
        url = self._createRequestUrl("PutAttributes",
            self._attributesToParameters(attributes, replace),
            DomainName=domainName, ItemName=itemName)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def deleteAttributes(self, domainName, itemName, attributes = {}):
        url = self._createRequestUrl("DeleteAttributes",
            self._deletedAttributesToParameters(attributes),
            DomainName= domainName, ItemName=itemName)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def getAttributes(self, domainName, itemName, attributes = []):
        if type(attributes) in (list, tuple):
            attributeNames = dict([("AttributeName.%s" % (n+1), v) for n,v in enumerate(attributes)])
        else:
            attributeNames = {'AttributeName': str(attributes)}
        url = self._createRequestUrl("GetAttributes", attributeNames, DomainName=domainName, ItemName=itemName)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def query(self, domainName, queryExpression = None, maxNumberOfItems = 100, nextToken = None):
        url = self._createRequestUrl("Query", DomainName=domainName, QueryExpression=queryExpression,
            NextToken=nextToken, MaxNumberOfItems=maxNumberOfItems)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

    def queryWithAttributes(self, domainName, attributeName=None, queryExpression = None, maxNumberOfItems = 100, nextToken = None):
        url = self._createRequestUrl("QueryWithAttributes", DomainName=domainName, QueryExpression=queryExpression,
            AttributeName=attributeName, MaxNumberOfItems=maxNumberOfItems, NextToken=nextToken)
        if self.debug:
            self.logger.debug(url)
        return self.process(url)

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

    def _commonCallback(self, response):
        if self.debug:
            self.logger.debug(response)
        tree = ElementTree.fromstring(response)
        response = self.RESPONSE_OBJECTS[tree.tag](tree)
        self.totalBoxUsage += response.boxUsage
        return response

    def _commonErrback(self, failure):
        if self.debug:
            self.logger.debug(str(failure))
        if int(failure.value.status) in self.SDB_ACCEPTABLE_ERRORS:
            tree = ElementTree.fromstring(failure.value.response)
            return self.RESPONSE_OBJECTS[tree.tag](tree)
        else:
            return failure

    def _generateSignature(self, params):
        signature = ""
        for (key, value) in sorted(params, key=lambda (k,v): (str.lower(k),v)):
            signature = signature + key + str(value)
        return base64.encodestring(hmac.new(self.secret, signature, sha).digest()).strip()

    def _createRequestUrl(self, action, *optParams, **namedParams):
        parameters = []
        for op in optParams:
            parameters += op.items()
        parameters += namedParams.items()

        filtered=[]
        for (k,v) in parameters:
            if v and type(v) in (list, tuple):
                i=1
                for vs in v:
                    if vs:
                        filtered.append(('%s.%d' % (k, i), vs))
                        i = i + 1
            elif v:
                filtered.append((k,v))
        filtered.append(('Action', action))
        filtered.append(('Version', SDB_API_VERSION))
        filtered.append(('AWSAccessKeyId', self.key))
        filtered.append(('SignatureVersion', "1"))
        filtered.append(('Timestamp', datetime.utcnow().isoformat()[0:19]+"+00:00"))
        filtered.append(('Signature', self._generateSignature(filtered)))
        return "%s?%s" % (SDB_SERVICE_ENDPOINT, urllib.urlencode(filtered))

    def _attributesToParameters(self, attributes, replace = None):
        parameters = {}
        n = 1
        for k,v in attributes.items():
            if replace and ((type(replace) == bool and k) or k in replace):
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
        elif type(attributes) == dict:
            return self._deletedAttributesToParametersDictionary(attributes)
        else:
            return {"Attribute.Name": str(attributes)}

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

class SimpleDB(SimpleDatabaseService):
    def __init__(self, key = None, secret = None, debug = False):
        SimpleDatabaseService.__init__(self, key, secret, debug, process=self._make_request)

    def _make_request(self, location, method='GET'):
        from urlparse import urlparse

        while True:
            (scheme, host, path, params, query, fragment) = urlparse(location)
            connection = self.get_connection(scheme, host)

            if query: path += "?" + query
            if fragment: path += '#' + fragment

            connection.request(method,path)
            resp = connection.getresponse()
            if resp.status < 300 or resp.status >= 400:
                return self._commonCallback(resp.read())

            # handle redirect
            location = resp.getheader('location')
            if not location:
                return self._commonErrback(resp.read())
            # (close connection)
            # retry with redirect

    def get_connection(self, scheme, host):
        import httplib
        if scheme == "http": return httplib.HTTPSConnection(host)
        elif scheme == "https": return httplib.HTTPConnection(host)
        else: raise invalidURL(scheme + '://' + location)

class TwistedDB(SimpleDatabaseService):
    from twisted.application import internet, service
    from twisted.internet import defer, task
    from twisted.web import client
    from twisted.web.error import Error
    from twisted.python import log

    def __init__(self, key = None, secret = None, debug = False):
        SimpleDatabaseService.__init__(self, key, secret, debug, process=self._make_request)

    def _make_request(self, location, method='GET'):
        from twisted.web import client
        return client.getPage(location).addCallback(self._commonCallback).addErrback(self._commonErrback)

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
