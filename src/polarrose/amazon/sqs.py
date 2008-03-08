#
# Copyright 2007 Polar Rose <http://www.polarrose.com> and Stefan
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
# limitations under the License.
#

from datetime import datetime
import hmac, sha, base64, urllib
from xml.etree import ElementTree

from twisted.application import internet, service
from twisted.web import client
from twisted.web.error import Error

__all__ = [ 'SimpleQueue', 'SimpleQueueMessage', 'SimpleQueueService' 'SimpleQueuePoller' ]

class SimpleQueue(object):
    def __init__(self, url):
        self.url = url
#    def __str__(self):
#        return self.url
    def __repr__(self):
        return '<SimpleQueue url: %s>' % (self.url)

class SimpleQueueMessage(object):
    def __init__(self, id, body):
        self.id = id
        self.body = body
#    def __str__(self):
#        return self.id
    def __repr__(self):
        return '<SimpleQueueMessage id: %s body: %s>' % (self.id, self.body)

class ErrorResponse(object):
    def __init__(self, tree):
        self.requestId = tree.findtext('RequestID')
        self.errors = {}
        for e in tree.findall('Errors/Error'):
            code = e.findtext('Code')
            message = e.findtext('Message')
            self.errors[code] = message
    def getRequestId(self):
        return self.requestId
    def isSuccess(self):
        return False
    def __repr__(self):
        return '<ErrorResponse requestId: %s errors: %s>' % (self.requestId, self.errors)
    
class BaseResponse(object):
    def __init__(self, tree):
        self.requestId = tree.findtext('{http://queue.amazonaws.com/doc/2007-05-01/}ResponseStatus/{http://queue.amazonaws.com/doc/2007-05-01/}RequestId')
        self.statusCode = tree.findtext('{http://queue.amazonaws.com/doc/2007-05-01/}ResponseStatus/{http://queue.amazonaws.com/doc/2007-05-01/}StatusCode')
        if self.isSuccess():
            self.parseResponse(tree)        
    def parseResponse(self, tree):
        pass
    def getRequestId(self):
        return self.requestId
    def getStatusCode(self):
        return self.statusCode
    def isSuccess(self):
        return True

class CreateQueueResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def parseResponse(self, tree):
        self.queue = SimpleQueue(tree.findtext('{http://queue.amazonaws.com/doc/2007-05-01/}QueueUrl'))
    def __repr__(self):
        return '<CreateQueueResponse statusCode: "%s" requestId: "%s" queue: %s>' % (self.statusCode, self.requestId, self.queue)

class DeleteQueueResponse(BaseResponse):    
    def __init__(self, response):
        BaseResponse.__init__(self, response)
    def __repr__(self):
        return '<DeleteQueueResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class ChangeMessageVisibilityResponse(BaseResponse):    
    def __init__(self, response):
        BaseResponse.__init__(self, response)
    def __repr__(self):
        return '<ChangeMessageVisibilityResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class SendMessageResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def parseResponse(self, tree):
        self.messageId = tree.findtext('{http://queue.amazonaws.com/doc/2007-05-01/}MessageId')
    def __repr__(self):
        return '<SendMessageResponse statusCode: "%s" requestId: "%s" messageId: "%s">' % (self.statusCode, self.requestId, self.messageId)

class ReceiveMessagesResponse(BaseResponse):
    def __init__(self, tree):
        self.messages = []
        BaseResponse.__init__(self, tree)
    def parseResponse(self, tree):
        for e in tree.findall('{http://queue.amazonaws.com/doc/2007-05-01/}Message'):
            self.messages.append(SimpleQueueMessage(e.findtext('{http://queue.amazonaws.com/doc/2007-05-01/}MessageId'),
               e.findtext('{http://queue.amazonaws.com/doc/2007-05-01/}MessageBody')))
    def __repr__(self):
        return '<ReceiveMessagesResponse statusCode: "%s" requestId: "%s" messages: %s>' % (self.statusCode, self.requestId, str(self.messages))

class DeleteMessageResponse(BaseResponse):
    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
    def __repr__(self):
        return '<DeleteMessageResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class ListQueuesResponse(BaseResponse):
    def __init__(self, tree):
        self.queues = []
        BaseResponse.__init__(self, tree)
    def parseResponse(self, tree):
        for e in tree.findall("{http://queue.amazonaws.com/doc/2007-05-01/}QueueUrl"):
            self.queues.append(SimpleQueue(e.text))        
    def __repr__(self):
        return '<ListQueuesResponse statusCode: "%s" requestId: "%s" queues: %s>' % (self.statusCode, self.requestId, self.queues)

class GetQueueAttributesResponse(BaseResponse):
    def __init__(self, tree):
        self.attributes = {}
        BaseResponse.__init__(self, tree)
    #def parseResponse(self, tree):
    #    for e in tree.findall("{http://queue.amazonaws.com/doc/2007-05-01/}QueueUrl"):
    #        self.queues.append(SimpleQueue(e.text))        
    def __repr__(self):
        return '<GetQueueAttributesResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

class PeekMessageResponse(BaseResponse):
    def __init__(self, tree):
        self.message = None
        BaseResponse.__init__(self, tree)
    #def parseResponse(self, tree):
    #    for e in tree.findall("{http://queue.amazonaws.com/doc/2007-05-01/}QueueUrl"):
    #        self.queues.append(SimpleQueue(e.text))        
    def __repr__(self):
        return '<PeekMessageResponse statusCode: "%s" requestId: "%s">' % (self.statusCode, self.requestId)

RESPONSE_OBJECTS = {
    'Response':
        ErrorResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}CreateQueueResponse':
        CreateQueueResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}DeleteQueueResponse':
        DeleteQueueResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}GetQueueAttributesResponse':
        GetQueueAttributesResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}SendMessageResponse':
        SendMessageResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}ReceiveMessageResponse':
        ReceiveMessagesResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}DeleteMessageResponse':
        DeleteMessageResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}ListQueuesResponse':
        ListQueuesResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}ChangeMessageVisibilityResponse':
        ChangeMessageVisibilityResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}GetQueueAttributesResponse':
        GetQueueAttributesResponse,
    '{http://queue.amazonaws.com/doc/2007-05-01/}PeekMessageResponse':
        PeekMessageResponse
}

class SimpleQueueService(object):

    SQS_API_VERSION = "2007-05-01"
    SQS_SERVICE_ENDPOINT = "http://queue.amazonaws.com"
    
    ACCEPTABLE_ERRORS = [400]
    DEFAULT_VISIBILITY_TIMEOUT = 30

    def __init__(self, key = None, secret = None):
        self.key = key
        self.secret = secret

    def changeMessageVisibility(self, queue, messageId, visibilityTimeout):
        url = self._createQueueRequestUrl(queue, "ChangeMessageVisibility", {'VisibilityTimeout': visibilityTimeout})
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)
    
    def createQueue(self, name, defaultVisibilityTimeout = 30):
        url = self._createServiceRequestUrl("CreateQueue", {'QueueName': name,
           'DefaultVisibilityTimeout': defaultVisibilityTimeout})
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    def deleteMessage(self, queue, messageId):
        if isinstance(messageId, SimpleQueueMessage): messageId = messageId.id
        url = self._createQueueRequestUrl(queue, "DeleteMessage", {'MessageId': messageId })
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)
    
    def deleteQueue(self, queue, forceDeletion = False):
        url = self._createQueueRequestUrl(queue, "DeleteQueue", {'ForceDeletion': str(forceDeletion).lower() })
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)
    
    def getQueueAttributes(self, queue, attributeName = 'All'):
        url = self._createQueueRequestUrl(queue, "GetQueueAttributes", {'AttributeName': attributeName })
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)        
    
    def sendMessage(self, queue, messageBody):
        url = self._createQueueRequestUrl(queue, "SendMessage", {'MessageBody': messageBody })
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)
    
    def receiveMessages(self, queue, numberOfMessages = 1, visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT):
        url = self._createQueueRequestUrl(queue, "ReceiveMessage", {'VisibilityTimeout': visibilityTimeout,
           'NumberOfMessages': numberOfMessages })
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)        

    def listQueues(self, queueNamePrefix = None):
        parameters = {}
        if queueNamePrefix != None: parameters['QueueNamePrefix'] = queueNamePrefix
        url = self._createServiceRequestUrl("ListQueues", parameters)
        return client.getPage(url).addCallback(self._commonCallback).addErrback(self._commonErrback)

    def _commonCallback(self, response):
        #print response
        tree = ElementTree.fromstring(response)
        return RESPONSE_OBJECTS[tree.tag](tree)
    
    def _commonErrback(self, failure):
        if isinstance(failure, Error) and int(failure.value.status) in self.ACCEPTABLE_ERRORS:
            tree = ElementTree.fromstring(failure.value.response)
            return RESPONSE_OBJECTS[tree.tag](tree)
        else:
            return failure

    def _getCommonRequestParameters(self, action):
        parameters = {}
        parameters['Action'] = action
        parameters['Version'] = self.SQS_API_VERSION
        parameters['AWSAccessKeyId'] = self.key
        parameters['Timestamp'] = datetime.utcnow().isoformat()[0:19]+"Z"
        parameters['SignatureVersion'] = 1
        return parameters

    def _createServiceRequestUrl(self, action, parameters):
        parameters.update(self._getCommonRequestParameters(action))
        parameters['Signature'] = self._generateSignature(parameters)
        return "%s?%s" % (self.SQS_SERVICE_ENDPOINT, urllib.urlencode(parameters))

    def _createQueueRequestUrl(self, queue, action, parameters):
        parameters.update(self._getCommonRequestParameters(action))
        parameters['Signature'] = self._generateSignature(parameters)
        return "%s?%s" % (queue.url, urllib.urlencode(parameters))
    
    def _generateSignature(self, params):
        signature = ""
        for key in sorted(params.keys(), key=str.lower):
            signature = signature + key + str(params[key])
        return base64.encodestring(hmac.new(self.secret, signature, sha).digest()).strip()


# This is work in progress and far from perfect

class SimpleQueuePoller(internet.TimerService):
    
    DEFAULT_POLLER_INTERVAL = 5
    DEFAULT_NUMBER_OF_MESSAGES = 16
    DEFAULT_VISIBILITY_TIMEOUT = 30

    def __init__(self, key = None, secret = None, queue = None, numberOfMessages = DEFAULT_NUMBER_OF_MESSAGES, visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT, interval = DEFAULT_POLLER_INTERVAL):
        internet.TimerService.__init__(self, interval, self.poll)
        self.queue = queue
        self.numberOfMessages = numberOfMessages
        self.sqs = SimpleQueueService(key = key, secret = secret)    

    def poll(self):
        self.sqs.receiveMessages(self.queue, self.numberOfMessages).addCallback(self.receiveMessagesCallback).addErrback(self.receiveMessagesErrback)

    def receiveMessagesCallback(self, response):
        if response.isSuccess():
            for message in response.messages:
                try:
                    self.processMessage(message)
                except:
                    print "Exception caught while processing message"
                # Always delete the message
                deferred = self.sqs.deleteMessage(self.queue, message)
                deferred.addCallback(self.deleteMessageCallback)
                deferred.addErrback(self.deleteMessageErrback)
        else:
            print "Failed to receive messages: XXX"

    def receiveMessagesErrback(self, failure):
        print "Failed to receiveMessages(): %s" % str(failure)

    def deleteMessageCallback(self, response):
        pass

    def deleteMessageErrback(self, failure):
        print "Failed to deleteMessage(): %s" % str(failure)
        
    def processMessage(self, message):
        print "Processing message %s" % message


