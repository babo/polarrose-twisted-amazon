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

#
# XXX THIS IS ALL PRETTY BAD - NEEDS SOME SERIOUS CLEANUP XXX
#

from datetime import datetime
from time import gmtime, strftime
import base64, hmac, sha, md5, urllib
from xml.etree import ElementTree

from twisted.web import client

class SuccessResponse(object):
    
    success = True
    requestId = None
    code = 200
    message = "OK"
    
    def __init__(self, response):
        pass

class ErrorResponse(object):
    def __init__(self, tree):
        self.success = False
        self.requestId = tree.findtext('RequestId')
        self.code = tree.findtext('Code')
        self.message = tree.findtext('Message')
    def __repr__(self):
        return '<ErrorResponse requestId: %s code: %s message: %s>' % (self.requestId, self.code, self.message)

class SimpleStorageBucket(object):
    def __init__(self, name, creationDate):
        self.name = name
        self.creationDate = creationDate
    def __repr__(self):
        return '<SimpleStorageBucket name: %s creationDate: %s>' % (self.name, self.creationDate)

class SimpleStorageItem(object):
    def __init__(self, key, lastModified, etag, size, storageClass, ownerId, ownerName):
        self.key = key
        self.lastModified = lastModified
        self.etag = etag
        self.size = size
        self.storageClass = storageClass
        self.ownerId = ownerId
        self.ownerName = ownerName
    def __repr__(self):
        return "<SimpleStorageItem key: %s size: %s etag: %s lastModified: %s>" % (self.key, self.size, self.etag, self.lastModified)

class BaseResponse(object):
    def __init__(self, tree):
        self.requestId = None
        self.success = True

class ListAllMyBucketsResult(BaseResponse):

    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.buckets = []
        r = tree.find('{http://s3.amazonaws.com/doc/2006-03-01/}Buckets')
        for e in r.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Bucket"):
            creationDate = e.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}CreationDate')
            name = e.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}Name')
            self.buckets.append(SimpleStorageBucket(name, creationDate))
    def __repr__(self):
        return '<ListAllMyBucketsResult requestId: %s buckets: %s>' % (self.requestId, self.buckets)

class ListBucketResult(BaseResponse):

    def __init__(self, tree):
        BaseResponse.__init__(self, tree)
        self.items = []
        for e in tree.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
            key = e.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}Key')
            lastModified = e.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}LastModified')
            etag = e.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}ETag')
            size = e.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}Size')
            self.items.append(SimpleStorageItem(key, lastModified, etag, int(size), None, None, None))
    def __repr__(self):
        return '<ListBucketResult requestId: %s items: %s>' % (self.requestId, self.items)


RESPONSE_OBJECTS = {
    '{http://s3.amazonaws.com/doc/2006-03-01/}ListAllMyBucketsResult': ListAllMyBucketsResult,
    '{http://s3.amazonaws.com/doc/2006-03-01/}ListBucketResult': ListBucketResult
}

class SimpleStorageService(object):

    S3_VERSION = "2006-03-01"
    S3_PROTOCOL = "http"
    S3_ENDPOINT = "s3.amazonaws.com"
    S3_ACCEPTABLE_ERRORS = [400, 403, 404, 409]
    S3_IGNORE_ERRORS = [204]

    def __init__(self, key = None, secret = None):
        self.key = key
        self.secret = secret

    def _executeBucketOperation(self, method, bucket, parameters = {}):

        def success(response):
            if len(response) > 0:
                tree = ElementTree.fromstring(response)
                if RESPONSE_OBJECTS.has_key(tree.tag):
                    return RESPONSE_OBJECTS[tree.tag](tree)
            return SuccessResponse(None)

        def failure(failure):
            if int(failure.value.status) in self.S3_IGNORE_ERRORS:
                return Response(None)
            elif int(failure.value.status) in self.S3_ACCEPTABLE_ERRORS:
                return ErrorResponse(ElementTree.fromstring(failure.value.response))
            else:
                return failure

        host = "%s.%s" % (bucket, self.S3_ENDPOINT)
        headers = { 'Host': host }
        if method == 'PUT':
            headers['Content-Length'] = '0'
        headers.update(self._getAuthorizationHeaders(method, "", "", headers, "/" + bucket + "/"))
        url = "%s://%s/%s" % (self.S3_PROTOCOL, self.S3_ENDPOINT, urllib.urlencode(parameters))
        return client.getPage(url, method = method, headers = headers).addCallbacks(success, failure)

    def _executeServiceOperation(self, method, parameters = {}):

        def success(response):
            if len(response) > 0:
                tree = ElementTree.fromstring(response)
                if RESPONSE_OBJECTS.has_key(tree.tag):
                    return RESPONSE_OBJECTS[tree.tag](tree)
            return SuccessResponse(None)


        def failure(failure):
            if int(failure.value.status) in self.S3_IGNORE_ERRORS:
                return Response(None)
            elif int(failure.value.status) in self.S3_ACCEPTABLE_ERRORS:
                return ErrorResponse() # Parse the error, etc.
            else:
                return failure

        headers = { 'Host': self.S3_ENDPOINT }
        headers.update(self._getAuthorizationHeaders(method, "", "", headers, "/"))
        url = "%s://%s/%s" % (self.S3_PROTOCOL, self.S3_ENDPOINT, urllib.urlencode(parameters))
        return client.getPage(url, method = method, headers = headers).addCallbacks(success, failure)

    #
    # GET / HTTP/1.1
    # Host: s3.amazonaws.com
    # Date: Wed, 01 Mar  2006 12:00:00 GMT
    # Authorization: AWS 15B4D3461F177624206A:xQE0diMbLRepdf3YB+FIEXAMPLE=
    #

    def listAllMyBuckets(self):
        return self._executeServiceOperation('GET')

    #
    # PUT / HTTP/1.1
    # Host: colorpictures.s3.amazonaws.com
    # Content-Length: 0
    # Date: Wed, 01 Mar  2006 12:00:00 GMT
    # Authorization: AWS 15B4D3461F177624206A:xQE0diMbLRepdf3YB+FIEXAMPLE=
    #

    def createBucket(self, bucket):
        return self._executeBucketOperation('PUT', bucket)

    #
    # DELETE / HTTP/1.1
    # Host: quotes.s3.amazonaws.com
    # Date: Wed, 01 Mar  2006 12:00:00 GMT
    # Authorization: AWS 15B4D3461F177624206A:xQE0diMbLRepdf3YB+FIEXAMPLE=
    #
    
    def deleteBucket(self, bucket):
        return self._executeBucketOperation('DELETE', bucket)

    #
    # GET /?prefix=N&marker=Ned&max-keys=40 HTTP/1.1
    # Host: quotes.s3.amazonaws.com
    # Date: Wed, 01 Mar  2006 12:00:00 GMT
    # Authorization: AWS 15B4D3461F177624206A:xQE0diMbLRepdf3YB+FIEXAMPLE=
    #

    def listBucket(self, bucket, prefix = None, marker = None, delimiter = None, max = None):
        parameters = { }
        if prefix:
            parameters['prefix'] = prefix
        if marker:
            parameters['marker'] = marker
        if delimiter:
            parameters['delimiter'] = delimiter
        if max:
            parameters['max'] = max
        return self._executeBucketOperation('GET', bucket, parameters)
    
    #
    # GET /?location HTTP/1.1
    # Host: quotes.s3.amazonaws.com
    # Date: Tue, 09 Oct 2007 20:26:04 +0000
    # Authorization: AWS 1ATXQ3HHA59CYF1CVS02:JUtd9kkJFjbKbkP9f6T/tAxozYY=
    #
    
    def getBucketLocation(self, bucket):
        parameters = { 'location': None }
        return self._executeBucketOperation('GET', bucket, parameters)

    #

    def putObjectData(self, bucket, key, data, contentType = "binary/octet-stream", headers = {}, public = False):

        def success(response):
            if len(response) > 0:
                tree = ElementTree.fromstring(response)
                if RESPONSE_OBJECTS.has_key(tree.tag):
                    return RESPONSE_OBJECTS[tree.tag](tree)
            return SuccessResponse(None)

        def failure(failure):
            if int(failure.value.status) in self.S3_IGNORE_ERRORS:
                return Response(None)
            elif int(failure.value.status) in self.S3_ACCEPTABLE_ERRORS:
                return ErrorResponse(ElementTree.fromstring(failure.value.response))
            else:
                return failure

        hash = base64.encodestring(md5.new(data).digest()).strip()
        
        generatedHeaders = { 'Content-Length': str(len(data)), 'Content-Type': contentType, 'Content-MD5': hash }
        
        amzHeaders = {}
        if public:
            amzHeaders['x-amz-acl'] = 'public-read'
        
        authHeaders = self._getAuthorizationHeaders('PUT', hash, contentType, amzHeaders, "/" + bucket + "/" + key)
        
        allHeaders = headers
        allHeaders.update(amzHeaders)
        allHeaders.update(generatedHeaders)
        allHeaders.update(authHeaders)
        
        return client.getPage("%s://%s/%s/%s" % (self.S3_PROTOCOL, self.S3_ENDPOINT, bucket, key), method = 'PUT',
           headers = allHeaders, postdata = data).addCallbacks(success, failure)

    def getObject(self, bucket, key):
        pass

    def deleteObject(self, bucket, key):
        pass

    def _canonalizeAmzHeaders(self, headers):
        keys = [k for k in headers.keys() if k.startswith("x-amz-")]
        keys.sort(key = str.lower)
        return "\n".join(map(lambda key: key + ":" + headers.get(key), keys))
    
    def _getAuthorizationHeaders(self, method, contentHash, contentType, headers, resource):
        date = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        amzHeaders = self._canonalizeAmzHeaders(headers)
        if len(amzHeaders):
            data = "%s\n%s\n%s\n%s\n%s\n%s" % (method, contentHash, contentType, date, amzHeaders, resource)
        else:
            data = "%s\n%s\n%s\n%s\n%s%s" % (method, contentHash, contentType, date, amzHeaders, resource)            
        authorization = "AWS %s:%s" % (self.key, base64.encodestring(hmac.new(self.secret, data, sha).digest()).strip())
        return { 'Authorization': authorization, 'Date': date }

    
