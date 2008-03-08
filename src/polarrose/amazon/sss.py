
from datetime import datetime
from time import gmtime, strftime
import base64, hmac, sha, md5

from twisted.web import client

class SimpleStorageBucket(object):
    pass

class SimpleStorageService(object):

    S3_PROTOCOL = "http"
    S3_ENDPOINT = "s3.amazonaws.com"

    def __init__(self, key = None, secret = None):
        self.key = key
        self.secret = secret

    def createBucket(self, bucket):
        headers = { 'Content-Length': 0 }
        headers.update(self._getAuthorizationHeaders('PUT', "", "", headers, "/" + bucket))
        return client.getPage("%s://%s/%s" % (self.S3_PROTOCOL, self.S3_ENDPOINT, bucket), method = 'PUT',
                headers = headers)
    
    def deleteBucket(self, bucket):
        headers = self._getAuthorizationHeaders('DELETE', "", "", {}, "/" + bucket)
        return client.getPage("%s://%s/%s" % (self.S3_PROTOCOL, self.S3_ENDPOINT, bucket), method = 'DELETE',
                headers = headers)

    def putObjectData(self, bucket, key, data, contentType = "binary/octet-stream", headers = {}, public = False):
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
           headers = allHeaders, postdata = data)
        pass

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
        data = "%s\n%s\n%s\n%s\n%s\n%s" % (method, contentHash, contentType, date, self._canonalizeAmzHeaders(headers), resource)
        authorization = "AWS %s:%s" % (self.key, base64.encodestring(hmac.new(self.secret, data, sha).digest()).strip())
        return { 'Authorization': authorization, 'Date': date }

    
