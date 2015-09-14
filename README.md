# azure-graphapi

Node.js package for making Azure Active Directory Graph API calls

v0.0.8

## Installation

```
npm install azure-graphapi --save
```

## Usage

```javascript
var GraphAPI = require('azure-graphapi');

var graph = new GraphAPI(tenant, clientId, clientSecret);
// The tenant, clientId, and clientSecret are usually in a configuration file.

graph.get('users/a8272675-dc21-4ff4-bc8d-8647830fa7db', function(err, user) {
    if (!err) {
        console.dir(user);
    }
}
```

## Details

This package provides an HTTPS interface to the [Azure Active Directory Graph API](https://msdn.microsoft.com/Library/Azure/Ad/Graph/api/api-catalog). You will need the tenant (i.e., domain) of your Azure AD instance as well as an application within that AD instance that has permissions to access your directory. This application is identified by a `clientId` and authenticated using a `clientSecret`. The `clientSecret` is also called the application key.

The typical verbs are supported (GET, POST, PUT, PATCH, and DELETE). The `getObjects` method is useful for reading a large number of objects. Azure AD limits each response to 100 objects. The `getObject` method follows the `odata.nextLink` and accumulates all objects of a specific object type.

## Interface

This section describes the constructor returned by this module and the available instance methodss.

### Constructor

#### GraphAPI(tenant, clientId, clientSecret, [apiVersion])

Creates a new `GraphAPI` instance. If the `apiVersion` is not specified, it defaults to version 1.5.

```javascript
var GraphAPI = require('azure-graphapi');

var graph = new GraphAPI(tenant, clientId, clientSecret);
```

### Methods

Each of these methods takes `ref` parameter with optional replacement `args`. These are passed to [strformat](https://github.com/fhellwig/strformat) to create the actual URI reference. This allows for the following constructs:

```javascript
var person = {
    userId: 'a8272675-dc21-4ff4-bc8d-8647830fa7db'
};

graph.get('users/{0}', person.userId, function(err, user) {
    if (!err) {
        console.dir(user);
    }
}
```

The following REST-style URI reference is also available:

```javascript
var person = {
    userId: 'a8272675-dc21-4ff4-bc8d-8647830fa7db'
};

graph.get('users/{userId}', person, function(err, user) {
    if (!err) {
        console.dir(user);
    }
}
```

In all cases, do *not* prefix the `ref` with a forward slash and do *not* add the API version as this is added automatically based on the API version specified in the constructor.

#### graph.get(ref [,args...], callback)

Performs an HTTPS GET request. The callback signature is `callback(err, response, deltaLink)`.

#### graph.post(ref [,args...], data, callback)

Performs an HTTPS POST request. The `data` is the request object. The callback signature is `callback(err, response)`.

#### graph.put(ref [,args...], data, callback)

Performs an HTTPS PUT request. The `data` is the request object. The callback signature is `callback(err, response)`.

#### graph.patch(ref [,args...], data, callback)

Performs an HTTPS PATCH request. The `data` is the request object. The callback signature is `callback(err, response)`.

#### graph.delete(ref [,args...], callback)

Performs an HTTPS DELETE request. The callback signature is `callback(err)`.

#### graph.getObjects(ref [,args...], objectType, callback)

Performs an HTTPS GET request and accumulates all objects having the specified `objectType` (e.g., "User"). The callback signature is `callback(err, objects, deltaLink)`. This method follows either the `odata.nextLink` or `aad.nextLink` property in the response and continues until no more batches of objects are available.

### Differential queries

Azure supports [Differential Queries](https://msdn.microsoft.com/en-us/library/azure/jj836245.aspx) by passing the `deltaLink` request parameter.  Both `get` and `getObjects` callbacks will return the deltaLink that should be used with the *next* request.  For example:

```javascript
graph.getObjects('users?deltaLink={0}', getLastDeltaLink(), 'User', function(err, users, deltaLink) {
    ...
    setLastDeltaLink(deltaLink || "");
})
```

## Notes

1. The HTTPS request logic parses out the `error_description` and `odata.error` messages from JSON responses to unsuccessful requests. These become part of the error message in the `err` object provided to the callback method.
2. If the request data is a string, instead of a JavaScript object, it is assumed to be form data and is sent as `application/x-www-form-urlencoded` content instead of `application/json`.
3. The callback is passed only one argument (err) if the response status code is 204 (No Content). This is important when using libraries such as the [async utilities](https://github.com/caolan/async).
4. If the response has a `value` property, it is that value that is passed to the callback function and not the entire response object.

## License

(The MIT License)

Copyright (c) 2015 Frank Hellwig

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
