/**
 * Azure Active Directory (AAD) Graph API
 *
 * Provides an HTTPS interface to the AAD Graph API. This module requests an
 * access token for the application specified in the constructor and then uses
 * that token to make the API calls. If a call fails due to a 401 error, a new
 * new access token is obtained and the request is retried.
 *
 * @author Frank Hellwig
 * @module GraphAPI
 * @version 0.0.8
 */

var http = require('http'),
    https = require('https'),
    querystring = require('querystring'),
    strformat = require('strformat'),
    isAbsoluteUrl = require('is-absolute-url'),
    async = require('async'),
    _ = require('underscore'),
    url = require('url'),
    slice = Array.prototype.slice,
    AAD_LOGIN_HOSTNAME = 'login.windows.net',
    GRAPH_API_HOSTNAME = 'graph.windows.net',
    DEFAULT_API_VERSION = '1.5';

//-----------------------------------------------------------------------------
// PUBLIC
//-----------------------------------------------------------------------------

/**
 * Constructor
 */
function GraphAPI(tenant, clientId, clientSecret, apiVersion) {
    if (!(this instanceof arguments.callee)) {
        throw new Error("Constructor called as a function");
    }
    this.tenant = tenant;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.apiVersion = apiVersion || DEFAULT_API_VERSION;
    this.accessToken = null;
}

/**
 * HTTPS GET
 */
GraphAPI.prototype.get = function(ref, callback) {
    ref = strformat.apply(null, slice.call(arguments, 0, -1));
    callback = slice.call(arguments, -1)[0];
    this._request('GET', ref, null, wrap(callback));
}

/**
 * HTTPS GET
 * Will repeatedly make GET requests until there are no more results.
 * Callback is called for each object, with number of concurrent callbacks
 * limited by @concurrency {number}
 */
GraphAPI.prototype.getPages = function(ref, concurrency, callback) {
  ref = strformat.apply(null, slice.call(arguments, 0, -2));
  concurrency = slice.call(arguments, -2, -1)[0];
  callback = slice.call(arguments, -1)[0];
  this._getPages(ref, concurrency, callback);
}

/**
 * HTTPS GET with odata.nextList recursive call
 */
GraphAPI.prototype.getObjects = function(ref, objectType, callback) {
    ref = strformat.apply(null, slice.call(arguments, 0, -2));
    objectType = slice.call(arguments, -2, -1)[0];
    callback = slice.call(arguments, -1)[0];
    this._getObjects(ref, [], objectType, callback);
}

/**
 * HTTPS POST
 */
GraphAPI.prototype.post = function(ref, data, callback) {
    ref = strformat.apply(null, slice.call(arguments, 0, -2));
    data = slice.call(arguments, -2, -1)[0];
    callback = slice.call(arguments, -1)[0];
    this._request('POST', ref, data, wrap(callback));
}

/**
 * HTTPS PUT
 */
GraphAPI.prototype.put = function(ref, data, callback) {
    ref = strformat.apply(null, slice.call(arguments, 0, -2));
    data = slice.call(arguments, -2, -1)[0];
    callback = slice.call(arguments, -1)[0];
    this._request('PUT', ref, data, wrap(callback));
}

/**
 * HTTPS PATCH
 */
GraphAPI.prototype.patch = function(ref, data, callback) {
    ref = strformat.apply(null, slice.call(arguments, 0, -2));
    data = slice.call(arguments, -2, -1)[0];
    callback = slice.call(arguments, -1)[0];
    this._request('PATCH', ref, data, wrap(callback));
}

/**
 * HTTPS DELETE
 */
GraphAPI.prototype.delete = function(ref, callback) {
    ref = strformat.apply(null, slice.call(arguments, 0, -1));
    callback = slice.call(arguments, -1)[0];
    this._request('DELETE', ref, null, wrap(callback));
}

//-----------------------------------------------------------------------------
// PRIVATE
//-----------------------------------------------------------------------------

// The deltaLink is used for differential queries. Return undefined
// if it's not in the response
function getDeltaLink(response) {
  var deltaLink = response['aad.deltaLink'] || "";
  return url.parse(deltaLink, true).query.deltaLink;
}

// The nextLink can be found in 'odata.nextLink' for normal queries
// or in 'aad.nextLink' for differential queries.  Return undefined
// if it's not in the response
//
// Note that the nextLink format is a path in the normal case and
// an absolute URL, sans api-version, in the differential case
function getNextLink(response) {
  return response['odata.nextLink'] || response['aad.nextLink'];
}

// Only return the value and the correct number of arguments.
function wrap(callback) {
    return function(err, response) {
        if (err) {
            callback(err);
        } else if (typeof response === 'undefined') {
            // Handle 204 responses by not adding a second argument.
            callback(null);
        } else if (response.value) {
            callback(null, response.value, getDeltaLink(response));
        } else {
            callback(null, response);
        }
    }
}

// Recursive method that follows the odata.nextLink.
GraphAPI.prototype._getObjects = function(ref, objects, objectType, callback) {
    var self = this;
    self._request('GET', ref, null, function(err, response) {
        if (err) return callback(err);
        var value = response.value;
        for (var i = 0, n = value.length; i < n; i++) {
            if (value[i].objectType === objectType) {
                objects.push(value[i]);
            }
        }
        var nextLink = getNextLink(response);
        if (nextLink) {
            self._getObjects(nextLink, objects, objectType, callback);
        } else {
            callback(null, objects, getDeltaLink(response));
        }
    });
}

// If the results of the GET request is too large, the Graph API will return them
// a page a time.  Each page will have up to 200 DirectoryObject entities and up to 3000
// DirectoryLinkChange entities.
//
// The API currently only supports the following values for 'ref':
//   - directoryObjects (returns all changes, including DirectoryLinkChange)
//   - users (returns only objects of type User)
//   - contacts (returns only objects of type Contact)
//   - groups (returns only objects of type Group)
//
// Note: The Group objects only contain the basic information is returned and that
// DirectoryLinkChange objects only notify that something has changed in a relationship
// between two entities.  You will need to do subsequent GET requests to discover
// what happened
GraphAPI.prototype._getPages = function(ref, concurrency, callback) {
    var self = this;
    var isLastPage = false;
    var deltaLink;
    var q;

    // allow callbacks to work in parallel
    q = async.queue(function (task, queueCallback) {
        // return the pages as we get them, aynchronously
        callback(null, task.page, false, deltaLink, queueCallback);
    }, concurrency);

    async.whilst(
        // stop condition
        function() {
            return !isLastPage;
        },

        // worker
        function (callback) {
            self._request('GET', ref, null, function(err, response) {
                if (err) {
                  callback(err);
                  return;
                }
                ref = getNextLink(response);
                isLastPage = _.isUndefined(ref);
                if (isLastPage) deltaLink = getDeltaLink(response);

                // queue up this page of objects
                q.push({page: response.value}, function (err) {
                    if (err) callback(err);
                });

                callback(null);
              })
        },

        // done
        function(err) {
            if (err) {
              callback(err)
              return;
            }
            // wait until queue is empty
            q.drain = function() {
                // return an empty array of objects because the callback
                // has already been called per page
                callback(null, [], true, deltaLink);
            }
            // push an empty task to ensure drain callback is triggered
            // (in case the queue was empty before setting drain)
            q.push([])
        }
    );
}

// If there is an access token, perform the request. If not, get an
// access token and then perform the request.
GraphAPI.prototype._request = function(method, ref, data, callback) {
    method = arguments[0];
    ref = strformat.apply(null, slice.call(arguments, 1, -2));
    data = slice.call(arguments, -2, -1)[0];
    callback = slice.call(arguments, -1)[0];
    var self = this;
    if (self.accessToken) {
        self._requestWithRetry(method, ref, data, false, callback);
    } else {
        self._requestAccessToken(function(err, token) {
            if (err) {
                callback(err);
            } else {
                self.accessToken = token;
                self._requestWithRetry(method, ref, data, false, callback);
            }
        });
    }
}

// Performs the HTTPS request and tries again on a 401 error
// by getting another access token and repeating the request.
GraphAPI.prototype._requestWithRetry = function(method, ref, data, secondAttempt, callback) {
    var self = this;
    var path;

    // handle absolute URLs by assuming they have the complete path except for the api-version, which
    // is the case when paging through the results of a differential query (for example)
    if (isAbsoluteUrl(ref)) {
      path = [url.parse(ref).path];
    } else {
      path = ['/'];
      path.push(self.tenant);
      path.push('/');
      path.push(ref);
    }
    if (ref.indexOf('?') < 0) {
        path.push('?');
    } else {
        path.push('&');
    }
    path.push('api-version=');
    path.push(self.apiVersion);
    var options = {
        hostname: GRAPH_API_HOSTNAME,
        path: path.join(''),
        method: method,
        headers: {
            'Authorization': 'Bearer ' + self.accessToken
        }
    };
    httpsRequest(options, data, function(err, response) {
        if (err) {
            if (err.statusCode === 401 && !secondAttempt) {
                self._requestAccessToken(function(err, token) {
                    if (err) {
                        callback(err);
                    } else {
                        self.accessToken = token;
                        self._requestWithRetry(method, ref, data, true, callback);
                    }
                });
            } else {
                callback(err);
            }
        } else {
            callback(null, response);
        }
    });
}

// Gets an access token using the client id and secret.
GraphAPI.prototype._requestAccessToken = function(callback) {
    var query = {
        client_id: this.clientId,
        client_secret: this.clientSecret,
        grant_type: 'client_credentials',
        resource: 'https://' + GRAPH_API_HOSTNAME
    };
    var content = querystring.stringify(query);
    var options = {
        hostname: AAD_LOGIN_HOSTNAME,
        path: '/' + this.tenant + '/oauth2/token',
        method: 'POST'
    };
    httpsRequest(options, content, function(err, response) {
        if (err) {
            callback(err);
        } else {
            callback(null, response.access_token);
        }
    });
}

// Our own wrapper around the https.request method.
function httpsRequest(options, content, callback) {
    options.headers = options.headers || {};
    options.headers['Accept'] = 'application/json';
    if (!callback) {
        callback = content;
        content = null;
    } else if (typeof content === 'string') {
        options.headers['Content-Type'] = 'application/x-www-form-urlencoded';
        options.headers['Content-Length'] = content.length;
    } else if (content !== null && typeof content === 'object') {
        content = JSON.stringify(content);
        options.headers['Content-Type'] = 'application/json';
        options.headers['Content-Length'] = content.length;
    } else {
        content = null;
    }
    var req = https.request(options, function(res) {
        res.setEncoding('utf8');
        var buf = [];
        res.on('data', function(data) {
            buf.push(data);
        });
        res.on('end', function() {
            var data = buf.join('');
            if (data.length > 0) {
                data = JSON.parse(data);
            } else {
                data = null;
            }
            if (res.statusCode === 204) {
                callback(null); // no data
            } else if (res.statusCode >= 200 && res.statusCode <= 299) {
                callback(null, data); // success
            } else {
                if (data && data.error_description) {
                    data = data.error_description.split(/[\r\n]/)[0];
                } else if (data && data['odata.error']) {
                    data = data['odata.error'].message.value;
                } else {
                    data = null;
                }
                var err = new Error(errmsg(res.statusCode, data));
                err.statusCode = res.statusCode;
                callback(err);
            }
        });
    });
    req.on('error', function(err) {
        callback(err);
    })
    if (content) {
        req.write(content);
    }
    req.end();
}

// Creates an exception error message.
function errmsg(status, message) {
    message = message || '[no additional details]';
    return strformat('Graph API Error: {0} ({1}) {2}',
        status, http.STATUS_CODES[status], message);
}

//-----------------------------------------------------------------------------
// EXPORTS
//-----------------------------------------------------------------------------

module.exports = GraphAPI; // export the constructor
