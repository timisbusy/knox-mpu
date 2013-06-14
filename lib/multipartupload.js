var EventEmitter = require('events').EventEmitter,
    Batch = require('batch'),
    fs = require('fs'),
    path = require('path'),
    os = require('os'),
    util = require('util'),
    parse = require('./parse');

/**
 * Initializes a Amazon S3 Multi part file upload with the given options
 */
function MultiPartUpload(opts, callback) {
    if (!opts.client || !opts.objectName) {
        throw new Error('MultiPart upload must be created from a client and provide a object name');
    }

    if (!opts.stream && !opts.file) {
        throw new Error('MultiPart upload must be passed either a stream or file parameter');
    }

    if (opts.stream && opts.file) {
        throw new Error('You cannot provide both a stream and a file to upload');
    }

    this.objectName = opts.objectName;
    this.headers = opts.headers || {};
    this.client = opts.client;
    this.partSize = opts.partSize || 5242880; // 5MB default
    this.uploadId = null;
    this.uploads = new Batch();
    this.noDisk = opts.noDisk; // if true, uses in-memory data property of the part instead of a temp file
    this.retries = opts.retries || 5;

    // initialise the tmp directory based on opts (fallback to os.tmpDir())
    this.tmpDir = !this.noDisk && (opts.tmpDir || os.tmpDir());

    if (opts.stream) {
        this._putStream(opts.stream, callback);
    } else {
        this._putFile(opts.file, callback);
    }

}
util.inherits(MultiPartUpload, EventEmitter);

/**
 * Attempts to initiate the MultiPartUpload request (gets the upload ID)
 */
MultiPartUpload.prototype._initiate = function(callback) {
    // Send the initiate request
    var req = this.client.request('POST', this.objectName + '?uploads', this.headers),
        mpu = this;

    // Handle the xml response
    parse.xmlResponse(req, function(err, body) {

        if (err) return callback(err);
        if (!body.UploadId) return callback('Invalid upload ID');

        mpu.uploadId = body.UploadId;
        mpu.emit('initiated', body.UploadId);
        return callback(null, body.UploadId);
    });

    req.end();
};

/**
 * Streams a file to S3 using a multipart form upload
 *
 * Divides the file into separate files, and then writes them to Amazon S3
 */
MultiPartUpload.prototype._putFile = function(file, callback) {
    if (!file) return callback('Invalid file');

    var mpu = this;

    fs.exists(file, function(exists) {
        if (!exists) {
            return callback('File does not exist');
        }

        var stream = fs.createReadStream(file);
        mpu._putStream(stream, callback);
    });
};

/**
 * Streams a stream to S3 using a multipart form upload.
 *
 * It will attempt to initialize the upload (if not already started), read the stream in,
 * write the stream to a temporary file of the given partSize, and then start uploading a part
 * each time a part is available
 */
MultiPartUpload.prototype._putStream = function(stream, callback) {

    if (!stream) return callback('Invalid stream');

    var mpu = this;

    if (!this.uploadId) {
        this._initiate(function(err, uploadId) {
            if (err || !uploadId) return callback('Unable to initiate stream upload');
        });
    }
    // Start handling the stream straight away
    mpu._handleStream(stream, callback);
};

/**
  Handles an incoming stream, divides it into parts, and uploads it to S3
 **/
MultiPartUpload.prototype._handleStream = function(stream, callback) {

    var mpu = this,
        parts = [],
        results = [],
        current,
        ended = false;

    // Create a new part
    function newPart() {
        var partId = parts.length + 1,
            partFileName = !mpu.noDisk && path.resolve(path.join(mpu.tmpDir, 'mpu-' + this.objectName + '-' + random_seed() + '-' + (mpu.uploadId || Date.now()) + '-' + partId)),
            partFile = !mpu.noDisk && fs.createWriteStream(partFileName),
            part = {
                id: partId,
                stream: partFile,
                fileName: partFileName,
                length: 0,
                data: null
            };

        parts.push(part);
        return part;
    }

    function partReady(part) {
        if (!part) {
            // this shouldn't happen, but just in case.
            if (ended) { return mpu._uploadsComplete(results, callback); }
            return;
        }

        // skip batch and apply backpressure for noDisk option

        if (!part.stream) {

            stream.pause();

            var retried = 0;

            function attemptUpload () {
                mpu._uploadPart(part, function (err, result) {
                    if (err) {
                        if (retried >= mpu.retries) {
                            return callback(err);
                        }
                        retried++;
                        return attemptUpload();
                    }
                    results.push(result);
                    delete part.data;
                    if (ended) {
                        mpu._uploadsComplete(results, callback);
                    } else {
                        stream.resume();
                    }
                });

            }

            return attemptUpload();
        }

        // Ensure the stream is closed
        if (part.stream && part.stream.writable) {
            part.stream.end();
        }

        mpu.uploads.push(mpu._uploadPart.bind(mpu, part));
    }

    // Handle the data coming in
    stream.on('data', function(buffer) {
        if (!current) {
            current = newPart();
        }

        if (current.stream) {
            current.stream.write(buffer);
        } else {
            if (!current.data) {
                current.data = buffer;
            } else {
                current.data = Buffer.concat([current.data, buffer]);
            }
        }
        current.length += buffer.length;

        // Check if we have a part
        if (current.length >= mpu.partSize) {
            partReady(current);
            current = null;
        }
    });

    // Handle the end of the stream
    stream.on('end', function() {
        ended = true;
        if (current) {
            partReady(current);
        } else if (parts.length === 0) {
            // this case means that the stream ended with no 'data' events received
            return mpu._uploadsComplete(results, callback);
        }
        // Wait for the completion of the uploads
        if (!mpu.noDisk) {
            return mpu._completeUploads(callback);
        }
    });

    // Handle errors
    stream.on('error', function(err) {
        // Clean up
        return callback(err);
    });
};

/**
  Uploads a part, or if we are not ready yet, waits for the upload to be initiated
  and will then upload
 **/
MultiPartUpload.prototype._uploadPart = function(part, callback) {
    // If we haven't started the upload yet, wait for the initialization
    if (!this.uploadId) {
        return this.on('initiated', this._uploadPart.bind(this, part, callback));
    }

    var url = this.objectName + '?partNumber=' + part.id + '&uploadId=' + this.uploadId,
        headers = { 'Content-Length': part.length },
        req = this.client.request('PUT', url, headers),
        partStream = !this.noDisk && fs.createReadStream(part.fileName),
        mpu = this,
        errors = [],
        result;

    // Wait for the upload to complete
    req.on('response', function(res) {
        if (res.statusCode != 200) return errors.unshift({part: part.id, message: 'Upload failed with status code '+res.statusCode});

        // Grab the etag and return it
        var etag = res.headers.etag,
            _result = {part: part.id, etag: etag, size: part.length};

        mpu.emit('uploaded', _result);
        result = _result, req.end();
        // Remove the temporary file
        if (part.fileName) {
            fs.unlink(part.fileName, function(err) {
                if (!err) delete part.fileName;
            });
        }
    });

    // Handle close event and call back
    req.on('close', function() {
        if (result) { return callback(null, result); }
        if (errors.length < 1) { return callback(new Error("recieved 'close' event with no results or errors."));}
        callback(new Error(JSON.stringify(errors[0])));
    });


    // Handle errors
    req.on('error', function(err) {
        var error = {part: part.id, message: err};
        mpu.emit('error', error);
        errors.unshift(error);
    });

    if (!this.noDisk) {
        partStream.pipe(req);
    } else {
        req.write(part.data);
        req.end();
    }
    mpu.emit('uploading', part.id);
};

/**
  Indicates that all uploads have been started and that we should wait for completion
 **/
MultiPartUpload.prototype._completeUploads = function(callback) {
    var mpu = this;
    this.uploads.end(function(err, results) {
        if (err) return callback(err);
        mpu._uploadsComplete(results, callback);
    });
};

/**
  All uploads have been completed and we should end the mpu and callback with results
 **/

MultiPartUpload.prototype._uploadsComplete = function(results, callback) {
    var mpu = this
      , size = 0
      , parts = "";

    for (var i = 0; i < results.length; i++) {
        var value = results[i];
        size += value.size;
        parts += util.format('<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>', value.part, value.etag);
    }

    var req = mpu.client.request('POST', mpu.objectName + '?uploadId=' + mpu.uploadId);

    // Register the response handler
    parse.xmlResponse(req, function(err, body) {
        if (err) return callback(err);
        delete body.$;
        body.size = size;
        mpu.emit('completed', body);
        return callback(null, body);
    });

    // Write the request
    req.write('<CompleteMultipartUpload>' + parts + '</CompleteMultipartUpload>');
    req.end();
};

module.exports = MultiPartUpload;

function random_seed(){
    return 'xxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);});
}
