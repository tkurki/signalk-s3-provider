/*
 * Copyright 2016 Teppo Kurki <teppo.kurki@iki.fi>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var Transform = require('stream').Transform;
const AWS = require('aws-sdk')
const debug = require('debug')('signalk-server:s3-provider')

function S3Provider(options) {
  Transform.call(this, {
    objectMode: false
  });
  this.options = options;
  AWS.config.credentials = new AWS.SharedIniFileCredentials()
}

require('util').inherits(S3Provider, Transform);

S3Provider.prototype.pipe = function(pipeTo) {
  const s3 = new AWS.S3()
  const bucket = this.options.bucket
  const params = {
    Bucket: this.options.bucket,
    Prefix: this.options.prefix
  }
  s3.listObjects(params).promise().then(data => {
    const jobs = data.Contents.map(item => function() {
      return new Promise((resolve, reject) => {
        console.log("Starting key " + item.Key)
        params.Key = item.Key
        const request = s3.getObject({
          Bucket: bucket,
          Key: item.Key
        })
        request.on('error', (err) => {
          console.log(err)
        })
        const stream = request.createReadStream()
        stream.on('end', resolve)
        stream.pipe(pipeTo, {
          end: false
        })
      })
    })
    var result = Promise.resolve()
    jobs.forEach(job => {
      result = result.then(() => job()).catch(() => job())
    })
  }).catch(error => {
    console.error(error)
  })
}


module.exports = S3Provider;
