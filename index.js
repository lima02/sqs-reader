const stringify = require('csv-stringify/lib/sync');
const fs = require('fs');
const AWS = require('aws-sdk');

// Set the region 
AWS.config.update({ profile: 'rfs', region: 'ap-southeast-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

const ACCOUNT_ID = '***';
const AWS_REGION = 'ap-southeast-2';
const QUEUE_NAME = 'jaggaer-vendor-connector-dlq';
const VISIBILITY_TIMEOUT = 300;

const queueURL = `https://sqs.${AWS_REGION}.amazonaws.com/${ACCOUNT_ID}/${QUEUE_NAME}`;

(async () => {
  let messageCount = 0;
  let totalMessageCount = 0;
  const params = {
    AttributeNames: [
      "SentTimestamp"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
      "All"
    ],
    QueueUrl: queueURL,
    VisibilityTimeout: VISIBILITY_TIMEOUT,
    WaitTimeSeconds: 0
  };

  const tzoffset = (new Date()).getTimezoneOffset() * 60000; //offset in milliseconds
  const localISOTime = (new Date(Date.now() - tzoffset)).toISOString().split('.')[0].replace(/:/g, '');
  const filePath = `C:\\temp\\jaggaer-uat\\${QUEUE_NAME}_${localISOTime}.csv`;
  
  const stream = fs.createWriteStream(filePath, {flags:'a'});
  
  // CSV Headers
  let arr = [['DateTime', 'S3BucketName', 'S3ObjectKey', 'CompanyName', 'ABN', 'User Alias', 'SAP Vendor Id', 'Operation', 'JaggaerAPIRequest', 'JaggaerAPIResponse', 'Exception', 'Stack Trace']];
  let stringify_output = stringify(arr);
  stream.write(`${stringify_output}`);
  
  do {
    let data = await sqs.receiveMessage(params).promise();
    messageCount = data.Messages ? data.Messages.length : 0;
    totalMessageCount += messageCount;
    console.log(`received ${messageCount} messages. Total received so far: ${totalMessageCount}`);
    if (data.Messages) {
      let arr = [];
      data.Messages.forEach(e => {
        let line = [];
        let dt = new Date(parseInt(e.Attributes.SentTimestamp));
        line.push(`${dt.toLocaleDateString('en-AU')} ${dt.toLocaleTimeString('en-AU')}`);
        // line.push(e.Body);
        if (e.MessageAttributes) {
          if (e.MessageAttributes["BucketName"]) line.push(e.MessageAttributes["BucketName"].StringValue);
          if (e.MessageAttributes["ObjectKey"]) line.push(decodeURIComponent(e.MessageAttributes["ObjectKey"].StringValue));
          if (e.MessageAttributes["JaggaerAPIRequest"]) {
            let request = JSON.parse(e.MessageAttributes["JaggaerAPIRequest"].StringValue);
            line.push(request.companyData.company.companyInfo.companyName || '');
            line.push(request.companyData.company.companyInfo.vat || '');
            line.push(request.companyData.company.companyInfo.userAlias || '');
            line.push(request.companyData.company.companyInfo.extUniqueCode || '');
            line.push(request.companyData.company.operationCode || '');
            line.push(e.MessageAttributes["JaggaerAPIRequest"].StringValue);
          }
          if (e.MessageAttributes["JaggaerAPIResponse"]) {
            let msg = e.MessageAttributes["JaggaerAPIResponse"].StringValue;
            line.push(msg);
            let msgObj = JSON.parse(msg);
            line.push(msgObj.returnMessage);
          }
          if (e.MessageAttributes["Exception"]) {
            line.push('');
            line.push('');
            line.push('');
            line.push('');
            line.push('');
            line.push('');
            line.push('');

            let ex = e.MessageAttributes["Exception"].StringValue;
            line.push(ex.split(':')[0]);
            line.push(ex);
          }
        }
        arr.push(line);

        
      });
      let stringify_output = stringify(arr);
      stream.write(`${stringify_output}`);
    }
  } while (messageCount > 0);

  stream.end();
})();
