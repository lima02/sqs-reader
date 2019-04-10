var stringify = require('csv-stringify');
const fs = require('fs');
const AWS = require('aws-sdk');
// Set the region 
AWS.config.update({ profile: 'rfs', region: 'ap-southeast-2' });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

const ACCOUNT_ID = '****';
const AWS_REGION = 'ap-southeast-2';
const QUEUE_NAME = 'jaggaer-procurepoint-connector-dlq';
// const OUTPUT_PATH = `./${QUEUE_NAME}_${(new Date()).toIsoString()}.csv`;

const queueURL = `https://sqs.${AWS_REGION}.amazonaws.com/${ACCOUNT_ID}/${QUEUE_NAME}`;

// const worker = async () => {
(async () => {
  let messageCount = 0;
  const arr = [];
  const params = {
    AttributeNames: [
      "SentTimestamp"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
      "All"
    ],
    QueueUrl: queueURL,
    VisibilityTimeout: 60,
    WaitTimeSeconds: 0
  };

  do {
    let data = await sqs.receiveMessage(params).promise();
    messageCount = data.Messages ? data.Messages.length : 0;
    console.log(`received ${messageCount} messages`);
    if (data.Messages) {
      data.Messages.forEach(e => {
        // console.log(JSON.stringify(e));
        let line = [];
        let dt = new Date(Math.abs(parseInt(e.Attributes.SentTimestamp) / 1000));
        // console.log(dt.toISOString());
        line.push(dt);
        line.push(e.Body);
        if (e.MessageAttributes) {
          if (e.MessageAttributes["BucketName"]) line.push(e.MessageAttributes["BucketName"].StringValue);
          if (e.MessageAttributes["ObjectKey"]) line.push(decodeURIComponent(e.MessageAttributes["ObjectKey"].StringValue));
          if (e.MessageAttributes["JaggaerAPIRequest"]) line.push(e.MessageAttributes["JaggaerAPIRequest"].StringValue);
          if (e.MessageAttributes["JaggaerAPIResponse"]) line.push(e.MessageAttributes["JaggaerAPIResponse"].StringValue);
          if (e.MessageAttributes["Exception"]) line.push(e.MessageAttributes["Exception"].StringValue);
        }
        arr.push(line);
      });
    }
  } while (messageCount > 0);

  console.log(`Total: ${arr.length} messages`);
  stringify(arr, function (err, stringify_output) {
    console.log('saving csv file...');
    const tzoffset = (new Date()).getTimezoneOffset() * 60000; //offset in milliseconds
    const localISOTime = (new Date(Date.now() - tzoffset)).toISOString().split('.')[0].replace(/:/g, '');
    const filePath = `C:\\temp\\jaggaer-uat\\${QUEUE_NAME}_${localISOTime}.csv`;
    fs.writeFile(filePath, stringify_output, function (err) {
      if (err) {
        return console.log(err);
      }

      console.log(`The file was saved to ${filePath}!`);
    });
  });
})();
