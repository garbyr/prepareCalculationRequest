// dependencies
const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });


exports.handler = (event, context, callback) => {
    //parse the event from SNS
    console.log(event.Records[0].Sns.Message);
    var messageObj = event.Records[0].Sns.Message;
    var message = JSON.parse(messageObj);
    console.log(JSON.stringify(messageObj));
    console.log(messageObj);
    //execute function 
    mainProcess(context, event, message.requestUUID, message.ICIN, message.NAV, message.dateSequence, message.dateTime, message.sequence, message.category, message.frequency, message.description, message.user);
}


sendLambdaSNS = function (event, context, message, topic, subject) {
    var sns = new aws.SNS();
    var params = {
        Message: JSON.stringify(message),
        Subject: subject,
        TopicArn: topic
    };
    sns.publish(params, context.done);
    return null;
}


mainProcess = function (context, event, requestUUID, ICIN, NAV, dateSequence, dateTime, sequence, category, frequency, description, user) {
    //write to the database
    var doc = require('dynamodb-doc');
    var dynamo = new doc.DynamoDB();
    var sequenceWeekFloor = (sequence - 500);
    console.log("sequence in "+sequence);
    console.log("sequence floor "+sequenceFloor);
    var params = {
        TableName: 'NavHistory',
        // IndexName: 'index_name', // optional (if querying an index)

        // Expression to filter on indexed attributes
        KeyConditionExpression: '#hashkey = :hk_val AND #rangekey > :rk_val',
        ExpressionAttributeNames: {
            '#hashkey': 'ICIN',
            '#rangekey': 'Sequence',
        },
        ExpressionAttributeValues: {
            ':hk_val': ICIN,
            ':rk_val': sequenceFloor,
        }
    };
    dynamo.query(params, function (err, data) {
        if (err) {
            console.log("ERROR", err);
            context.fail();
        }
        else {
            console.log("SUCCESS", data);
            //sort out the array
            var navArray= data.Items;
            //publish to SNS
            var message = {
                updateShareClassDetail: "Yes",
                updateShareClassAudit: "Yes",
                requestUUID: requestUUID,
                ICIN: ICIN,
                sequence: sequence,
                category: category,
                frequency: frequency,
                description: description,
                user: user,
                navArray: navArray
            };
            sendLambdaSNS(event, context, message, "someTopic", "calculation request");
        }
    });
}