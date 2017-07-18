//var serialPortLib = require('serialport');
//var SerialPort = serialPortLib.SerialPort;

function SerialModbusMaster(serialResource, options, silent) {

    this.silent = silent;
    this.myTurn = false;

    this.requestsQueue = [];
    this.currentRequest = null;
    this.requestAttempts = options.requestAttempts?options.requestAttempts:3;
    this.responseTimeout = options.responseTimeout?options.responseTimeout:1000;
    this.requestTimeout = options.requestTimeout?options.requestTimeout:0;
    this.serialResource = serialResource.addUser(this);

    this.receiveBuffer = new Buffer(256);
    this.receiveBuffer.currentPos = 0;
    this.receiveBuffer.parts = 0;
    this.receiveBuffer.crc = 0xFFFF;

    this.banList = [];
    this.probationList = [];
    this.banTimeout = options.banTimeout?options.banTimeout:10000;

    setInterval(function(context){ //remove one item from the ban list each interval
        if(context.banList.length){
            context.probationList.push(context.banList.shift());
        }
    }, this.banTimeout, this);
}

SerialModbusMaster.prototype.readHoldings = function(slave, register, schema, callback) {
    var context = this;
    var buf = new Buffer([0x00,0x03,0x00,0x00,0x00,0x00,0xC5,0xD3]);

    if(!schema){
        var schema = [{itemType:'W',count:count}];
    }

    var count = 0;
    for(var record of schema){
        switch(record.itemType) {
            case 'E'://Empty
            case 'W'://Word
            case 'I'://Int
            case 'B'://Binary
                count += record.count; //
                break;
            case 'F'://Float
            case 'DW'://Double word
            case 'DI'://Double int
                count += 2 * record.count; //
                break;
            default:
        }
    }

    buf[0] = slave;
    buf.writeUInt16BE(register, 2);
    buf.writeUInt16BE(count, 4);
    buf.writeUInt16LE(crc16(0xFFFF, buf, 6), 6);

    context.requestsQueue.push({
        slave:slave,
        telegramm:buf,
        expectedRespLen: 5 + count*2,
        schema:schema,
        attemptsLeft:context.requestAttempts,
        callback:callback
    });
};

SerialModbusMaster.prototype.writeHoldings = function(slave, register, schema, data, callback) {
    var context = this;

    if(!schema){
        var schema = [{itemType:'W',count:count}];
    }

    var count = 0;
    for(var record of schema){
        switch(record.itemType) {
            case 'E'://Empty
            case 'W'://Word
            case 'I'://Int
            case 'B'://Binary
                count += record.count; //
                break;
            case 'F'://Float
            case 'DW'://Double word
            case 'DI'://Double int
                count += 2 * record.count; //
                break;
            default:
        }
    }

    var buf = new Buffer(count * 2 + 9);

    buf[0] = slave;
    buf[1] = 16; // write multiple holdings;
    buf.writeUInt16BE(register, 2);
    buf.writeUInt16BE(count, 4);
    buf[6] = count * 2;

    var dataCount = count * 2 + 7;
    var dataPos = 7;
    var dataNum = 0;
    var shemaPos = 0;
    var shemaItemNum = 0;
    while ((dataPos < dataCount) && (shemaPos < schema.length)) {
        var schemaItem = schema[shemaPos];
        switch(schemaItem.itemType) {
            case 'E'://Empty
                dataPos += 2 * schemaItem.count; //
                shemaItemNum = schemaItem.count; //skip loose loops
                break;
            case 'W'://Word
                buf.writeUInt16BE(data[dataNum], dataPos);
                dataPos += 2;
                dataNum ++;
                break;
            case 'I'://Int
                buf.writeInt16BE(data[dataNum], dataPos);
                dataPos += 2;
                dataNum ++;
                break;
            case 'B'://Binary
                var binArr = data[dataNum];
                var tmp = 0;
                for(var i = 15; i > -1; i--){
                    tmp<<=1;
                    tmp += binArr[i]
                }
                buf.writeUInt16BE(tmp, dataPos);
                dataPos += 2;
                dataNum ++;
                break;
            case 'F'://Float
                buf.writeFloatBE(data[dataNum], dataPos);
                dataPos += 4;
                dataNum ++;
                break;
            case 'DW'://Double Word
                buf.writeUInt32BE(data[dataNum], dataPos);
                dataPos += 4;
                dataNum ++;
                break;
            case 'DI'://Double Int
                buf.writeInt32BE(data[dataNum], dataPos);
                dataPos += 4;
                dataNum ++;
                break;
            default:
                dataPos += 2;
                dataNum ++;
        }
        shemaItemNum++;
        if(shemaItemNum >= schemaItem.count){
            shemaItemNum = 0;
            shemaPos++;
        }
    }

    buf.writeUInt16LE(crc16(0xFFFF, buf, count * 2 + 7), count * 2 + 7);

    context.requestsQueue.push({
        slave:slave,
        telegramm:buf,
        expectedRespLen: 8,
        schema:null,
        attemptsLeft:context.requestAttempts,
        callback:callback
    });
};

SerialModbusMaster.prototype.go = function() {
    var context = this;
    setTimeout(function () {
        request(context);
    }, context.requestTimeout);
};

SerialModbusMaster.prototype.onData = function(data) {
    var context = this;

    if (!context.myTurn) {
        if (!context.silent)console.log(new Date().toISOString() + '  SerialModbusMaster ERROR: unexpected data: ' + data);
        return;
    }

    clearTimeout(context.responseTimer);

    if (!context.silent)console.log(new Date().toISOString() + '  SerialModbusMaster data: ' + JSON.stringify(data));

    if (context.receiveBuffer.currentPos + data.length < 256) {
        data.copy(context.receiveBuffer, context.receiveBuffer.currentPos);
        context.receiveBuffer.currentPos += data.length;
        context.receiveBuffer.parts++;
        context.receiveBuffer.crc = crc16(context.receiveBuffer.crc, data, data.length);

        if (context.receiveBuffer.crc === 0) { //make reaction faster on correct telegrams
            if ((context.receiveBuffer.currentPos === context.currentRequest.expectedRespLen
                && context.receiveBuffer[1] === context.currentRequest.telegramm[1]) //correct function and response length
                || (context.receiveBuffer.currentPos === 5
                && context.receiveBuffer[1] === (context.currentRequest.telegramm[1] + 0x80))) { //exception length
                parseResponse(context);

                context.myTurn = false;
            }
        }
    } else {
        if (!context.silent)console.log(new Date().toISOString() + '  SerialModbusMaster ERROR: receive buffer is full');
    }

    if (context.myTurn) {
        context.responseTimer = setTimeout(timeoutEnd, context.responseTimeout, context);
    }else{
        context.serialResource.userFinished();
    }
};

SerialModbusMaster.prototype.onError = function(error) {
    var context = this;
    if (!context.silent)console.log(new Date().toISOString() + '  SerialModbusMaster ERROR occurred in serial resource: ' + error);
};

function parseResponse(context) {
    if(context.receiveBuffer[0] === context.currentRequest.telegramm[0]){   //correct address

        var index = context.probationList.indexOf(context.receiveBuffer[0]) //remove from probation list
        if(index !== -1)context.probationList.splice(index,1);

        if(context.receiveBuffer[1] === context.currentRequest.telegramm[1] //correct function
            && context.receiveBuffer.currentPos === context.currentRequest.expectedRespLen){ //correct response length
            switch(context.receiveBuffer[1]) {
                case 3:
                    var data = [];
                    var dataCount = context.currentRequest.expectedRespLen - 2;
                    var dataPos = 3;
                    var shemaPos = 0;
                    var shemaItemNum = 0;
                    var schema = context.currentRequest.schema;
                    while ((dataPos < dataCount) && (shemaPos < schema.length)) {
                        var schemaItem = schema[shemaPos];
                        switch(schemaItem.itemType) {
                            case 'E'://Empty
                                dataPos += 2 * schemaItem.count; //
                                shemaItemNum = schemaItem.count; //skip loose loops
                                break;
                            case 'W'://Word
                                data.push(context.receiveBuffer.readUInt16BE(dataPos));
                                dataPos += 2;
                                break;
                            case 'I'://Int
                                data.push(context.receiveBuffer.readInt16BE(dataPos));
                                dataPos += 2;
                                break;
                            case 'B'://Binary
                                var tmp = context.receiveBuffer.readInt16BE(dataPos);
                                var binArr = [];
                                for(var i = 0; i < 16; i++){
                                    binArr.push(tmp & 1 === 1);
                                    tmp>>>=1;
                                }
                                data.push(binArr);
                                dataPos += 2;
                                break;
                            case 'F'://Float
                                data.push(context.receiveBuffer.readFloatBE(dataPos));
                                dataPos += 4;
                                break;
                            case 'DW'://Double Word
                                data.push(context.receiveBuffer.readUInt32BE(dataPos));
                                dataPos += 4;
                                break;
                            case 'DI'://Double Int
                                data.push(context.receiveBuffer.readInt32BE(dataPos));
                                dataPos += 4;
                                break;
                            default:
                                data.push(null);
                                dataPos += 2;
                        }
                        shemaItemNum++;
                        if(shemaItemNum >= schemaItem.count){
                            shemaItemNum = 0;
                            shemaPos++;
                        }
                    }
                    break;
                case 16:
                    var data = [context.receiveBuffer.readUInt16BE(2), context.receiveBuffer.readUInt16BE(4)]; //1st element = address of the 1st register, 2nd = number of registers written
                    break;
                default:
                    var error = 'uncnown function code';
            }

        } else if(context.receiveBuffer[1] === context.currentRequest.telegramm[1] + 0x80){
            var error = 'response exception';
        }else{
            var error = 'response wrong function code or length';
        }

    } else {
        var error = 'response wrong slave';
    }

    if(error)console.log(new Date().toISOString() + '  SerialModbusMaster ERROR: ' + error);
    if(typeof context.currentRequest.callback === "function") context.currentRequest.callback(error, data)
}

function request(context) {

    context.currentRequest = context.requestsQueue.shift();

    if(!context.currentRequest){
        context.serialResource.userFinished();
        return;
    }

    if (context.banList.indexOf(context.currentRequest.slave) >= 0) { //Slave is in ban list
        if (context.currentRequest && typeof context.currentRequest.callback === "function") {
            setTimeout(function (pendingRequest) {
                pendingRequest.callback('Slave ' + pendingRequest.slave + ' is in ban list', []);
            }, 1000, context.currentRequest);
        }
        setTimeout(function () {
            request(context);
        },0);
        return;
    }


    if (context.serialResource
        && typeof context.serialResource.write === "function"
        && typeof context.serialResource.userFinished === "function") {

        context.receiveBuffer.currentPos = 0;
        context.receiveBuffer.parts = 0;
        context.receiveBuffer.crc = 0xFFFF;

        context.serialResource.write(context.currentRequest.telegramm, function (err, results) {
            if (err) {
                console.log(new Date().toISOString() + '  SerialModbusMaster ERROR writing to serial resource: ' + err);
                context.currentRequest.callback("Error writing to serial resource" + err);
                context.serialResource.userFinished();
            } else {
                context.myTurn = true;
                context.receiveBuffer.crc = 0xFFFF;

                if (!context.silent)console.log(new Date().toISOString() + '  SerialModbusMaster OK writing to serial resource, result: ' + results + ' ' + JSON.stringify(context.currentRequest.telegramm));

                clearTimeout(context.responseTimer);
                context.responseTimer = setTimeout(timeoutEnd, context.responseTimeout, context);
            }
        });

    } else {
        console.log(new Date().toISOString() + '  SerialModbusMaster ERROR: serialResourse  is not properly configured');
    }
}

function timeoutEnd(context) {

    context.myTurn = false;

    if (!context.silent)console.log(new Date().toISOString() + '  SerialModbusMaster timeout end');

    context.currentRequest.attemptsLeft--;
    var index = context.probationList.indexOf(context.currentRequest.slave); //check if slave is in probation list
    if(index !== -1)context.currentRequest.attemptsLeft = 0;    //no more attempts in this case
    if (context.currentRequest.attemptsLeft) { // if attempts left
        context.requestsQueue.push(context.currentRequest); //append to the end of requests queue
    } else {
        context.banList.push(context.currentRequest.slave); //add slave to ban list
        if(index !== -1)context.probationList.splice(index,1); //remove newly banned slave from probation list

        if (typeof context.currentRequest.callback === "function") {
            context.currentRequest.callback("Slave doesn't reply, or reply is inconsistent", []);
        }
    }
    context.serialResource.userFinished();
}

module.exports = SerialModbusMaster;

var CrcTable = [
    0X0000, 0XC0C1, 0XC181, 0X0140, 0XC301, 0X03C0, 0X0280, 0XC241,
    0XC601, 0X06C0, 0X0780, 0XC741, 0X0500, 0XC5C1, 0XC481, 0X0440,
    0XCC01, 0X0CC0, 0X0D80, 0XCD41, 0X0F00, 0XCFC1, 0XCE81, 0X0E40,
    0X0A00, 0XCAC1, 0XCB81, 0X0B40, 0XC901, 0X09C0, 0X0880, 0XC841,
    0XD801, 0X18C0, 0X1980, 0XD941, 0X1B00, 0XDBC1, 0XDA81, 0X1A40,
    0X1E00, 0XDEC1, 0XDF81, 0X1F40, 0XDD01, 0X1DC0, 0X1C80, 0XDC41,
    0X1400, 0XD4C1, 0XD581, 0X1540, 0XD701, 0X17C0, 0X1680, 0XD641,
    0XD201, 0X12C0, 0X1380, 0XD341, 0X1100, 0XD1C1, 0XD081, 0X1040,
    0XF001, 0X30C0, 0X3180, 0XF141, 0X3300, 0XF3C1, 0XF281, 0X3240,
    0X3600, 0XF6C1, 0XF781, 0X3740, 0XF501, 0X35C0, 0X3480, 0XF441,
    0X3C00, 0XFCC1, 0XFD81, 0X3D40, 0XFF01, 0X3FC0, 0X3E80, 0XFE41,
    0XFA01, 0X3AC0, 0X3B80, 0XFB41, 0X3900, 0XF9C1, 0XF881, 0X3840,
    0X2800, 0XE8C1, 0XE981, 0X2940, 0XEB01, 0X2BC0, 0X2A80, 0XEA41,
    0XEE01, 0X2EC0, 0X2F80, 0XEF41, 0X2D00, 0XEDC1, 0XEC81, 0X2C40,
    0XE401, 0X24C0, 0X2580, 0XE541, 0X2700, 0XE7C1, 0XE681, 0X2640,
    0X2200, 0XE2C1, 0XE381, 0X2340, 0XE101, 0X21C0, 0X2080, 0XE041,
    0XA001, 0X60C0, 0X6180, 0XA141, 0X6300, 0XA3C1, 0XA281, 0X6240,
    0X6600, 0XA6C1, 0XA781, 0X6740, 0XA501, 0X65C0, 0X6480, 0XA441,
    0X6C00, 0XACC1, 0XAD81, 0X6D40, 0XAF01, 0X6FC0, 0X6E80, 0XAE41,
    0XAA01, 0X6AC0, 0X6B80, 0XAB41, 0X6900, 0XA9C1, 0XA881, 0X6840,
    0X7800, 0XB8C1, 0XB981, 0X7940, 0XBB01, 0X7BC0, 0X7A80, 0XBA41,
    0XBE01, 0X7EC0, 0X7F80, 0XBF41, 0X7D00, 0XBDC1, 0XBC81, 0X7C40,
    0XB401, 0X74C0, 0X7580, 0XB541, 0X7700, 0XB7C1, 0XB681, 0X7640,
    0X7200, 0XB2C1, 0XB381, 0X7340, 0XB101, 0X71C0, 0X7080, 0XB041,
    0X5000, 0X90C1, 0X9181, 0X5140, 0X9301, 0X53C0, 0X5280, 0X9241,
    0X9601, 0X56C0, 0X5780, 0X9741, 0X5500, 0X95C1, 0X9481, 0X5440,
    0X9C01, 0X5CC0, 0X5D80, 0X9D41, 0X5F00, 0X9FC1, 0X9E81, 0X5E40,
    0X5A00, 0X9AC1, 0X9B81, 0X5B40, 0X9901, 0X59C0, 0X5880, 0X9841,
    0X8801, 0X48C0, 0X4980, 0X8941, 0X4B00, 0X8BC1, 0X8A81, 0X4A40,
    0X4E00, 0X8EC1, 0X8F81, 0X4F40, 0X8D01, 0X4DC0, 0X4C80, 0X8C41,
    0X4400, 0X84C1, 0X8581, 0X4540, 0X8701, 0X47C0, 0X4680, 0X8641,
    0X8201, 0X42C0, 0X4380, 0X8341, 0X4100, 0X81C1, 0X8081, 0X4040
];

function crc16(prevCRC, bytes, length){
    var crc = prevCRC;

    for(var i = 0, l = length; i < l; i++){
        crc = ((crc >> 8) ^ CrcTable[(crc ^ bytes[i]) & 0xFF]);
    }

    return crc;
}