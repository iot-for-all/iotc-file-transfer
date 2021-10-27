// Azure function to handle file uploads through IoT Central's data export feature

module.exports = async function (context, req) {
    context.log.info('executing function');
    // built-in modules
    const fs = require('fs');
    const path = require("path");

    // open source modules that need to be npm installed
    const glob = require("glob");
    const zlib = require('zlib');

    // paths
    const baseDir = path.join("c:", "home", "site", "wwwroot", "upload", "files");
    const tempDir = path.join(baseDir, "temp-uploads");
    const uploadDir = path.join(baseDir, "file-uploads");
    const deadLetterDir = path.join(baseDir, "dead-letter");
    
    // variables
    const deadLetterExpireTimeInHours = 12;
    let status_code = 200;
    let error_message = "";

    try {
        context.log.info('checking directory existence');
        createDirectoriesIfNotExist();

        // pull the message part meta data from the message properties
        context.log.info('checking request properties');
        const [deviceId, id, filepath, filename, part, maxPart, compression] = getRequestProperties(context, req);
        const confirmationFile = path.join(tempDir, (deviceId + "." + id + ".confirm"));

        // log new file part
        context.log.info("device-id: " + deviceId + " file-id: " + id + " part: " + part + " filepath: " + filepath + " filename: " + filename);
        // only the confirmation message will contain the maxPart field
        if(maxPart === 0) {
            context.log.info('writing file chunk');
            fs.writeFileSync(path.join(tempDir, (deviceId + "." + id + "." + part)), req.body.telemetry.data);
            context.log.info('write complete');

            if(fs.existsSync(confirmationFile)){
                // it is possible that the function processes a chunk message AFTER it has received the confirmation message if they arrived out of order.
                const confirmData = fs.readFileSync(confirmationFile, {encoding:'utf8', flag:'r'});
                context.log.info(`confirmation file read: ${confirmData}`);
                const confirmDataParsed = JSON.parse(confirmData);

                let filePartCount = glob.sync(path.join(tempDir, (deviceId + "." + id + ".*"))).length;
                if (filePartCount === confirmDataParsed.maxPart) {
                    // if this message was the last message in the multi-part, proceed to aggregate.
                    await aggregateChunks(context, deviceId, id, confirmDataParsed.maxPart, confirmDataParsed.compression, filepath, filename, confirmationFile);
                }
            }
        } else {
            context.log.info('processing final message');
            
            context.log.info('writing confirmation file');
            // save confirmation information to file in case it arrived out of order.
            fs.writeFileSync(confirmationFile, JSON.stringify({maxPart, compression}));
            context.log.info('write complete');

            // check to see if all the file parts are available
            let filePartCount = glob.sync(path.join(tempDir, (deviceId + "." + id + ".*"))).length;
            if (filePartCount === maxPart) {
                // if this message was the last message in the multi-part, proceed to aggregate.
                await aggregateChunks(context, deviceId, id, maxPart, compression, filepath, filename, confirmationFile);
            } else {
                context.log.warn(`Missing message part. received ${filePartCount}, expected ${maxPart}`);
            }
        }
        
        // check for expired files in temp directory and dead letter them
        cleanupDeadletter();
    } catch (e) {
        // log any exceptions as errors
        context.log.error("Exception thrown: " + e);
        error_message = e;
        status_code = 500;
    } finally {
        // return success or failure
        context.res = {
            status: status_code,
            body: error_message
        };
        context.done();
    }

    function createDirectoriesIfNotExist() {
        // make sure the needed directories are available
        if (!fs.existsSync(tempDir)) {
            fs.mkdirSync(tempDir, { recursive: true });
        }
        if (!fs.existsSync(deadLetterDir)) {
            fs.mkdirSync(deadLetterDir, { recursive: true });
        }
        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }
    }

    function getRequestProperties(context, req) {
        // pull the message part meta data from the message properties
        let deviceId = "";
        let id = "";
        let filepath = "";
        let filename = "";
        let part = 0;
        let maxPart = 0;
        let compression = "none";

        // check to make sure all the needed message properties have been sent
        if ("deviceId" in req.body) {
            deviceId = req.body.deviceId;
        } else {
            throw "Missing body property: deviceId";
        }
        
        if ("id" in req.body.messageProperties) {
            id = req.body.messageProperties.id;
        } else {
            throw "Missing message property: id";
        }

        if ("filepath" in req.body.messageProperties) {
            filepath = path.normalize(req.body.messageProperties.filepath);
            filename = path.basename(filepath);
            filepath = path.dirname(filepath);
        } else {
            throw "Missing message property: filepath";
        }

        if ("part" in req.body.messageProperties) {
            part = req.body.messageProperties.part;
        } else {
            throw "Missing message property: part";
        }

        // check if optional message properties have been sent
        if ("maxPart" in req.body.messageProperties) {
            maxPart = Number(req.body.messageProperties.maxPart);
        }

        if ("compression" in req.body.messageProperties) {
            compression = req.body.messageProperties.compression.toLowerCase();
            if (compression != "none" && compression != "deflate") {
                context.log.error("compression message property is invalid, received: " + compression);
            }
        }

        return [deviceId, id, filepath, filename, part, maxPart, compression];
    }

    function cleanupDeadletter() {
        let files = fs.readdirSync(tempDir);
        let dt = new Date();
        dt.setHours(dt.getHours() - deadLetterExpireTimeInHours);
        files.forEach(file => {
            // a race condition can happen here where a file has been deleted after the list of files has been collected, handled in the exception catch
            try {
                const { birthtime } = fs.statSync(path.join(tempDir, file));  
                if (dt > birthtime) {
                    fs.renameSync(path.join(tempDir, file), path.join(deadLetterDir, file));
                }
            } catch(e) {
                // none essential exception this will be called again so just log it
                context.log.warn("Exception occured during dead-letter cleanup. Details: " + e)
            }
        });
    }

    async function aggregateChunks(context, deviceId, id, maxPart, compression, filepath, filename, confirmationFile) {
        context.log.info('all chunks in temp storage. Reassembling.');
        const fullUploadDir = path.join(uploadDir, filepath);
        if (!fs.existsSync(fullUploadDir)) {
            context.log.warn('filepath does not exist. creating...');
            fs.mkdirSync(fullUploadDir, { recursive: true });
        }
        let filenameToWrite = filename;
        if (fs.existsSync(path.join(fullUploadDir, filename))) {
            context.log.warn('file exists. creating revision');
            // create a revision number between filename and extension
            const ext = path.extname(filename);
            filenameToWrite = filename.split('.').slice(0, -1).join('.');
            const filesExistingCount = glob.sync(path.join(fullUploadDir, (filenameToWrite + ".**" + ext))).length;
            filenameToWrite = filenameToWrite + "." + (filesExistingCount + 1).toString() + ext;
        }

        context.log.info('creating and writing file stream');
        const stream = fs.createWriteStream(path.join(fullUploadDir, filenameToWrite), {flags:'w'});
        for (let i = 1; i < maxPart; i++) {
            const fileToRead = path.join(tempDir, (deviceId + "." + id + "." + i.toString()));
            context.log.info(`reading file: ${fileToRead}`);
            const chunk = fs.readFileSync(fileToRead).toString();
            const buff = Buffer.from(chunk, 'base64');
            let dataBuff = null;
            if (compression == "deflate") {
                dataBuff = zlib.inflateSync(buff);
            } else {
                dataBuff = buff;
            }
            // write out the file chunk 
            context.log.info("writing chunk out to file: " + filenameToWrite);
            stream.write(dataBuff);
        }
        stream.end();

        // clean up the message parts
        await cleanupTempFiles(context, deviceId, id, confirmationFile, maxPart);
    }

    async function cleanupTempFiles(context, deviceId, id, confirmationFile, maxPart) {
        for (let i = 1; i <= maxPart; i++) {
            let fileToDelete = path.join(tempDir, (deviceId + "." + id + "." + i.toString()));
            if(i === maxPart) {
                fileToDelete = confirmationFile;
            }
            try {
                fs.unlinkSync(fileToDelete);
            } catch(e) {
                // pause and try this again incase there was a delay in releasing the file lock
                try {
                    context.log.warn("Failure whilst cleaning up a temporary file retrying: " + path.join(tempDir, (deviceId + "." + id + "." + i.toString())));
                    await new Promise((resolve) => {
                        setTimeout(() => {
                            fs.unlinkSync(fileToDelete);
                            return resolve();
                        }, 1000);    
                    });
                } catch(e) {
                    // failed a second time so log the error, the file will be caught and dead lettered at a later time
                    context.log.error("Error whilst cleaning up a temporary file: " + path.join(tempDir, (deviceId + "." + id + "." + i.toString())));
                }
            }
        }
    }
}