// Azure function to handle file uploads through IoT Central's data export feature

module.exports = async function (context, req) {

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

        let deviceId = req.body.deviceId;

        // pull the message part meta data from the message properties
        let id = "";
        let filepath = "";
        let filename = "";
        let part = 0;
        let maxPart = 0;
        let compression = "none";

        // check to make sure all the needed message properties have been sent
        if ("id" in req.body.messageProperties) {
            id = req.body.messageProperties.id;
        } else {
            throw "Missing message property: id";
        }

        if ("id" in req.body.messageProperties) {
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

        if ("maxPart" in req.body.messageProperties) {
            maxPart = Number(req.body.messageProperties.maxPart);
        } else {
            throw "Missing message property: maxPart";
        }

        if ("compression" in req.body.messageProperties) {
            compression = req.body.messageProperties.compression.toLowerCase();
            if (compression != "none" && compression != "deflate") {
                context.log("compression message property is invalid, received: " + compression);
            }
        } else {
            throw "Missing message property: compression";
        }

        // log new file part
        context.log.info("device-id: " + deviceId + " file-id: " + id + " part: " + part + " of: " + maxPart.toString() + " filepath: " + filepath + " filename: " + filename);

        // write out the file part
        fs.writeFileSync(path.join(tempDir, (deviceId + "." + id + "." + maxPart + "." + part)), req.body.telemetry.contentChunk);

        // check to see if all the file parts are available
        let filePartCount = glob.sync(path.join(tempDir, (deviceId + "." + id + ".*"))).length;
        if (filePartCount == maxPart) {
            // all expected file parts are available - time to rehydrate the file
            let encodedData = "";
            for (let i = 1; i <= maxPart; i++) {
                let chunk = fs.readFileSync(path.join(tempDir, (deviceId + "." + id + "." + maxPart + "." + i.toString())));
                encodedData = encodedData + chunk;
            }
            let buff = Buffer.from(encodedData, 'base64');
            let dataBuff = null;
            if (compression == "deflate") {
                dataBuff = zlib.inflateSync(buff);
            } else {
                dataBuff = buff;
            }

            // write out the rehydrated file 
            const fullUploadDir = path.join(uploadDir, filepath);
            if (!fs.existsSync(fullUploadDir)) {
                fs.mkdirSync(fullUploadDir, { recursive: true });
            }
            if (fs.existsSync(path.join(fullUploadDir, filename))) {
                // create a revision number between filename and extension
                ext = path.extname(filename);
                filename = filename.split('.').slice(0, -1).join('.');
                let filesExistingCount = glob.sync(path.join(fullUploadDir, (filename + ".**" + ext))).length;
                filename = filename + "." + (filesExistingCount + 1).toString() + ext;
            }
            context.log.info("writing out the file: " + filename);
            fs.writeFileSync(path.join(fullUploadDir, filename), dataBuff);

            // clean up the message parts
            for (let i = 1; i <= maxPart; i++) {
                try {
                    fs.unlinkSync(path.join(tempDir, (deviceId + "." + id + "." + maxPart + "." + i.toString())));
                } catch(e) {
                    // pause and try this again incase there was a delay in releasing the file lock
                    try {
                        context.log.warn("Failure whilst cleaning up a temporary file retrying: " + path.join(tempDir, (deviceId + "." + id + "." + maxPart + "." + i.toString())));
                        setTimeout(() => {
                            fs.unlinkSync(path.join(tempDir, (deviceId + "." + id + "." + maxPart + "." + i.toString())));
                        }, 1000);
                    } catch(e) {
                        // failed a second time so log the error, the file will be caught and dead lettered at a later time
                        context.log.error("Error whilst cleaning up a temporary file: " + path.join(tempDir, (deviceId + "." + id + "." + maxPart + "." + i.toString())));
                    }
                }
            }
        }
        
        // check for expired files in temp directory and dead letter them
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
}