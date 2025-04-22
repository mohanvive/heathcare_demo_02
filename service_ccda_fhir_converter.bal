import ballerina/ftp;
import ballerina/io;
import ballerina/log;
import ballerinax/health.fhir.r4;
import ballerinax/health.fhir.r4utils.ccdatofhir;

configurable string ftpHost = ?;
configurable int ftpPort = ?;
configurable string ftpUsername = ?;
configurable string ftpPassword = ?;

ftp:Client ftpClient = check new ({
    protocol: ftp:FTP,
    host: ftpHost,
    auth: {
        credentials: {
            username: ftpUsername,
            password: ftpPassword
        }
    },
    port: ftpPort
});

listener ftp:Listener fileListener = new ({
    protocol: ftp:FTP,
    host: ftpHost,
    auth: {
        credentials: {
            username: ftpUsername,
            password: ftpPassword
        }
    },
    port: ftpPort,
    fileNamePattern: "(.*).xml",
    path: "/home/in/",
    pollingInterval: 3
});

service on fileListener {
    remote function onFileChange(ftp:WatchEvent & readonly event, ftp:Caller caller) returns error? {
        do {
            foreach ftp:FileInfo addedFile in event.addedFiles {
                string fileName = addedFile.name;
                log:printInfo("File added: " + fileName);
                stream<byte[] & readonly, io:Error?> fileStream = check ftpClient->get(path = addedFile.pathDecoded);

                check fileStream.forEach(function(byte[] & readonly chunk) {
                    log:printInfo("-------- Started processing file content --------");
                    string fileContent = "";
                    string|error content = string:fromBytes(chunk);
                    if content is string {
                        fileContent += content;
                    } else {
                        log:printError("Error converting chunk to string", content);
                        return;
                    }

                    log:printInfo("-------- Finished consuming file content --------");
                    log:printInfo("File content: ", fileContent = fileContent);
                    if fileContent.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>") {
                        fileContent = fileContent.substring(38);
                    }
                    xml|error xmlContent = xml:fromString(fileContent);
                    if xmlContent is error {
                        log:printError("Invalid CCDA file recieved", xmlContent);
                        return;
                    }

                    r4:Bundle|r4:FHIRError ccdatofhirResult = ccdatofhir:ccdaToFhir(xmlContent);

                    if ccdatofhirResult is r4:FHIRError {
                        log:printError("Error converting CCDA to FHIR", ccdatofhirResult);
                        return;

                    } else {
                        io:println("Transformed FHIR message: ", (ccdatofhirResult).toString());
                        log:printInfo("-------- Finished processing file content --------");
                    }

                });

                ftp:Error? deletedFile = ftpClient->delete(path = addedFile.pathDecoded);
                if deletedFile is ftp:Error {
                    log:printError("Error deleting file", deletedFile);
                }
            }

        } on fail error err {
            return error("Error processing file", err);
        }
    }
}

