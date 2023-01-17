from assets import assetsConnect
import os, time, assets, logging, logging.handlers
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

# Script settings 
maxThreads = 10

logFileKeep = 10 # Number of days to keep the logfiles, before being rotated
logFile = os.path.dirname(os.path.abspath(__file__))+"/backup.log"

# Debug level
fileFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s [%(lineno)d] %(message)s')
handler = logging.handlers.TimedRotatingFileHandler(logFile, when="midnight", backupCount=logFileKeep)
handler.setFormatter(fileFormatter)
fileLogger = logging.getLogger()
fileLogger.addHandler(handler)
fileLogger.setLevel(logging.INFO)

# define a Handler which writes INFO messages or higher to the sys.stderr
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s [%(lineno)d] %(message)s')
# tell the handler to use this format
consoleLogger.setFormatter(consoleFormatter)
# add the handler to the root logger
logging.getLogger().addHandler(consoleLogger)
logging.info("-----------Start of Run-----------")    

def getObjectData(object):
    objectData = myAssets.getObjectData(object)
    return object['id'], objectData

def getObjectHistory(object):
    objectHistory = myAssets.getObjectHistory(object['id'])
    return objectHistory

def getObjectComment(object):
    objectComment = myAssets.getObjectComment(object['id'])
    return objectComment

try:
    timeString = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())

    # Load config settings
    options = assets.getCommandlineOptions()

    # Connect to assets
    myAssets = assetsConnect(options.get('siteName'), options.get('username'), options.get('apiToken'))

    # get the object schema keys we want to backup
    objectSchemaKeys = options.get('objectSchemaKeys')
    objectSchemas =[]

    if not objectSchemaKeys:
        # If no object schema keys are specified, backup everything
        objectSchemas = myAssets.getObjectSchemas()
    else:
        for objectSchemaKey in objectSchemaKeys:
            objectSchema = myAssets.getObjectSchemaByKey(objectSchemaKey)
            if not objectSchema:
                logging.info(f"WARNING: Could not find object schema for key: '{objectSchemaKey}'")
                continue
            else:
                objectSchemas.append(objectSchema)
    if len(objectSchemas) == 0:
        logging.info(f"ERROR: No (valid) object schema's found to backup")
        exit(1)
    backupLocationPrefix = os.path.dirname(os.path.realpath(__file__))+"/"+timeString

    # Backup object schema's
    assets.saveAsJson(objectSchemas,"objectschemas", backupLocationPrefix+"/config")

    logging.info("Start backup of:")
    for objectSchema in objectSchemas: 
        logging.info(f"   {objectSchema['name']} [{objectSchema['objectSchemaKey']}]")
        backupLocation = backupLocationPrefix+"/"+objectSchema['objectSchemaKey']
        
        # Backup meta data
        # - object schema
        assets.saveAsJson(objectSchema,"objectschema", backupLocation+"/config")
        logging.info("   - objectschema")
        
        # - object schema properties
        objectSchemaProperties = myAssets.getObjectSchemaProperties(objectSchema['id'])
        assets.saveAsJson(objectSchemaProperties,"objectschema_properties", backupLocation+"/config")
        logging.info("   - object schema properties")
        
        # - global reference types
        referenceTypes = myAssets.getGlobalReferenceTypes()
        assets.saveAsJson(referenceTypes,"global_referencetypes", backupLocation+"/config")
        logging.info("   - global referencetypes")

        # - global status types
        statusTypes = myAssets.getGlobalStatusTypes()
        assets.saveAsJson(statusTypes,"global_statustypes", backupLocation+"/config")
        logging.info("   - global statustypes")

        # Backup schema
        # - schema reference types
        objectSchemaReferenceTypes = myAssets.getReferenceTypes(objectSchema['id'])
        assets.saveAsJson(objectSchemaReferenceTypes,"referencetypes", backupLocation+"/config")
        logging.info("   - schema referencetypes")

        # - schema status types
        objectSchemaStatusTypes = myAssets.getStatusTypes(objectSchema['id'])
        assets.saveAsJson(objectSchemaStatusTypes,"statustypes", backupLocation+"/config")
        logging.info("   - schema statustypes")

        # - object types
        allObjectTypes = myAssets.getObjectTypes(objectSchema['id'])
        assets.saveAsJson(allObjectTypes,"objecttypes", backupLocation+"/config")
        nrOfObjectTypes = len(allObjectTypes)
        logging.info(f"   - objecttypes [{nrOfObjectTypes}]")

        # - attributes
        i = 0
        for objectType in allObjectTypes:
            i += 1
            logging.info(f"- '{objectType['name']}' [{i}/{nrOfObjectTypes}]:")
            attributeList = myAssets.getAttributeList(objectType['id'])
            assets.saveAsJson(attributeList,f"{objectType['name']}_{objectType['id']}", backupLocation+"/config/attributes")
            logging.info(f"     - attributes [{len(attributeList)}]")
            
            # - objects
            objects = myAssets.getObjects("objectTypeId="+objectType['id'])
            assets.saveAsJson(objects,f"{objectType['name']}_{objectType['id']}", backupLocation+"/objectsmeta")
            logging.info(f"     - object types [{len(objects)}]")

            # - object data
            objectsData = {}
            # start the thread pool
            with ThreadPoolExecutor(maxThreads) as executor:
                # submit tasks and collect futures
                futures = [executor.submit(getObjectData,object) for object in objects]
                # process task results as they are available
                for future in as_completed(futures):
                    # retrieve the result
                    objectId, objectData = future.result()
                    objectsData[objectId] = objectData
            assets.saveAsJson(objectsData,f"{objectType['name']}_{objectType['id']}", backupLocation+"/objects")
            logging.info(f"        - data")
            
            # - object history
            with ThreadPoolExecutor(maxThreads) as executor:
                # submit tasks and collect futures
                futures = [executor.submit(getObjectHistory,object) for object in objects]
                # process task results as they are available
                for future in as_completed(futures):
                    # retrieve the result
                    objectHistory = future.result()
                    if objectHistory:
                        assets.saveAsJson(objectHistory,objectHistory[0]['objectId'], backupLocation+"/objects/history")
            logging.info(f"        - history")
                
            # - object history
            with ThreadPoolExecutor(maxThreads) as executor:
                # submit tasks and collect futures
                futures = [executor.submit(getObjectComment,object) for object in objects]
                # process task results as they are available
                for future in as_completed(futures):
                    # retrieve the result
                    objectComment = future.result()
                    if objectComment:
                        assets.saveAsJson(objectComment,objectComment[0]['objectId'], backupLocation+"/objects/comments")
            logging.info(f"        - comments")
    
    # Zip the backup
    assets.zipDir(backupLocationPrefix, f"assets-backup-{timeString}.zip")
except KeyboardInterrupt:
    # handle Ctrl-C
    logging.warn("Cancelled by user")
except Exception as ex:
    # handle unexpected script errors
    logging.exception("Unhandled error\n{}".format(ex))
    raise
finally:
    logging.info("------------End of Run------------")
    logging.shutdown()