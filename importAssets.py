from turtle import position
from assets import assetsConnect
from concurrent.futures import ThreadPoolExecutor, as_completed
import os, logging, logging.handlers, re, assets
from os.path import isdir

# Script settings 
maxThreads = 8
logFileKeep = 10 # Number of days to keep the logfiles, before being rotated
logFile = os.path.dirname(os.path.abspath(__file__))+"/import.log"

# Debug level
fileFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s [%(lineno)d] %(message)s')
handler = logging.handlers.TimedRotatingFileHandler(logFile, when="midnight", backupCount=logFileKeep)
handler.setFormatter(fileFormatter)
fileLogger = logging.getLogger()
fileLogger.addHandler(handler)
fileLogger.setLevel(logging.DEBUG)

# define a Handler which writes INFO messages or higher to the sys.stderr
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s [%(lineno)d] %(message)s')
# tell the handler to use this format
consoleLogger.setFormatter(consoleFormatter)
# add the handler to the root logger
logging.getLogger().addHandler(consoleLogger)
logging.info("-----------Start of Run-----------")    

objectIdTranslate = {}

def getObjectSchemaIdTranslation(importObjectSchemaInfo, folder):
    objectSchemas = assets.loadJson(f"{folder}/config/objectschemas.json")
    
    translation = {}
    for objectSchema in objectSchemas:
        if objectSchema['objectSchemaKey']==importObjectSchemaInfo['oldObjectSchemaKey']:
            newObjectSchema = myAssets.getObjectSchemaByKey(importObjectSchemaInfo['newObjectSchemaKey'],True)
            if newObjectSchema:
                translation[objectSchema['id']]=newObjectSchema['id']
        else:
            newObjectSchema = myAssets.getObjectSchemaByKey(objectSchema['objectSchemaKey'],True)
            if newObjectSchema:
                translation[objectSchema['id']]=newObjectSchema['id']
    return translation

def updateObjectByObjectTypeId(updateObjectId, updateObjectTypeId, objectData):
    # Now we have to manipulate the objectData that was loaded from the backup
    # Because references to other objects might have changed
    # Luckily we have stored the old object keys (~id's)
    # And we have the objectTranslation
    
    # Find all reference attribute names
    attributesList = myAssets.getAttributeList(updateObjectTypeId)
    referenceAttributeNames = []
    for attribute in attributesList:
        if attribute['type'] == 1:
            referenceAttributeNames.append(attribute['name'])
    newObjectData = {}
    for key in objectData:
        if key in referenceAttributeNames:
            allrefValues = []
            if isinstance(objectData[key], list):
                # When we have a reference list
                for refValue in objectData[key]:
                    # Iterate through all referenced objects
                    translatedObjectId = objectIdTranslate.get(refValue['searchValue'].split("-",1)[1])
                    if translatedObjectId:
                        # only add id if we've found the translated object id
                        allrefValues.append(translatedObjectId)
                    else:
                        logging.info(f"WARNING: updateObjectByObjectTypeId > Could not find reference object '{refValue['displayValue']} [{refValue['searchValue']}]' for object id {updateObjectId}")
            else:
                # There is only one reference
                translatedObjectId = objectIdTranslate.get(objectData[key]['searchValue'].split("-",1)[1])
                if translatedObjectId:
                    # only add id if we've found the translated object id
                    allrefValues.append(translatedObjectId) 
                else:
                    logging.info(f"WARNING: updateObjectByObjectTypeId > Could not find reference object '{objectData[key]['displayValue']} [{objectData[key]['searchValue']}]' for object id {updateObjectId}")
            newObjectData[key] = allrefValues
        else:
            newObjectData[key] = objectData[key]

    updatedObject = myAssets.updateObjectByObjectTypeId(updateObjectId, updateObjectTypeId, newObjectData)
    if not updatedObject:
        logging.warning(f"Failed: updateObjectByObjectTypeId > Object id:{updateObjectId} - Object Type id: {updateObjectTypeId}")
    return updatedObject

def createObject(newObjectTypeId, object):
    newObject = None
    if object['id'] in objectIdTranslate:
        iql = f'objectId="{objectIdTranslate[object["id"]]}"'
        findObject = myAssets.getObjects(iql)
        if len(findObject)==1:
            # When object was found
            logging.info(f"Existing object: {object['name']} [{object['objectType']['name']}]")
            newObject=findObject[0]
    if not newObject:
        # Find the attribute which is used for the Label of the object
        labelAttribute = myAssets.getLabelAttribute(newObjectTypeId)
        logging.info(f"Creating object: {object['label']} [{object['objectType']['name']}]")

        data = {
            labelAttribute['name']: object['label']
        }
        newObject = myAssets.createObjectById(data, newObjectTypeId)
    return [object, newObject]

def createObjectAttribute(newObjectType, attribute, objectSchemaIdTranslate):
    # Check if attribute already exists
    newAttribute = myAssets.getAttributeByName(newObjectType['id'], attribute['name'])
    if not newAttribute:
        # Create new attribute
        data = {
            'objectTypeId': newObjectType['id'],
            'type': attribute['type'],
            'name': attribute['name']
        }
        if attribute.get('defaultType'):
            data['defaultTypeId'] = attribute['defaultType']['id']
        if attribute.get('description'):
            data['description'] = attribute['description']
        if attribute['type'] == 1: # Object reference type
            # Translate the reference object schema id, it might be another object schema
            referencedObjectSchemaId = objectSchemaIdTranslate.get(attribute['referenceObjectType']['objectSchemaId'])
            if not referencedObjectSchemaId:
                logging.warning ("WARNING: The referenced object schema id was not found, it might not exists yet.")
                logging.warning (f"   Skipping attribute {attribute['name']} for object {newObjectType['name']}")
                return [attribute, None]
            
            # Get the translation of the parent object id of the reference object type
            parentOTid = None
            if attribute['referenceObjectType'].get('parentObjectTypeId'):
                # Root objectypes have no parent object id
                parentOTid = objectTypeIdTranslate.get(attribute['referenceObjectType']['parentObjectTypeId'])

            # Find the reference object
            referenceObjectType = myAssets.getObjectTypeByName(attribute['referenceObjectType']['name'], referencedObjectSchemaId, parentOTid)
            if not referenceObjectType:
                logging.warning ("WARNING: The referenced object was not found, it might not exists yet.")
                logging.warning (f"   Skipping attribute {attribute['name']} for object {newObjectType['name']}")
                return [attribute, None]
                
            if referenceObjectType:
                data['typeValue'] = referenceObjectType['id']
            referenceType = myAssets.getReferenceTypeByName(attribute['referenceType']['name'])
            if referenceType:
                data['additionalValue'] = referenceType['id']

        logging.info(f"Create object type attribute '{attribute['name']}' [{attribute['id']}] for object {newObjectType['name']} [{newObjectType['id']}]")
        newAttribute = myAssets.createObjectTypeAttribute(newObjectType['id'], data)

    return [attribute, newAttribute]
def updateAttributeType(newObjectType, attribute, attributeIdTranslate):
    # add restrictions to attributes, like cardinality, regex
                    
    # Create attribute in objecttype 
    logging.info("updateAttributeType > Updating attribute: "+attribute['name'])
    newAttributeId = attributeIdTranslate.get(attribute['id'])
    data={}
    match attribute['type']:
        case 0: # Default
            logging.debug("updateAttributeType > Default")
            data['defaultTypeId'] = attribute['defaultType']['id']
            match attribute['defaultType']['id']:
                # See https://developer.atlassian.com/cloud/assets/rest/api-group-objecttypeattribute
                case -1: # None
                    logging.debug("updateAttributeType > defaultTypeId=-1")
                case  0: # Text
                    logging.debug("updateAttributeType > defaultTypeId=0")
                    if 'regexValidation' in attribute:
                        data['regexValidation'] = attribute['regexValidation']
                case  1: # Integer
                    logging.debug("updateAttributeType > defaultTypeId=1")
                    if 'suffix' in attribute:
                        data['suffix'] = attribute['suffix']
                    if 'summable' in attribute:
                        data['summable'] = attribute['summable']
                case  2: # Boolean
                    logging.debug("updateAttributeType > defaultTypeId=2")
                case  3: # Double
                    logging.debug("updateAttributeType > defaultTypeId=3")
                    if 'suffix' in attribute:
                        data['suffix'] = attribute['suffix']
                    if 'summable' in attribute:
                        data['summable'] = attribute['summable']
                case  4: # Date
                    logging.debug("updateAttributeType > defaultTypeId=4")
                case  5: # Time
                    logging.debug("updateAttributeType > defaultTypeId=5")
                case  6: # DateTime
                    logging.debug("updateAttributeType > defaultTypeId=6")
                case  7: # Url
                    logging.debug("updateAttributeType > defaultTypeId=7")
                    if 'additionalValue' in attribute:
                        data['additionalValue'] = attribute['additionalValue']
                case  8: # Email
                    logging.debug("updateAttributeType > defaultTypeId=8")
                    if 'minimumCardinality' in attribute:
                        data['minimumCardinality'] = attribute['minimumCardinality']
                    if 'maximumCardinality' in attribute:
                        data['maximumCardinality'] = attribute['maximumCardinality']
                    if 'regexValidation' in attribute:
                        data['regexValidation'] = attribute['regexValidation']
                case  9: # TextArea
                    logging.debug("updateAttributeType > defaultTypeId=9")
                case 10: # Select
                    logging.debug("updateAttributeType > defaultTypeId=10")
                    data['options'] = attribute['options']
                    if 'minimumCardinality' in attribute:
                        data['minimumCardinality'] = attribute['minimumCardinality']
                    if 'maximumCardinality' in attribute:
                        data['maximumCardinality'] = attribute['maximumCardinality']
                case 11: # IP Address
                    logging.debug("updateAttributeType > defaultTypeId=11")
                case _:
                    logging.error("updateAttributeType > Invalid defaultTypeId: "+str(attribute['defaultType']['id'])+" detected")
                    exit(-1)                                
        case 1: # Object Reference
            logging.debug("updateAttributeType > Object Reference")
            # data['typeValue'] = objectTypeIdTranslate[attribute['referenceObjectTypeId']]
            # data['additionalValue'] = myAssets.getReferenceTypeByName(attribute['referenceType']['name']))['id']
            if 'includeChildObjectTypes' in attribute:
                data['includeChildObjectTypes'] = attribute['includeChildObjectTypes']
            if 'iql' in attribute:
                data['iql'] = attribute['iql']
            if 'minimumCardinality' in attribute:
                data['minimumCardinality'] = attribute['minimumCardinality']
            if 'maximumCardinality' in attribute:
                data['maximumCardinality'] = attribute['maximumCardinality']
        case 2: # User
            logging.debug("updateAttributeType > User")
            if 'typeValueMulti' in attribute:
                valueMultiTranslated = []
                for value in attribute['typeValueMulti']:
                    valueMultiTranslated.append(value)
                data['typeValueMulti'] = valueMultiTranslated
            if 'additionalValue' in attribute:
                data['additionalValue'] = attribute['additionalValue']
            if 'minimumCardinality' in attribute:
                data['minimumCardinality'] = attribute['minimumCardinality']
            if 'maximumCardinality' in attribute:
                data['maximumCardinality'] = attribute['maximumCardinality']
        case 4: # Group
            logging.debug("updateAttributeType > Group")
            if 'minimumCardinality' in attribute:
                data['minimumCardinality'] = attribute['minimumCardinality']
            if 'maximumCardinality' in attribute:
                data['maximumCardinality'] = attribute['maximumCardinality']
        case 7: # Status
            logging.debug("updateAttributeType > Status")
            if 'typeValueMulti' in attribute:
                valueMultiTranslated = []
                for value in attribute['typeValueMulti']:
                    valueMultiTranslated.append(statusTypeIdTranslate[value])
                data['typeValueMulti'] = valueMultiTranslated
        case _:
            # See https://developer.atlassian.com/cloud/assets/rest/api-group-objecttypeattribute
            logging.error("updateAttributeType > Invalid attribute type: "+str(attribute['type'])+" detected")
            exit(-1)
    updatedObjectTypeAttribute = myAssets.updateObjectTypeAttribute(newObjectType['id'], newAttributeId, data)
    if not updatedObjectTypeAttribute:
        logging.warning("FAILED: updateAttributeType > attributeId: "+str(newAttributeId))
    else:
        logging.info("updateAttributeType > Updated attributeId: "+str(newAttributeId))
    
    return updatedObjectTypeAttribute

def addComment(filename, translate):
    if not filename.endswith('.json'):
        # This is not a JSON file, skip it
        return
    jsonfile = filename.replace(" ", '_')      
    comments  = assets.loadJson(jsonfile)
    objectId = translate.get(comments[0]['objectId']) # All comments in the list belong to the same object
    logging.info(f"   Comments for {comments[0]['objectId']}")
    # Iterate over comment list
    for comment in comments:
        # Decompose created > created date and created time        
        commentCreatedDate = re.search('(.*?)T.*', comment['created']).group(1)         # comment['created'] => 2022-03-01T09:46:32.409Z
        commentCreatedTime = re.search('.*?T(.*?)\..*', comment['created']).group(1)
        
        commentData = "<p><strong>Comment by: "+comment['actor']['displayName']+" on "+commentCreatedDate+" at "+commentCreatedTime+"</strong></p><p>"+comment['comment']+"</p>"

        # Create the comment
        myAssets.createComment(commentData, objectId)
    return

def addHistoryasComment(filename, translate):
    if not filename.endswith('.json'):
        # This is not a JSON file, skip it
        return
    jsonfile = filename.replace(" ", '_')      
    history = assets.loadJson(jsonfile)
    objectId = translate.get(history[0]['objectId']) # All history in the list belong to the same object

    # The history can not be recreated, we can only add the old history as a comment
    historyType = {
        0:'Created',
        1:'Added value',
        2:'Changed value',
        3:'Deleted value',
        4:'Added reference',
        5:'Changed reference',
        6:'Deleted reference',
        7:'Added attachment',
        8:'Deleted attachment',
        9:'Added User',
        10:'Changed user',
        11:'Deleted user',
        15:'Added group',
        16:'Changed group',
        17:'Deleted group',
        25:'Added avatar',
        26:'Changed avatar',
        27:'Deleted avatar'
    }

    # Create header for history
    comment = ("%-25s %-20s %-35s %-20s %-18s %s\n" % ("Created", "Type", "Actor", "Attribute", "Old value", "New value")) 
    comment+=("-" * 139)
    comment+="\n"

    # Iterate over history list
    for historyline in history:    
        objectToPrint =[]
        objectToPrint.append(historyline["created"])
        objectToPrint.append(historyType[historyline["type"]])   
        objectToPrint.append(historyline["actor"]["displayName"])
        if 'affectedAttribute' in historyline:
            objectToPrint.append(historyline["affectedAttribute"])
        else:
            objectToPrint.append("")
        if 'oldValue' in historyline:
            objectToPrint.append(historyline["oldValue"])
        else:
            objectToPrint.append("")
        if 'newValue' in historyline:
            objectToPrint.append(historyline["newValue"])
        else:
            objectToPrint.append("")
        comment+=("%-25s %-20s %-35s %-20s %-18s %s\n" % (objectToPrint[0], objectToPrint[1], objectToPrint[2], objectToPrint[3], objectToPrint[4], objectToPrint[5]))
    # Create the history comment
    logging.info(f"   History comment for {objectId}")
    myAssets.createComment(f"<pre>{comment}</pre>", objectId)
    return

def orderObjectTypes(objectTypes):
    # order object types so we can create them
    # It is not possible to create two object types with the same name on the same level

    level=0
    objectTypesPerLevel = {}
    # Find root object types = level 0
    objectTypesPerLevel[level] = list(filter(lambda objectType: 'parentObjectTypeId' not in objectType, objectTypes))
    # Create list without level 0
    remainingObjectTypes = list(filter(lambda objectType: 'parentObjectTypeId' in objectType, objectTypes))

    while remainingObjectTypes:
        idsInLevel = []
        for otpl in objectTypesPerLevel[level]:
            # Create a list of type object id's which are in the current level
            idsInLevel.append(otpl['id'])
        # Find objects which are in the next level
        objectTypesPerLevel[level+1] = list(filter(lambda objectType: objectType.get('parentObjectTypeId') in idsInLevel, remainingObjectTypes))
        # Create list without elements of the current level
        remainingObjectTypes  = list(filter(lambda objectType: objectType.get('parentObjectTypeId') not in idsInLevel, remainingObjectTypes))
        # Go to the next level
        level += 1

    orderedObjectTypes = []
    # Remove levels, but retain the object type order
    for otpl in objectTypesPerLevel:
        for ot in objectTypesPerLevel[otpl]:
            orderedObjectTypes.append(ot)
    return orderedObjectTypes

def getOldObjectTypeId(dict, newObjectTypeId):
    for name, age in dict.iteritems():
        if age == newObjectTypeId:
            return name
try:
    # Load config settings
    options = assets.getCommandlineOptions()
    
    processObjects = options.get('processObjects') if 'processObjects' in options else True
    processComments = options.get('processComments') if 'processComments' in options else True
    processHistory = options.get('processHistory') if 'processHistory' in options else True
    setAttributeRestrictions = options.get('setAttributeRestrictions') if 'setAttributeRestrictions' in options else True

    # Connect to assets
    myAssets = assetsConnect(options.get('siteName'), options.get('username'), options.get('apiToken'))

    # get the object schemas info we want to import
    objectSchemasInfoToImport = options.get('objectSchemas')

    for objectSchemaInfo in objectSchemasInfoToImport:
        folder = os.path.normpath(os.path.abspath(options.get('folder')))
        if not isdir(folder):
            logging.fatal(f"Path to data dir '{folder}' does not exists.")
            exit(1)
        
        if os.path.exists(folder+"/createdObjects.json"):
            # If a run was already done, reload already created objects
            objectIdTranslate = assets.loadJson(folder+"/createdObjects.json")

        importDataPath = f"{folder}/{objectSchemaInfo['oldObjectSchemaKey']}"

        # Import meta data
        # - create global reference types
        referenceTypes = assets.loadJson(importDataPath+'/config/global_referencetypes.json')
        for referenceType in referenceTypes:
            if not myAssets.getReferenceTypeByName(referenceType['name']):
                # The reference type does not exists  
                logging.info(f"Create global reference type '{referenceType['name']}'")
                myAssets.createReferenceType(referenceType['name'], referenceType['color'], referenceType['description'])

        # - create global status types
        statusTypeIdTranslate={}
        statusTypes = assets.loadJson(importDataPath+'/config/global_statustypes.json')
        for statusType in statusTypes:
            newStatusType = myAssets.getStatusTypeByName(statusType['name'])
            if not newStatusType:
                # The status type does not exists
                logging.info(f"Create global status type '{statusType['name']}'")
                newStatusType = myAssets.createStatusType(statusType['name'], statusType['category'], statusType['description'])
                if newStatusType:
                    statusTypeIdTranslate[statusType['id']]=newStatusType['id']
            else:
                logging.info(f"Found existing global status type '{statusType['name']}'")
                statusTypeIdTranslate[statusType['id']]=newStatusType['id']
            
        # Load object schema
        objectSchema = assets.loadJson(importDataPath+'/config/objectschema.json')

        # Create schema
        newObjectSchema = myAssets.getObjectSchemaByKey(objectSchemaInfo['newObjectSchemaKey'])
        if not newObjectSchema:
            logging.info(f"Create object schema '{objectSchemaInfo['newObjectSchemaName']}'")
            description = objectSchema['description'] if objectSchema.get('description') else ""
            newObjectSchema = myAssets.createObjectSchema(objectSchemaInfo['newObjectSchemaName'], objectSchemaInfo['newObjectSchemaKey'], description)

        # Load object schemas translation for referenced objects
        objectSchemaIdTranslate = getObjectSchemaIdTranslation(objectSchemaInfo, folder)

        # Load object schema properties
        objectSchemaProperties = assets.loadJson(importDataPath+'/config/objectschema_properties.json')
        if objectSchemaProperties:
            logging.info(f"Set object schema properties")
            myAssets.updateObjectSchemaProperties(newObjectSchema['id'], objectSchemaProperties['allowOtherObjectSchema'],objectSchemaProperties['createObjectsCustomField'],objectSchemaProperties['quickCreateObjects'],objectSchemaProperties['serviceDescCustomersEnabled'],objectSchemaProperties['validateQuickCreate'])

        # - create schema reference types
        referenceTypes = assets.loadJson(importDataPath+'/config/referencetypes.json')
        for referenceType in referenceTypes:
            referenceTypesOfObjectSchema = myAssets.getReferenceTypes(newObjectSchema['id'])
            createReference = True
            for referenceTypeOfObjectSchema in referenceTypesOfObjectSchema:    
                if referenceTypeOfObjectSchema == referenceType['name']:
                    createReference = False
                    continue
            if createReference:
                # No reference type exists for this object schema
                description = referenceType.get('description') if referenceType.get('description') else ""
                logging.info(f"Create reference type '{referenceType['name']}'")
                myAssets.createReferenceType(referenceType['name'], referenceType['color'], description, newObjectSchema['id'])
        # Reload all known reference types
        referenceTypes = myAssets.getAllReferenceTypes(True)        
        
        # - create schema status types
        statusTypes = assets.loadJson(importDataPath+'/config/statustypes.json')
        for statusType in statusTypes:
            newStatusType = myAssets.getStatusTypeByName(statusType['name'])
            if not newStatusType:
                # The status type does not exists  
                logging.info(f"Create status type '{statusType['name']}'")
                newStatusType = myAssets.createStatusType(statusType['name'], statusType['category'], statusType['description'], newObjectSchema['id'])
            statusTypeIdTranslate[statusType['id']]=newStatusType['id']
        # Reload    
        # - create schema object types
        # Load  the list and order them by level
        objectTypes = orderObjectTypes(assets.loadJson(importDataPath+'/config/objecttypes.json'))
        objectTypeIdTranslate={}
        newObjectTypes={}

        for objectType in objectTypes:
            # Check if object type exists
            parentOTid = None
            if objectType.get('parentObjectTypeId'):
                # Root objectypes have no parent object id
                parentOTid = objectTypeIdTranslate.get(objectType.get('parentObjectTypeId'))
            newObjectType = myAssets.getObjectTypeByName(objectType['name'], newObjectSchema['id'], parentOTid, True)
            if not newObjectType:
                data = {
                    'objectSchemaId': newObjectSchema['id'],
                    'name': objectType['name'],
                    'iconId': objectType['icon']['id']            
                }
                if objectType.get('description'):
                    data['description'] = objectType['description']
                if objectType.get('inherited'):
                    data['inherited'] = objectType['inherited']
                if objectType.get('abstractObjectType'):
                    data['abstractObjectType'] = objectType['abstractObjectType']
                if objectType.get('parentObjectTypeId'):
                    if not objectType['parentObjectTypeId']=='0':
                        data['parentObjectTypeId'] = objectTypeIdTranslate[objectType['parentObjectTypeId']]
                
                # Create the object type
                logging.info(f"Create object type '{objectType['name']}'")
                newObjectType = myAssets.createObjectType(data)
                
            # Add objecttype id to translation dict
            objectTypeIdTranslate[objectType['id']]=newObjectType['id']
            newObjectTypes[newObjectType['id']]=newObjectType
            
        for objectType in objectTypes:
            # Reposition of object type
            if objectType.get('parentObjectTypeId'):
                myAssets.changeObjectTypePosition(objectTypeIdTranslate.get((objectType['id'])), objectTypeIdTranslate.get((objectType['parentObjectTypeId'])), objectType['position'])

        newObjectTypes = myAssets.getObjectTypes(newObjectSchema['id'], False, True)
        newObjectTypes.sort(key=lambda x: x['id']) # Sort the object types list
        attributeIdTranslate={}
        
        for newObjectType in newObjectTypes:
            # Create attributes for object type
            oldOjbectTypeId = ''
            for oldId, newId in objectTypeIdTranslate.items():
                if newId == newObjectType['id']:
                    oldOjbectTypeId = oldId
                    break
            fn = f"{newObjectType['name']}_{oldOjbectTypeId}"
            fn = fn.replace("/","_") # if a slash '/' is in the name turn it into a underscore '_'
            fn = fn.replace("\\","_") # if a backslash '\' is in the name turn it into a underscore '_'
            jsonfile = f"config/attributes/{fn}.json"
            jsonfile = jsonfile.replace(" ", '_')      
            jsonfile = f"{importDataPath}/{jsonfile}"

            if not os.path.exists(jsonfile):
                # Catch upstream logic bug where author makes assumptions of what was exported. TODO: For RCI when we have time.
                logging.warning(f"BUG Missing attribute type in backup: name={newObjectType['name']}, "
                                f"id={oldOjbectTypeId} (expected file to exist: {jsonfile}). Skipping")
                continue

            attributes = assets.loadJson(jsonfile)
            newAttributes=[]

            # Not threaded for debugging
            # for attribute in attributes:
            #     attribute, newAttribute = createObjectAttribute(newObjectType, attribute, objectSchemaIdTranslate)
                
            #     if newAttribute:
            #         attributeIdTranslate[attribute['id']]=newAttribute['id']
            #         newAttributes.append(newAttribute)

            # start the thread pool
            with ThreadPoolExecutor(maxThreads) as executor:
                # submit tasks and collect futures
                futures = [executor.submit(createObjectAttribute, newObjectType, attribute, objectSchemaIdTranslate) for attribute in attributes]
                # process task results as they are available
                for future in as_completed(futures):
                    # retrieve the result
                    if future:
                        attribute, newAttribute = future.result()
                        if newAttribute:
                            attributeIdTranslate[attribute['id']]=newAttribute['id']
                            newAttributes.append(newAttribute)
                          
            # set the correct position of the attribute
            newAttributesList = sorted(newAttributes, key=lambda d: (d['position'] * -1)) 
            for attribute in newAttributesList:
                logging.info(f"  Attribute {attribute['name']} set to position {attribute['position']} for {newObjectType['name']}")    
                myAssets.moveObjectTypeAttribute(newObjectType['id'], attribute['id'], attribute['position'])
            logging.info(f"Attributes ordered for {newObjectType['name']}")

        # - create objects without attributes (only labels)
        # This is done to be able to refer to objects in attributes.
        newObjects = {}
        if processObjects:
            for filename in os.listdir(f'{importDataPath}/objectsmeta'):
                if filename.endswith('.json'):
                    jsonfile = f'{importDataPath}/objectsmeta/{filename}'
                    jsonfile = jsonfile.replace(" ", '_')      
                    objects = assets.loadJson(jsonfile)

                    # start the thread pool
                    with ThreadPoolExecutor(maxThreads) as executor:
                        # submit tasks and collect futures
                        futures = [executor.submit(createObject, objectTypeIdTranslate.get(object['objectType']['id']), object) for object in objects]
                        # process task results as they are available
                        for future in as_completed(futures):
                            # retrieve the result
                            # if not future:
                            #     a = future.result()
                            oldObject, newObject = future.result()
                            if newObject:
                                if 'errorMessages' in newObject:
                                    logging.warning(f"Object '{oldObject.get('label')}'of type '{oldObject['objectType']['name']}' could not be created")
                                    logging.warning(f"")
                                    continue
                                objectIdTranslate[oldObject['id']]=newObject['id']
                                newObjects[newObject['id']]=newObject

            # - Update the object with attribute values            
            for filename in os.listdir(f'{importDataPath}/objects'):
                if filename.endswith('.json'):
                    jsonfile = f'{importDataPath}/objects/{filename}'
                    jsonfile = jsonfile.replace(" ", '_')      
                    objects = assets.loadJson(jsonfile)

                    # start the thread pool
                    with ThreadPoolExecutor(maxThreads) as executor:
                        # Verify original author's precondition assumptions, and allow other restore steps to
                        # continue if those are invalid, so as to not get a complete recovery failure.
                        futures = []
                        for objectId, obj in objects.items():
                            if objectId not in objectIdTranslate:
                                logging.warning(f'BUG Missing objectId={objectId} in objectIdTranslate')
                                continue
                            translatedId = objectIdTranslate[objectId]

                            if translatedId not in newObjects:
                                logging.warning(f'BUG Missing translated objectId={translatedId} in newObjects')
                                continue

                            newObject = newObjects[translatedId]

                            if 'objectType' not in newObject:
                                logging.warning(f'BUG Missing objectType attrbute in newObject with translatedId={translatedId}. object missing the attribute: {newObject}')
                                continue

                            if 'id' not in newObject['objectType']:
                                logging.warning(f'BUG Missing objectType->id attrbute in newObject with translatedId={translatedId}. object missing the attribute: {newObject}')
                                continue

                            newObjectId = newObject['objectType']['id']
                            futures.append(executor.submit(updateObjectByObjectTypeId, translatedId, newObjectId, obj))
                        # END author assumption validation

                        # Update the object
                        # submit tasks and collect futures
                        #futures = [executor.submit(updateObjectByObjectTypeId, objectIdTranslate[objectId], newObjects[objectIdTranslate[objectId]]['objectType']['id'], objects[objectId]) for objectId in objects]

                        # process task results as they are available
                        for future in as_completed(futures):
                            
                            # retrieve the result
                            newObject = future.result()
                            if newObject:
                                if newObject.get('id'):
                                    newObjects[newObject['id']]=newObject
                                    logging.info(f"Updated: {newObject['name']}")

            # - add comments to objects
            if processComments:
                logging.info("Start restoring comments")
                if isdir(f'{importDataPath}/objects/comments'):
                    with ThreadPoolExecutor(maxThreads) as executor:
                        futures = [executor.submit(addComment, f'{importDataPath}/objects/comments/{filename}', objectIdTranslate) for filename in os.listdir(f'{importDataPath}/objects/comments')]
                        # process task results as they are available
                        for future in as_completed(futures):
                            commentResponse = future.result()
                    logging.info(f"Comments created")

            # - add history to objects
            if processHistory:
                logging.info("Start restoring history")
                if isdir(f'{importDataPath}/objects/history'):
                    with ThreadPoolExecutor(maxThreads) as executor:
                        futures = [executor.submit(addHistoryasComment, f'{importDataPath}/objects/history/{filename}', objectIdTranslate) for filename in os.listdir(f'{importDataPath}/objects/history')]
                        # process task results as they are available
                        for future in as_completed(futures):
                            historyResponse = future.result()
                    logging.info(f"History comments created")
                
            # - add restrictions to attributes
            if setAttributeRestrictions:
                for newObjectType in newObjectTypes:
                    oldOjbectTypeId = ''
                    for oldId, newId in objectTypeIdTranslate.items():
                        if newId == newObjectType['id']:
                            oldOjbectTypeId = oldId
                            break
                    # Create attributes for object type
                    fn = f"{newObjectType['name']}_{oldOjbectTypeId}"
                    fn = fn.replace("/","_") # if a slash '/' is in the name turn it into a underscore '_'
                    fn = fn.replace("\\","_") # if a backslash '\' is in the name turn it into a underscore '_'
                    jsonfile = f"{importDataPath}/config/attributes/{fn}.json"
                    jsonfile = jsonfile.replace(" ", '_')      
                
                    if not os.path.exists(jsonfile):
                        # Catch upstream logic bug where author makes assumptions of what was exported. TODO: For RCI when we have time.
                        logging.warning(f"BUG Missing attribute restriction in backup: name={newObjectType['name']}, "
                                        f"id={oldOjbectTypeId} (expected file to exist: {jsonfile}). Skipping")
                        continue

                    attributes = assets.loadJson(jsonfile)
                    with ThreadPoolExecutor(maxThreads) as executor:
                        futures = [executor.submit(updateAttributeType, newObjectType, attribute, attributeIdTranslate) for attribute in attributes]
                        # process task results as they are available
                        for future in as_completed(futures):
                            updatedAttribute = future.result()
                            logging.info(f"Attribute {updatedAttribute.get('name')} updated")

except KeyboardInterrupt:
    # handle Ctrl-C
    logging.warn("Cancelled by user")
except Exception as ex:
    # handle unexpected script errors
    logging.exception("Unhandled error\n{}".format(ex))
    raise
finally:
    assets.saveAsJson(objectIdTranslate, "createdObjects",folder)
    logging.info("------------End of Run------------")
    logging.shutdown()