from os.path import exists,abspath
from dateutil.tz import tzlocal
import re, io, os, json, base64, logging, logging.handlers, urllib.parse, zipfile, optparse
import requests         # python -m pip install requests
import dateutil.parser as parser

class insightConnect():
    def __init__(self, jiraUrl, username, apiToken):
        if not jiraUrl:
            logging.fatal(f"Jira URL was not provided.")
            exit(1)
        if not username:
            logging.fatal(f"Username not provided.")
            exit(1)
        if not( apiToken):
            logging.fatal(f"API Token was not provided.")
            exit(1)
        self.jiraUrl = jiraUrl.rstrip("/")
        self.username = username
        self.apiKey   = base64.b64encode(bytes(username+":"+apiToken, 'utf-8')).decode('ascii')
        self.headers  = {"Authorization": "Basic "+self.apiKey, "Content-Type": "application/json"}
        
        # Get workspaceId
        workspaceId = self.getWorkspaceId()
        if workspaceId:
            logging.debug("Get workspaceId: "+str(workspaceId))
        else:
            logging.fatal("Workspace id could not be found, can't continue.")
            exit(1)
        self.workspaceId = workspaceId
        self.insightUrl = 'https://api.atlassian.com/jsm/insight/workspace/'+self.workspaceId
        
        self.objectSchemas = {}
        self.statusTypes = {}
        self.globalStatusTypes = {}
        self.referenceTypes = {}
        self.globalReferenceTypes = {}
        self.objectTypes = {}
        self.objectTypeAttributes = {}
        self.jiraUserAccounts = {}
        self.jiraGroups = {}
        
    def getWorkspaceId(self):
        logging.debug("getWorkspaceId")
        query = self.jiraUrl+'/rest/servicedeskapi/insight/workspace'
        result = self.insightGet(query)
        return result['values'][0]['workspaceId'] if result else None
        
    def insightGet(self, query):
        logging.debug("insightGet")
        try:
            result = requests.get(query, headers=self.headers)
            return result.json()
        except Exception as e:
            logging.exception(e)
            return None

    def insightDelete(self, query, params=None):
        logging.debug("insightDelete")
        logging.debug(f"  {query}")
        try:
            result = None
            if params:
                result = requests.delete(query, params=params, headers=self.headers)
            else:
                result = requests.delete(query, headers=self.headers)
            return result.json()
        except Exception as e:
            logging.exception(e)
            return None

    def insightPut(self, query, data=None):
        logging.debug("insightPut")
        try:
            if data:
                logging.debug(f"  {query}")
                logging.debug(f"  {json.dumps(data)}")
                result = requests.put(query, json=data, headers=self.headers)
            else:
                logging.debug(f"  {query}")
                result = requests.put(query, headers=self.headers)
            return result.json()
        except Exception as e:
            logging.exception(e)
            return None

    def insightPost(self, query, data):
        logging.debug("insightPost")
        logging.debug(f"  {query}")
        logging.debug(f"  {json.dumps(data)}")
        try:
            result =  requests.post(query, json=data, headers=self.headers)
            if result.text != '':
                return result.json()
            else: 
                return None
        except Exception as e:
            logging.exception(e)
            return None

    def getAllStatusTypes(self, reload=False):
        # Get global status types
        allStatusTypes = self.getGlobalStatusTypes(reload=reload)
        
        # Get object schemas
        objectSchemas = self.getObjectSchemas()
        for objectSchema in objectSchemas:
            # Get status type for this object schema
            statusTypes = self.getStatusTypes(objectSchema['id'],reload=reload)
            # Extend the list 
            allStatusTypes.extend(statusTypes)
        
        self.statusTypes = allStatusTypes
        return allStatusTypes
    
    def getGlobalStatusTypes(self, reload=False):
        logging.debug("getGlobalStatusTypes")
        if self.globalStatusTypes and not reload:
            # Return statusTypes when we already got them once
            logging.debug("getGlobalStatusTypes > return cached status types")
            return self.globalStatusTypes
        
        statusTypes = []
        # Get global status types
        statusTypes = self.insightGet(self.insightUrl+'/v1/config/statustype')
        self.globalStatusTypes = statusTypes
        return statusTypes
    
    def getStatusTypeByName(self, name, reload=False):
        logging.debug("getStatusTypeByName name:"+str(name)+", reload:"+(str(reload)))
        if not self.statusTypes:
            self.getAllStatusTypes(reload)
        for statusType in self.statusTypes:
            if statusType.get('name') == name:
                return statusType
        # No status type with that name was found
        return None

    def getStatusTypes(self, objectSchemaId, reload=False):
        logging.debug("getStatusType")
        if self.statusTypes.get(objectSchemaId) and not reload:
            # Return statusTypes when we already got them once
            logging.debug("getStatusType > return cached status types")
            return self.statusTypes.get(objectSchemaId)

        # Get status type of object schema
        statusTypes = []
        statusTypes = self.insightGet(self.insightUrl+'/v1/config/statustype?objectSchemaId='+objectSchemaId)
        self.statusTypes[objectSchemaId] = statusTypes  
        return statusTypes
    
    def getStatusType(self, id):
        logging.debug("getStatusType id:"+str(id))
        query = self.insightUrl+'/v1/config/statustype/'+str(id)
        result = self.insightGet(query)
        return result if result else None

    def createStatusType(self, name, category, desc=None, objectSchemaId=None):
        logging.debug("createStatusType name:"+str(name)+", category:"+(str(category))+", desc:"+(str(desc))+", objectSchemaId:"+(str(objectSchemaId)))
        #categoryList = {'INACTIVE':0,'ACTIVE':1,'PENDING':2}
        #if categoryList.get(category.upper()):
        #    category = categoryList.get(category.upper())
        
        query = self.insightUrl+'/v1/config/statustype'
        data = {
            "name": name,
            "category": category
        }
        if objectSchemaId:
            data['description']=desc
        if objectSchemaId:
            data['objectSchemaId']=objectSchemaId
        result = self.insightPost(query, data)
        return result if result else None
    
    def updateStatusType(self, id, category, name=None, desc=None, objectSchemaId=None):
        logging.debug("updateStatusType id:"+str(id)+", name:"+(str(name))+", category:"+(str(category))+", desc:"+(str(desc))+", objectSchemaId:"+(str(objectSchemaId)))
        categoryList = {'INACTIVE':0,'ACTIVE':1,'PENDING':2}
        if categoryList.get(category.upper()):
            category = categoryList.get(category.upper())
        
        query = self.insightUrl+'/v1/config/statustype/'+str(id)
        data = {
            "category": category # Mandatory
        }
        if objectSchemaId:
            data['name']=name # Optional
        if objectSchemaId:
            data['desc']=desc # Optional
        if objectSchemaId:
            data['objectSchemaId']=objectSchemaId # Optional
        result = self.insightPut(query, data)
        return result["id"] if "id" in result else None

    def deleteStatusType(self, id):
        logging.debug("updateStatusType id"+str(id))
        query = self.insightUrl+'/v1/config/statustype/'+str(id)
        return self.insightDelete(query)
    
    def getAllReferenceTypes(self, reload=False):
        # Get global reference types
        allReferenceTypes = self.getGlobalReferenceTypes(reload=reload)
        
        # Get object schemas
        objectSchemas = self.getObjectSchemas()
        for objectSchema in objectSchemas:
            # Get reference type for this object schema
            referenceTypes = self.getReferenceTypes(objectSchema['id'],reload=reload)
            # Extend the list 
            allReferenceTypes.extend(referenceTypes)
        
        self.referenceTypes = allReferenceTypes
        return allReferenceTypes
    
    def getGlobalReferenceTypes(self, reload=False):
        logging.debug("getGlobalReferenceTypes")
        if self.globalReferenceTypes and not reload:
            # Return reference types when we already got them once
            logging.debug("getGlobalReferenceTypes > return cached reference types")
            return self.globalReferenceTypes
        
        referenceTypes = []
        # Get global reference types
        referenceTypes = self.insightGet(self.insightUrl+'/v1/config/referencetype')
        self.globalReferenceTypes = referenceTypes
        return referenceTypes
    
    def getReferenceTypeByName(self, name, reload=False):
        logging.debug(f'getReferenceTypeByName name: {name}, reload: {reload}')
        if not self.referenceTypes:
            self.getAllReferenceTypes(reload)
        for referenceType in self.referenceTypes:
            if referenceType.get('name') == name:
                return referenceType
        # No reference type with that name was found
        return None

    def getReferenceTypes(self, objectSchemaId, reload=False):
        logging.debug("getReferenceType")
        # Get reference type of object schema
        referenceTypes = []
        referenceTypes = self.insightGet(self.insightUrl+'/v1/config/referencetype?objectSchemaId='+objectSchemaId)
        return referenceTypes
    
    def createReferenceType(self, name, color, desc="", objectSchemaId=None):
        logging.debug(f'createReferenceType name: {name}, description: {desc}, color: {color}, objectSchemaId: {objectSchemaId}')
        data = {
            "name": name,
            "description": desc,
            "color": color
        }
        if objectSchemaId:
            data['objectSchemaId']=objectSchemaId # Optional

        query = f'{self.insightUrl}/v1/config/referencetype'
        result = self.insightPost(query, data)
        return result if result else None

    def deleteReferenceType(self,referenceTypeId):
        logging.debug(f'deleteReferenceType id: {referenceTypeId}')
        query = f'{self.insightUrl}/v1/config/referencetype/{referenceTypeId}'
        return self.insightDelete(query)
    
    def updateReferenceType(self, id, name=None, color=None, desc=None, objectSchemaId=None):
        logging.debug(f'updateReferenceType id:{id}, name:{name}, color:{color}, desc:{desc}, objectSchemaId:{objectSchemaId}')
        
        if not name and not color and not desc:
            # nothing to change
            logging.warning(f'updateReferenceType: name, color and description are empty, can not update!')
            return None 
        
        query = f'{self.insightUrl}/v1/config/referencetype/{id}'
        data = {}
        if name:
            data['name']=name # Optional
        if name:
            data['color']=color # Optional
        if name:
            data['desc']=desc # Optional
        if objectSchemaId:
            data['objectSchemaId']=objectSchemaId # Optional
        result = self.insightPut(query, data)
        return result["id"] if "id" in result else None

    def getAttributeList(self, objectTypeId):
        logging.debug(f"getAttributeList objectTypeId: {objectTypeId}")
        query = f"{self.insightUrl}/v1/objecttype/{objectTypeId}/attributes"
        result = self.insightGet(query)
        return result if result else None
        
    def getLabelAttribute(self, objectTypeId):
        logging.debug(f"getLabelAttribute objectTypeId: {objectTypeId}")
        attributes = self.getAttributeList(objectTypeId)
        for attribute in attributes:
            if attribute.get('label'):
                return attribute
        return None
        
    def getObjects(self, iql, includeExtendedInfo=False, includeAttributes=True, includeAttributesDeep=1):
        logging.debug("getObjects iql:"+urllib.parse.quote_plus(iql)+", includeExtendedInfo:"+(str(includeExtendedInfo))+", includeAttributes:"+(str(includeAttributes))+", includeAttributesDeep:"+(str(includeAttributesDeep)))
        
        query = self.insightUrl+'/v1/iql/objects?includeExtendedInfo='+str(includeExtendedInfo)+'&includeAttributes='+str(includeAttributes)+'&includeAttributesDeep='+str(includeAttributesDeep)+'&iql='+urllib.parse.quote_plus(iql)
        # Get first page of iql query
        responseIql = self.insightGet(query)
        if not responseIql:
            # Something went wrong and we did not get back a proper response
            return None
            
        objects = []
        if (responseIql.get('iqlSearchResult')):
            # Iql query returned a result
            objects = responseIql['objectEntries']
            nextPage = responseIql['pageNumber']+1
            totalPages = responseIql['pageSize']
            
            # Get all other pages of iql query
            while nextPage <= totalPages:
                nextResponseIql = self.insightGet(query+'&page='+str(nextPage))
                objects.extend(nextResponseIql['objectEntries'])
                nextPage = nextResponseIql['pageNumber']+1
    
        return objects
    
    def getObjectsViaNavlist(self, data, includeAttributes=True):
        logging.debug("getObjectsViaNavlist :includeAttributes:"+(str(includeAttributes)))
        
        query = self.insightUrl+'/v1/object/navlist/iql'
        data['includeAttributes'] = includeAttributes
        # Get first page of iql query
        responseIql = self.insightPost(query, data)
        if not responseIql:
            # Something went wrong and we did not get back a proper response
            return None

        objects = []
        if (responseIql.get('iqlSearchResult')):
            # Iql query returned a result
            objects = responseIql['objectEntries']
            nextPage = responseIql['pageNumber']+1
            totalPages = responseIql['pageSize']
            
            # Get all other pages of iql query
            while nextPage <= totalPages:
                nextResponseIql = self.insightGet(query+'&page='+str(nextPage))
                objects.extend(nextResponseIql['objectEntries'])
                nextPage = nextResponseIql['pageNumber']+1
    
        return objects
        

    def getObject(self, id):
        logging.debug("getObject id:"+str(id))
        query = self.insightUrl+'/v1/object/'+str(id)
        result = self.insightGet(query)
        return result if result else None

    def deleteObject(self, id):
        logging.debug("deleteObject id:"+str(id))
        query = self.insightUrl+'/v1/object/'+str(id)
        result = self.insightDelete(query)
        return result if result else None

    def getObjectAttributes(self, id):
        logging.debug("getObjectAttributes id:"+str(id))
        query = self.insightUrl+'/v1/object/'+str(id)+'/attributes'
        result = self.insightGet(query)
        return result if result else None

    def getObjectHistory(self, id):
        logging.debug("getObjectHistory id:"+str(id))
        query = self.insightUrl+'/v1/object/'+str(id)+'/history'
        result = self.insightGet(query)
        return result if result else None

    def getObjectComment(self, id):
        logging.debug("getObjectComment id:"+str(id))
        query = self.insightUrl+'/v1/comment/object/'+str(id)
        result = self.insightGet(query)
        return result if result else None

    def getObjectReferenceInfo(self, id):
        logging.debug("getObjectReferenceInfo id:"+str(id))
        query = self.insightUrl+'/v1/object/'+str(id)+'/referenceinfo'
        result = self.insightGet(query)
        return result if result else None

    def updateObject(self, objectId, data):
        logging.debug("updateObject id:"+str(objectId)+", data:"+(str(data)))
        query = self.insightUrl+'/v1/object/'+str(objectId)
        result = self.insightPut(query, data)
        return result if result else None

    def updateObjectByLabel(self, objectLabelToUpdate, myDict, objectTypeName, objectSchemaName):
        logging.debug("updateObjectByLabel objectLabelToUpdate:"+str(objectLabelToUpdate)+", myDict:"+(str(myDict))+", objectType:"+(str(objectTypeName))+", objectSchema:"+(str(objectSchemaName)))
        iql = 'objectSchema="'+objectSchemaName+'" and objectType="'+objectTypeName+ '" and label="'+objectLabelToUpdate+'"'
        objectToUpdate = self.getObjects(iql)
        if not objectToUpdate:
            logging.warning("Unknown object with label: "+objectLabelToUpdate)
            return None
        objectSchema = self.getObjectSchemaByName(objectSchemaName)
        if not objectSchema:
            logging.warning("Unknown objectschame name: "+objectSchemaName)
            return None
        objectType = self.getObjectTypeByName(objectTypeName,objectSchema.get('id'))
        if not objectType:
            logging.warning("Unknown objecttype name: "+objectTypeName)
            return None
        objectTypeId = objectType.get('id')
        payload = self.constructObjectPayload(myDict,objectTypeId, objectToUpdate)
        return self.updateObject(objectToUpdate[0]['id'],json.loads(payload))

    def updateObjectByObjectTypeId(self, objectId, objectTypeId, myDict):
        logging.debug("updateObjectById objectId: "+str(objectId)+", objectTypeId:"+str(objectTypeId)+", myDict:"+(str(myDict)))
        payload = self.constructObjectPayload(myDict,objectTypeId)
        return self.updateObject(objectId,json.loads(payload))

    def createObject(self, data):
        logging.debug("createObject data:"+str(data))
        query = self.insightUrl+'/v1/object/create'
        result = self.insightPost(query, data)
        return result if result else None

    def createObjectByName(self, myDict, objectTypeName, objectSchemaName):
        logging.debug("createObjectByName myDict:"+str(myDict)+", objectTypeName:"+(str(objectTypeName))+", objectSchemaName:"+(str(objectSchemaName)))
        objectSchema = self.getObjectSchemaByName(objectSchemaName)
        if not objectSchema:
            logging.warning("Unknown objectschame name: "+objectSchemaName)
            return None
        objectType = self.getObjectTypeByName(objectTypeName, objectSchema.get('id'))
        if not objectType:
            logging.warning("Unknown objecttype name: "+objectTypeName)
            return None
        
        payload = self.constructObjectPayload(myDict,objectType.get('id'))
        return self.createObject(json.loads(payload))

    def createObjectById(self, myDict, objectTypeId):
        logging.debug("createObjectById myDict:"+str(myDict)+", objectTypeId:"+(str(objectTypeId)))
        payload = self.constructObjectPayload(myDict,objectTypeId)
        return self.createObject(json.loads(payload))

    def getObjectSchemas(self, reload=False):
        logging.debug("getObjectSchemas reload:"+str(reload))
        if self.objectSchemas and not reload:
            # Return object schema's when we already got them once
            return self.objectSchemas
        
        isLast = False
        result = []
        startAt = 0
        while not isLast:
            query = self.insightUrl+'/v1/objectschema/list?maxResults=50&startAt='+str(startAt)
            subResult = self.insightGet(query)
            if subResult.get('total') > 0:
                result.extend(subResult['values'])
                startAt += subResult.get('maxResults')
                isLast = subResult.get('isLast')
            else:
                isLast = True

        self.objectSchemas = result
        return self.objectSchemas

    def getObjectSchemaByName (self, name, reload=False):
        logging.debug("getObjectSchemaByName name:"+str(name)+", reload:"+(str(reload)))
        objectSchemas = self.getObjectSchemas(reload)
        
        foundObjectSchema = None
        # Find the object schema with the correct name
        for objectSchema in objectSchemas:
            if objectSchema['name'] == name:
                foundObjectSchema = objectSchema
                continue
        return foundObjectSchema
 
    def getObjectSchemaByKey (self, key, reload=False):
        logging.debug("getObjectSchemaByKey key:"+str(key)+", reload:"+(str(reload)))
        objectSchemas = self.getObjectSchemas(reload)
        
        foundObjectSchema = None
        # Find the object schema with the correct name
        for objectSchema in objectSchemas:
            if objectSchema['objectSchemaKey'] == key:
                foundObjectSchema = objectSchema
                continue
        return foundObjectSchema

    def getObjectSchema(self,id):
        logging.debug("getObjectSchema id:"+str(id))
        query = self.insightUrl+'/v1/objectschema/'+str(id)
        result = self.insightGet(query)
        return result if result else None

    def deleteObjectSchema(self, id):
        logging.debug("deleteObjectSchema id:"+str(id))
        query = self.insightUrl+'/v1/objectschema/'+str(id)
        result = self.insightDelete(query)
        return result if result else None

    def createObjectSchema(self, name, objectSchemaKey, description=None):
        logging.debug(f'createObjectSchema name:{name}, key:{objectSchemaKey}, description:{description}')
        query = self.insightUrl+'/v1/objectschema/create'
        data = {
            'name': name,
            'objectSchemaKey': objectSchemaKey
        }
        if description:
            data['description'] = description
        result = self.insightPost(query, data)
        return result if result else None

    def updateObjectSchema(self, id, data):
        logging.debug("updateObjectschema id:"+str(id)+", data:"+(str(data)))
        query = self.insightUrl+'/v1/objectschema/'+str(id)
        result = self.insightPut(query, data)
        return result if result else None

    def getObjectSchemaAttributes(self,id,onlyValueEditable=False,extended=True,query=""):
        logging.debug("getObjectSchemaAttributes id:"+str(id)+", onlyValueEditable:"+(str(onlyValueEditable))+", extended:"+(str(extended))+", query:"+(str(query)))
        url = self.insightUrl+'/v1/objectschema/'+str(id)+'/attributes?onlyValueEditable='+str(onlyValueEditable)+'&extended='+str(extended)+'&query='+urllib.parse.quote_plus(query)
        result = self.insightGet(url)
        return result if result else None

    def getObjectSchemaProperties(self, id):
        logging.debug(f"getObjectSchemaProperties id: {id}")
        query = f"{self.insightUrl}/v1/global/config/objectschema/{id}/property"
        result = self.insightGet(query)
        return result if result else None

    def updateObjectSchemaProperties(self, id, allowOtherObjectSchema, createObjectsCustomField, quickCreateObjects, serviceDescCustomersEnabled, validateQuickCreate):
        logging.debug( f"updateObjectSchemaProperties id: {id}, allowOtherObjectSchema = {allowOtherObjectSchema}, createObjectsCustomField = {createObjectsCustomField}, quickCreateObjects = {quickCreateObjects}, serviceDescCustomersEnabled = {serviceDescCustomersEnabled}, validateQuickCreate = {validateQuickCreate}")
        data = {}
        if allowOtherObjectSchema is not None:
            data['allowOtherObjectSchema'] = allowOtherObjectSchema
        if createObjectsCustomField is not None:
            data['createObjectsCustomField'] = createObjectsCustomField
        if quickCreateObjects is not None:
            data['quickCreateObjects'] = quickCreateObjects
        if serviceDescCustomersEnabled is not None:
            data['serviceDescCustomersEnabled'] = serviceDescCustomersEnabled
        if validateQuickCreate is not None:
            data['validateQuickCreate'] = validateQuickCreate
        query = f"{self.insightUrl}/v1/global/config/objectschema/{id}/property"
        result = self.insightPost(query, data)
        return result if result else None

    def getObjectTypes (self, objectSchemaId, includeObjectCounts=False, reload=False):
        logging.debug("getObjectSchemaAttributes objectSchemaId:"+str(objectSchemaId)+", includeObjectCounts:"+(str(includeObjectCounts))+", reload:"+(str(reload)))
        if self.objectTypes.get(objectSchemaId) and not reload:
            # Return objectTypes for this object schema when we already got them once
            logging.debug("getObjectSchemaAttributes > return cached object types")
            return self.objectTypes.get(objectSchemaId)

        # Get object types for object schema
        query = self.insightUrl+'/v1/objectschema/'+str(objectSchemaId)+'/objecttypes/flat?includeObjectCounts='+str(includeObjectCounts)
        result = self.insightGet(query)
        self.objectTypes[objectSchemaId] = result
        return result if result else None

    def getObjectTypeByName (self, name, objectSchemaId, parentObjectTypeId = None, reload=False):
        logging.debug("getObjectTypeByName objectSchemaId:"+str(objectSchemaId)+", name:"+(str(name))+", reload:"+(str(reload)))
        objectTypes = self.getObjectTypes(objectSchemaId, reload)
        
        if not objectTypes:
            return None
        
        # Find the object type with the correct name
        for objectType in objectTypes:
            if objectType['name'] == name:
                ## The object names are equal
                if parentObjectTypeId:
                    if objectType['parentObjectTypeId'] == parentObjectTypeId:
                        # The parent object type ids are equal
                        # We've found the correct object type
                        return objectType
                else:
                    # This is a root object type, so name is unique
                    return objectType
        # No corresponding object type was found
        return None
 
    def getObjectType(self, id):
        logging.debug("getObjectType id:"+str(id))
        if not id:
            logging.debug("getObjectType: No argument passed")
            return None
        
        query = self.insightUrl+'/v1/objecttype/'+str(id)
        result = self.insightGet(query)
        
        if result:
            self.objectTypes[result['objectSchemaId']] = result
            return result 
        else:
            return None

    def deleteObjectType(self, id):
        logging.debug("deleteObjectType id:"+str(id))
        if not id:
            logging.debug("deleteObjectType: No argument passed")
            return None
        query = self.insightUrl+'/v1/objecttype/'+str(id)
        result = self.insightDelete(query)
        return result if result else None

    def createObjectType(self, data):
        logging.debug("createObjectType data:"+str(data))
        if not data:
            logging.debug("createObjectType: No argument passed")
            return None
        query = self.insightUrl+'/v1/objecttype/create'
        result = self.insightPost(query, data)
        return result if result else None

    def updateObjectType(self, id, data):
        logging.debug("updateObjectType id:"+str(id)+", data:"+(str(data)))
        query = self.insightUrl+'/v1/objecttype/'+str(id)
        result = self.insightPut(query, data)
        return result if result else None

    def getObjectTypeAttributes(self, id, reload=False):
        logging.debug("getObjectTypeAttributes id:"+str(id)+", reload:"+(str(reload)))
        if self.objectTypeAttributes.get(id) and not reload :
            
            # Return objectType attributes for this objecttype when we already got them
            # and the attributes should not be reloaded
            return self.objectTypeAttributes.get(id)

        query = self.insightUrl+'/v1/objecttype/'+str(id)+'/attributes'
        result = self.insightGet(query)
        self.objectTypeAttributes[id] = result
        return result if result else None

    def getAttributeByName(self, objectTypeId, name, reload=False):
        logging.debug("getAttributeByName objectTypeId:"+str(objectTypeId)+", name:"+(str(name))+", reload:"+(str(reload)))
        attributes = self.getObjectTypeAttributes(objectTypeId, reload)
        
        foundAttribute = None
        # Find the attribute  with the correct name
        for attribute in attributes:
            if attribute['name'] == name:
                foundAttribute = attribute
                break
        return foundAttribute

    def changeObjectTypePosition(self, id, parentObjectTypeId, newPosition):
        logging.debug("changeObjectTypePosition id:"+str(id)+", parentObjectTypeId:"+(str(parentObjectTypeId))+", newPosition:"+(str(newPosition)))
        query = self.insightUrl+'/v1/objecttype/'+str(id)+'/position'
        data = {
            "toObjectTypeId": parentObjectTypeId,
            "position": newPosition
        }
        result = self.insightPost(query, data)
        return result if result else None

    def createObjectTypeAttribute(self, objectTypeId, data):
        logging.debug("createObjectTypeAttribute objectTypeId:"+str(objectTypeId)+", data:"+(str(data)))
        query = self.insightUrl+'/v1/objecttypeattribute/'+str(objectTypeId)
        result = self.insightPost(query,data)
        return result if result else None
    
    def updateObjectTypeAttribute(self, objectTypeId, id, data):
        logging.debug("updateObjectTypeAttribute objectTypeId:"+str(objectTypeId)+", id:"+(str(id))+", data:"+(str(data)))
        query = self.insightUrl+'/v1/objecttypeattribute/'+str(objectTypeId)+'/'+str(id)
        result = self.insightPut(query,data)
        return result if result else None

    def moveObjectTypeAttribute(self,objectTypeId, id, position):
        logging.debug(f"moveObjectTypeAttribute objectTypeId:{objectTypeId}, id:{id}, position: {position}")
        query = f"{self.insightUrl}/v1/objecttypeattribute/{objectTypeId}/{id}/move"
        data = {
            "position": position
        }
        result = self.insightPost(query,data)
        return result if result else None
        
    def deleteObjectTypeAttribute(self, id):
        logging.debug("deleteObjectTypeAttribute id:"+str(id))
        query = self.insightUrl+'/v1/objecttypeattribute/'+str(id)
        result = self.insightDelete(query)
        return result if result else None

    def createComment(self,comment,objectId, roleId=0):
        data = {
            'role': roleId,
            'objectId': objectId,
            'comment': comment
        }
        query = self.insightUrl+'/v1/comment/create'
        result = self.insightPost(query,data)
        return result if result else None
    
    def constructObjectPayload(self, myDict, objectTypeId):
        logging.debug("constructObjectPayload myDict:"+str(myDict)+", objectTypeId:"+(str(objectTypeId)))
        # Construct attribute payload (main)
        payload = '{"objectTypeId": "'+str(objectTypeId)+'",'
        updatedAttributes = '"attributes": ['
        for key in myDict:
            value = myDict[key] if str(myDict[key]) != 'nan' else ""
                        
            attribute = self.getAttributeByName(objectTypeId, key, True)
            if not attribute:
                # Attribute could not be found, skip attribute
                logging.warning("Attribute '"+key+"' could not be found for objectTypeId: "+str(objectTypeId))
                continue
            attributeId = attribute.get('id')
    
            # Construct attribute payload (single attribute)
            updatedAttribute = '{"objectTypeAttributeId": "'+str(attributeId)+'",'
            
            # Note aggregated columns can not lead to multiple values 
            updatedAttributeValues = ''
            skipAttribute = False
            skipValue = False
            valueList = []
            if isinstance(value, list):
                valueList = value
            else:
                # When the value is split by double pipes || then multiple values should be added
                valueList = value.split("||")
            for aggVal in valueList:
                if attribute['type'] == 0:
                    # This is the default attribute
                    if attribute['defaultType']['id'] in [0,9]:
                        aggVal = aggVal.replace('\\','\\\\')
                        aggVal = aggVal.replace('\n','\\n')
                        aggVal = aggVal.replace('\t','\\t')
                        aggVal = aggVal.replace('"','\\"')
                    elif attribute['defaultType']['id'] in [4]:
                        # Date
                        if re.search("\d+[-/]\d+[-/]\d+", aggVal):
                            aggVal = parser.parse(aggVal).strftime("%Y-%m-%d")
                        aggVal = parser.parse(aggVal).strftime("%Y-%m-%d")
                    elif attribute['defaultType']['id'] in [6]:
                        # DateTime
                        aggVal = parser.parse(aggVal).replace(tzinfo=tzlocal()).isoformat(timespec='milliseconds')
                elif attribute['type'] == 1:
                    # The attribute is a reference, find the objectKey of the referenced objects
                    referencedObject = self.getObjects(f"objectId={aggVal}")
    
                    if referencedObject:
                        aggVal = referencedObject[0].get('objectKey')
                    else:
                        # We don't want this attribute in our payload, because it has no result and we don't want to change the current value
                        logging.warning("constructObjectPayload > Can't find referenced object "+attribute['name'] + " for value: "+aggVal)
                        logging.debug("constructObjectPayload > skip attribute: "+attribute['name'] + " for value: "+aggVal)
                        skipValue = True
                
                elif attribute['type'] == 2:
                    # The attribute is a user, find the account id and replace it for the display name
                    account = self.getJiraUserAccount(aggVal)
                    if account:
                        aggVal = account.get('accountId')
                    else:
                        # We don't want this attribute in our payload, because it has no result and we don't want to change the current value
                        logging.debug("constructObjectPayload > skip attribute: "+attribute['name'] + " for value: "+aggVal)
                        skipAttribute = True 
                    
                elif attribute['type'] == 4:
                    # The attribute is a group, find the group id and replace it for the name
                    group = self.getJiraGroup(aggVal)
                    if group:
                        aggVal = group.get('groupId')
                    else:
                        # We don't want this attribute in our payload, because it has no result and we don't want to change the current value
                        logging.debug("constructObjectPayload > skip attribute: "+attribute['name'] + " for value: "+aggVal)
                        skipAttribute = True 

                elif attribute['type'] == 7:
                    # The attribute is a status, find the status id and replace it for the name
                    status = self.getStatusTypeByName(aggVal)
                    if status:
                        aggVal = status.get('id')
                    else:
                        # We don't want this attribute in our payload, because it has no result and we don't want to change the current value
                        logging.debug("constructObjectPayload > skip attribute: "+attribute['name'] + " for value: "+aggVal)
                        skipAttribute = True 
                    
                if not skipValue:
                    # updatedAttributeValues = updatedAttributeValues + '{"value": "'+aggVal.replace('"', '\\"')+'"}' + ","
                    updatedAttributeValues = updatedAttributeValues + '{"value": "'+aggVal+'"}' + ","
                skipValue = False
            
            if skipAttribute:
                continue # skip this attribute and move on to the next
            
            updatedAttributeValues = updatedAttributeValues.rstrip(",") # Remove last comma
            updatedAttribute = updatedAttribute + '"objectAttributeValues": ['+updatedAttributeValues+']'+"}," # Add values 
            updatedAttributes = updatedAttributes + updatedAttribute # Add attribute to attribute list
        
        updatedAttributes = updatedAttributes.rstrip(",") + "]" # Remove last comma
        payload = payload + updatedAttributes + "}" # Add all attributes to the Json payload

        return payload
    
    def getJiraUserAccount(self, value):
        logging.debug("getJiraUserAccount name:"+str(value))
        jiraUserAccounts = self.getAllJiraUserAccounts(0,10000)
        for userAccount in jiraUserAccounts:
            # We don't know if the value is a displayName, emailAddress or accountId
            # Whatever matches first we will return
            if userAccount.get('displayName') == value:
                return userAccount # Match found for displayName
            if userAccount.get('emailAddress') == value:
                return userAccount # Match found for emailAddress
            if userAccount.get('accountId') == value:
                return userAccount # Match found for accountId
        
        return None # No match found
    
    def getJiraGroup(self, name):
        logging.debug("getJiraGroup name:"+str(name))
        jiraGroups = self.getAllJiraUserGroups(0,10000)
        for group in jiraGroups:
            if group['name'] == name:
                return group # Match found

        return None  # No match found

    def getAllJiraUserAccounts(self, startAt=0, maxResults=50, reload=False):
        logging.debug("getAllJiraUserAccounts startAt:"+str(startAt)+", maxResults:"+(str(maxResults))+", reload:"+(str(reload)))
        if self.jiraUserAccounts and not reload :
            logging.debug("getAllJiraUserAccounts > return cached user accounts")
            return self.jiraUserAccounts
        
        # Get all users from the Jira site
        query = self.jiraUrl+'/rest/api/3/users/search?startAt='+str(startAt)+'&maxResults='+str(maxResults)
        result = self.insightGet(query)
        self.jiraUserAccounts = result
        return result if result else None

    def getAllJiraUserGroups(self, startAt=0, maxResults=50, reload=False):
        logging.debug("getAllJiraUserGroups startAt:"+str(startAt)+", maxResults:"+(str(maxResults))+", reload:"+(str(reload)))
        if self.jiraGroups and not reload :
            logging.debug("getAllJiraUserGroups > return cached groups")
            return self.jiraGroups
        
        # Get all user groups from the Jira site
        query = self.jiraUrl+'/rest/api/3/bulk?startAt='+str(startAt)+'&maxResults='+str(maxResults)
        result = self.insightGet(query)
        self.jiraGroups = result
        return result['values'] if result else None
    
    def getObjectData(self, object):
        logging.debug(f"insight > getObjectData > object: {object['name']} [{object['id']}]")
        objectData = {}
        objectAttributes = self.getObjectAttributes(object['id'])
        for attribute in objectAttributes:
            attributeName = attribute['objectTypeAttribute']['name']
            attributeValue = []
            for value in attribute['objectAttributeValues']:
                logging.debug(f"insight > getObjectData > value: {value}")
                if value['referencedType']:
                    refValue = {}
                    refValue['displayValue'] = value['displayValue']
                    refValue['searchValue'] = value['searchValue']
                    attributeValue.append(refValue)
                else: 
                    attributeValue.append(value['displayValue'])
            if len(attributeValue)==1:
                # If only one value, then return value, otherwise return the list of values
                # Like: ['value1','value2']
                attributeValue = attributeValue[0]
            logging.debug(f"insight > getObjectData > attributeValue: {attributeValue}")
            objectData[attributeName]=attributeValue
        
        return objectData


############
# Below are help functions that have nothing to do with Jira Insight
# If you don't use them in your scripts, you can delete them from this module

# save Json to file
def saveDataToFile(data, fileName, path):
    logging.debug("insight > saveDataToFile > Saving: "+path+"/"+fileName+".json")
    if len(path)>0:
        # Create dir if needed
        os.makedirs(path, exist_ok = True)
    
    # Replace invalid characters in filename with underscore '_'
    invalid = '<>:"/\|?* '
    for char in invalid:
        fileName = fileName.replace(char, '_')      
    
    # write file
    f = open(abspath(f"{path}/{fileName}.json"), "w")
    f.write(data)
    f.close()
    return 

def saveAsJson(data, fileName, path, indent=2, sortKeys='id'):
    logging.debug("insight > saveAsJson > Saving JSON file: "+fileName)
    jsonData = json.dumps(data, indent=indent, sort_keys=sortKeys)
    return saveDataToFile(jsonData, fileName, path)

def loadJson(fileName):
    # Opening JSON file
    logging.debug("insight > loadJson > Loading JSON file: "+fileName)
    with io.open(abspath(fileName), 'r', encoding='UTF-8') as f:
        # returns JSON object as a dictionary
        data = json.load(f)
    
    # Closing file
    f.close()
    return data

# Zip the files from given directory
def zipDir(path, zipname):
    zipFile = zipfile.ZipFile(zipname, 'w', zipfile.ZIP_DEFLATED)
    
    for root, dirs, files in os.walk(path):
        for file in files:
            zipFile.write(os.path.join(root, file), 
                       os.path.relpath(os.path.join(root, file), 
                                       os.path.join(path, '..')))
    zipFile.close()

# Unzip a zipped file to a given directory
def unzipFile(zipname, path):
    with zipfile.ZipFile(zipname, 'r') as zip_ref:
        zip_ref.extractall(path)

def getCommandlineOptions():
    options = []

    # Parse commandline options
    parser = optparse.OptionParser()
    parser.add_option("-f",
                    dest = "configFilename",
                    help = "Location of the config file")
    options, args = parser.parse_args()

    configFile = abspath((str(options.configFilename)).lstrip())
    if configFile is None: 
        parser.print_help()
        exit(1)

    if not exists(configFile):
        logging.error(f"ERROR: the filepath '{configFile}' doesn't exist!")
        exit(1)
    if not os.path.isfile(configFile):
        logging.error(f"ERROR: '{configFile}' is not a file!")
        exit(1)
        
    options = loadJson(configFile)
    
    return options
