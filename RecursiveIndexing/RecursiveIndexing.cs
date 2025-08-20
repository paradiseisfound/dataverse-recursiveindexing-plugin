using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Riata
{
    public class RecursiveIndexing : IPlugin
    {
        private const string publisherPrefix = "mda"; // Adjust this based on your publisher's prefix

        private const string profileAttribute = publisherPrefix + "_profile"; // [OPTIONAL] // attribute that segregates groups of records stored in the same table. Remove reference if unused.
        private const string parentAttribute = publisherPrefix + "_parent"; // This attribute holds the parent record reference
        private const string indexAttribute = publisherPrefix + "_index"; // Aan integer that will pre-sort records based on heirarchy and spelling
        private const string depthAttribute = publisherPrefix + "_depth"; // An integer that denotes the depth of the record in the hierarchy
        private const string nameAttribute = publisherPrefix + "_name"; // The name/description of the record

        public void Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            var tracing = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.Depth > 1) // Prevents plugin recursion
            {
                return;
            }

            var serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            var service = serviceFactory.CreateOrganizationService(context.UserId);
            Entity targetRecord = null;
            EntityReference targetRef;
            EntityReference profileRef = null; // Remove if profileAttribute is unused
            EntityReference parentTargetRef = null;
            EntityReference deletedParentRef = null;
            string entityName = context.PrimaryEntityName;
            string guidAttribute = entityName + "id";
            bool updateTriggeredFromDelete = (context.ParentContext != null) &&
                (context.ParentContext.MessageName.Contains("Delete")) &&
                (context.MessageName.Contains("Update"));

            if (!context.InputParameters.Contains("Targets") &&
                !context.InputParameters.Contains("Target"))
            {
                tracing.Trace("[ERROR] Target(s) not found.");
                throw new InvalidPluginExecutionException("Target(s) not found.");
            }

            if (context.Stage == (int)SdkMessageProcessingStepStage.PreValidation)
            {
                if (context.InputParameters.Contains("Targets") &&
                    context.InputParameters["Targets"] is EntityReferenceCollection deletedRefCollection)
                {
                    var targetList = deletedRefCollection.ToList();

                    if (targetList.Count != 1)
                    {
                        tracing.Trace("[ERROR] Only 1 record is supported for Multiple messages.");
                        throw new InvalidPluginExecutionException("Only 1 record is supported for Multiple messages.");
                    }

                    targetRef = targetList[0];
                }
                else
                {
                    targetRef = context.InputParameters["Target"] as EntityReference;
                }

                context.SharedVariables["DeleteRef"] = targetRef;

                // Remove if profileAttribute is unused
                targetRecord = service.Retrieve(entityName, targetRef.Id, new ColumnSet(profileAttribute));
                context.SharedVariables["ProfileRef"] = targetRecord.GetAttributeValue<EntityReference>(profileAttribute);
                //////////////////////////////////////

                var childQuery = new QueryExpression(entityName)
                {
                    ColumnSet = new ColumnSet(guidAttribute, parentAttribute),
                    Criteria = new FilterExpression()
                    {
                        Conditions =
                        {
                            new ConditionExpression(parentAttribute, ConditionOperator.Equal, targetRef.Id)
                        }
                    }
                };

                context.SharedVariables["ChildrenOfDeletedTarget"] =
                service.RetrieveMultiple(childQuery);
                return;
            }

            if (context.InputParameters.Contains("Targets") &&
                context.InputParameters["Targets"] is EntityCollection entityCollection)
            {
                if (entityCollection.Entities.Count != 1)
                {
                    tracing.Trace("[ERROR] Only 1 record is supported for Multiple messages.");
                    throw new InvalidPluginExecutionException("Only 1 record is supported for Multiple messages.");
                }

                targetRecord = entityCollection.Entities[0];

                if (context.MessageName == "UpdateMultiple")
                {
                    if (targetRecord.TryGetAttributeValue(parentAttribute, out EntityReference parentRef) &&
                        parentRef.Id == targetRecord.Id)
                    {
                        tracing.Trace("[ERROR] Record cannot be a parent of itself.");
                        throw new InvalidPluginExecutionException("Record cannot be a parent of itself.");
                    }

                    Entity targetRecordPreUpdate = service.Retrieve(entityName, targetRecord.Id, new ColumnSet(nameAttribute, profileAttribute, parentAttribute));

                    foreach (var attr in targetRecordPreUpdate.Attributes)
                    {
                        if ((!targetRecord.Attributes.ContainsKey(attr.Key) && attr.Key == parentAttribute) ||
                            (!targetRecord.Attributes.ContainsKey(attr.Key) || targetRecord[attr.Key] == null || targetRecord[attr.Key] == "") && attr.Key != parentAttribute)  
                        {
                            targetRecord.Attributes[attr.Key] = attr.Value;
                        }
                    }
                }
                else // CreateMultiple
                {
                    if (!targetRecord.Attributes.ContainsKey(nameAttribute) ||
                        string.IsNullOrWhiteSpace(targetRecord.GetAttributeValue<string>(nameAttribute)))
                    {
                        tracing.Trace($"[ERROR] Record is missing or has an empty {nameAttribute}");
                        throw new InvalidPluginExecutionException($"Record is missing or has an empty {nameAttribute}");
                    }
                }
            }
            else if (context.MessageName == "Update")
            {
                targetRecord = context.InputParameters["Target"] as Entity;

                if (targetRecord.TryGetAttributeValue(parentAttribute, out EntityReference parentRef) &&
                        parentRef.Id == targetRecord.Id)
                {
                    tracing.Trace("[ERROR] Record cannot be a parent of itself.");
                    throw new InvalidPluginExecutionException("Record cannot be a parent of itself.");
                }

                Entity targetRecordPreUpdate = service.Retrieve(entityName, targetRecord.Id, new ColumnSet(nameAttribute, profileAttribute, parentAttribute));

                foreach (var attr in targetRecordPreUpdate.Attributes)
                {
                    if ((!targetRecord.Attributes.ContainsKey(attr.Key) && attr.Key == parentAttribute) ||
                        (!targetRecord.Attributes.ContainsKey(attr.Key) || targetRecord[attr.Key] == null || targetRecord[attr.Key] == "") && attr.Key != parentAttribute)
                    {
                        targetRecord.Attributes[attr.Key] = attr.Value;
                    }
                }
            }
            else if (context.MessageName == "Create")
            {
                targetRecord = context.InputParameters["Target"] as Entity;

                if (!targetRecord.Attributes.ContainsKey(nameAttribute) ||
                    string.IsNullOrWhiteSpace(targetRecord.GetAttributeValue<string>(nameAttribute)))
                {
                    tracing.Trace($"[ERROR] Record is missing or has an empty {nameAttribute}");
                    throw new InvalidPluginExecutionException($"Record is missing or has an empty {nameAttribute}");
                }
            }
            // Remove if profileAttribute is unused
            else // Delete and DeleteMultiple
            {
                profileRef = context.ParentContext.SharedVariables["ProfileRef"] as EntityReference;
            }

            if (!context.MessageName.Contains("Delete"))
            {
                if (!targetRecord.Attributes.TryGetValue(profileAttribute, out var profileObj) ||
                    !(profileObj is EntityReference))
                {
                    tracing.Trace("[ERROR] Profile reference is required.");
                    throw new InvalidPluginExecutionException("Profile reference is required.");
                }
                else
                {
                    profileRef = (EntityReference)profileObj;
                }
            }
            ///////////////////////////////////////////////////

            var query = new QueryExpression(entityName)
            {
                ColumnSet = new ColumnSet(parentAttribute, nameAttribute),
                Criteria = new FilterExpression(LogicalOperator.And)
                {
                    Conditions =
                    {
                    // Remove this if profileAttribute is unused
                    new ConditionExpression(profileAttribute, ConditionOperator.Equal, profileRef.Id)
                    ///////////////////////////////////////////////////
                    }
                }
            };

            if (context.MessageName.Contains("Update"))
            {
                query.Criteria.AddCondition(new ConditionExpression(guidAttribute, ConditionOperator.NotEqual, targetRecord.Id));

                if (updateTriggeredFromDelete)
                {
                    if (context.ParentContext.InputParameters.Contains("Targets") &&
                    context.ParentContext.InputParameters["Targets"] is EntityReferenceCollection parentTargetRefCollection)
                    {
                        var targetList = parentTargetRefCollection.ToList();
                        parentTargetRef = parentTargetRefCollection.FirstOrDefault();
                    }
                    else // EntityReference
                    {
                        parentTargetRef = context.ParentContext.InputParameters["Target"] as EntityReference;
                    }

                    query.Criteria.AddCondition(new ConditionExpression(guidAttribute, ConditionOperator.NotEqual, parentTargetRef.Id));
                    deletedParentRef = service
                        .Retrieve(entityName, parentTargetRef.Id, new ColumnSet(parentAttribute))
                        .GetAttributeValue<EntityReference>(parentAttribute);
                    targetRecord[parentAttribute] = deletedParentRef;
                }
            }
            else if (context.MessageName.Contains("Delete"))
            {
                query.Criteria.AddCondition(new ConditionExpression(guidAttribute, ConditionOperator.NotEqual, ((EntityReference)context.ParentContext.SharedVariables["DeleteRef"]).Id));
            }

            var listWholeRecordSet =
                service
                .RetrieveMultiple(query).Entities
                .ToList();

            if (context.MessageName.Contains("Create") ||
                context.MessageName.Contains("Update"))
            { 
                listWholeRecordSet.Add(targetRecord);
            }

            if (WouldCauseCircularReference(targetRecord, listWholeRecordSet, parentAttribute))
            {
                tracing.Trace("[ERROR] Record cannot become a child of its own child. Break relationship first.");
                throw new InvalidPluginExecutionException("Record cannot become a child of its own child. Break relationship first.");
            }

            if (updateTriggeredFromDelete)
            {
                foreach (var ThisRecord in listWholeRecordSet
                        .Where(r =>
                        r.Attributes.TryGetValue(parentAttribute, out var parentObj) &&
                        parentObj is EntityReference parentRef &&
                        parentRef.Id == parentTargetRef.Id)
                    )
                {
                    ThisRecord[parentAttribute] = deletedParentRef;
                }
            }

            var dicChildrenLookup = new Dictionary<Guid, List<Entity>>();

            foreach (var ThisRecord in listWholeRecordSet)
            {
                    if (ThisRecord.Attributes.TryGetValue(parentAttribute, out var parentObj) && parentObj is EntityReference parentRef)
                    {
                        if (!dicChildrenLookup.TryGetValue(parentRef.Id, out var children))
                        {
                            children = new List<Entity>();
                            dicChildrenLookup[parentRef.Id] = children;
                        }
                        children.Add(ThisRecord);
                }
            }

            foreach (var children in dicChildrenLookup.Values)
            {
                children.Sort((a, b) =>
                    string.Compare(
                        a.GetAttributeValue<string>(nameAttribute),
                        b.GetAttributeValue<string>(nameAttribute),
                        StringComparison.OrdinalIgnoreCase
                    )
                );
            }

            var listRoots =
                    listWholeRecordSet
                    .Where(ThisRecord =>
                        !ThisRecord.Attributes.ContainsKey(parentAttribute) ||
                        ThisRecord.GetAttributeValue<EntityReference>(parentAttribute) == null)
                    .OrderBy(ThisRecord => ThisRecord.GetAttributeValue<string>(nameAttribute))
                    .ToList();
            var listRecordsToUpdate = new List<Entity>();
            int index = 0;

            foreach (var ThisRecord in listRoots)
                index = AssignIndices(ThisRecord, 
                                      index, 
                                      0, 
                                      dicChildrenLookup, 
                                      listRecordsToUpdate, 
                                      targetRecord, 
                                      updateTriggeredFromDelete, 
                                      (context.ParentContext.SharedVariables.TryGetValue("ChildrenOfDeletedTarget", out var tmp) ? tmp as EntityCollection : null)?.Entities
                                      ).index;

            if (listRecordsToUpdate.Count == 0)
            {
                tracing.Trace("[LOG] No updates necessary.");
                return;
            }

            var updateRequest = new ExecuteMultipleRequest
            {
                Settings = new ExecuteMultipleSettings
                {
                    ContinueOnError = false,
                    ReturnResponses = true
                },
                Requests = new OrganizationRequestCollection()
            };

            foreach (var entityToUpdate in listRecordsToUpdate)
            {
                /* Removing the parentAttribute in this context avoids a SQL deadlock when the (EntityReference)parentAttribute 
                 * was already operated on in a previous, cascading Update message for referential integrity.
                */

                if (context.MessageName.Contains("Delete") &&
                    entityToUpdate.Attributes.Contains(parentAttribute))
                {
                     entityToUpdate.Attributes.Remove(parentAttribute);
                }
                
                updateRequest.Requests.Add(new UpdateRequest { Target = entityToUpdate });
            }

            try
            {
                var response = (ExecuteMultipleResponse)service.Execute(updateRequest);

                if (response.IsFaulted)
                {
                    for (int i = 0; i < response.Responses.Count; i++)
                    {
                        var item = response.Responses[i];
                        if (item.Fault != null)
                        {
                            tracing.Trace($"[ERROR] Update failed for request #{i}: {item.Fault.TraceText} | Detail: {item.Fault.InnerFault?.Message ?? "No inner fault"} | Code: {item.Fault.ErrorCode}");
                        }
                    }
                    throw new InvalidPluginExecutionException("Some updates failed. See trace for details.");
                }
            }
            catch (Exception ex)
            {
                tracing.Trace("[ERROR] Exception during update: " + ex.ToString());
                throw;
            }
        }
        

        private (int index, Entity targetRecord) AssignIndices(
            Entity ThisRecord,
            int index,
            int depth,
            Dictionary<Guid, List<Entity>> dicChildrenLookup,
            List<Entity> listRecordsToUpdate,
            Entity targetRecord,
            bool updateTriggeredFromDelete,
            IEnumerable<Entity> childrenOfDeletedTarget)
        {
            if (object.ReferenceEquals(ThisRecord, targetRecord) &&
                targetRecord != null)
            {
                targetRecord[indexAttribute] = index++;
                targetRecord[depthAttribute] = depth++;
            }
            else
            {
                ThisRecord[indexAttribute] = index++;
                ThisRecord[depthAttribute] = depth++;

                /* Need to exclude any children records that were already operated on in a previous,
                 * cascading Update message, to avoid a SQL deadlock.
                */

                if (!updateTriggeredFromDelete &&
                    !(childrenOfDeletedTarget?.Any(r => r.Id == ThisRecord.Id) ?? false))
                { 
                    listRecordsToUpdate.Add(ThisRecord);
                }
            }

            if (dicChildrenLookup.TryGetValue(ThisRecord.Id, out var children))
            {
                foreach (var child in children)
                    index = AssignIndices(child, index, depth, dicChildrenLookup, listRecordsToUpdate, targetRecord, updateTriggeredFromDelete, childrenOfDeletedTarget).index;
            }

            return (index, targetRecord);
        }

        private bool WouldCauseCircularReference(Entity targetRecord, List<Entity> listWholeRecordSet, string parentAttribute)
        {
            if (!targetRecord.Attributes.Contains(parentAttribute) || targetRecord[parentAttribute] == null)
                return false;

            if (!(targetRecord[parentAttribute] is EntityReference newParentRef))
                return false;
            var childrenLookup = listWholeRecordSet
                .Where(e => e.Contains(parentAttribute) && e[parentAttribute] is EntityReference)
                .GroupBy(e => ((EntityReference)e[parentAttribute]).Id)
                .ToDictionary(g => g.Key, g => g.ToList());

            return IsDescendantOf(newParentRef.Id, targetRecord.Id, childrenLookup);
        }

        private bool IsDescendantOf(Guid candidateId, Guid rootId, Dictionary<Guid, List<Entity>> childrenLookup)
        {
            if (!childrenLookup.TryGetValue(rootId, out var children))
                return false;

            foreach (var child in children)
            {
                if (child.Id == candidateId)
                    return true;

                if (IsDescendantOf(candidateId, child.Id, childrenLookup))
                    return true;
            }

            return false;
        }

        public enum SdkMessageProcessingStepStage
        {
            PreValidation = 10,
            Preoperation = 20,
            Postoperation = 40
        }
    }
}
