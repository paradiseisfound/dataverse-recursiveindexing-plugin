# Dataverse Plugin - Recursive Indexing

### What it is
I've generically called this plugin "recursive indexing", because this logic could be reused for any kind of indexing operation in the Dataverse that require a theoretically infinite hierarchy. Dataverse only supports recursive functions at the plugin layer - all other attempts at the same logic elsewhere are either asynchronous or require a maximum depth of hierarchy to be defined. This plugin simply indexes a set of records of the same Dataverse Entity that have a parent-child relationship to each other - sorting first alphabetically, then by hierarchy. This plugin also assigns a depth value to each record for easy UX formatting at the client layer. Calculating the index and depth of each record syncronously at the database layer takes the load off the client to figure those out - and ensures all logic is validated before any CRUD messages are committed, regardless of the client.

### How to deploy it
1. Download the [Plugin Registration Tool](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/download-tools-nuget)
2. Familiarize yourself with [plugin registration](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/register-plug-in)
3. Register this repo as the assembly
4. Register 8 steps on your target Entity in Dataverse (select the least amount filtering attributes necessary to trigger the step):
  - Delete, Pre-Validation, Synchronous
  - DeleteMultiple, Pre-Validation, Synchronous
  - Create, Pre-Operation, Synchronous
  - CreateMultiple, Pre-Operation, Synchronous
  - Update, Pre-Operation, Synchronous
  - UpdateMultiple, Pre-Operation, Synchronous
  - Delete, Pre-Operation, Synchronous
  - DeleteMultiple, Pre-Operation, Synchronous

### How to use it
Since this plugin resides at the database layer and is ran within the user's context, you can execute this logic as any authenticated user in your tenant via native platform operations (Power Apps, Power Automate, Business Process Flows, etc) or the Web API. You can send the payload as either an object or 1 record array. After a 200 response is received, follow with a GET request (or Refresh() in PowerFx) to retrieve the updated records.

### Limitations / Unsupported
- Only 1 record can be processed at a time
- Records with children cannot become children of thier own children on an Update message
