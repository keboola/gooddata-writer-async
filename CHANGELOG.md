## 2014-01-13 (v 5.0)
- changed configuration of datasets

## 2013-11-29 (v 4.13)
- added POST API call `proxy` which can be used to create/update GoodData objects directly under writer's credentials (see http://docs.keboolagooddatawriter.apiary.io/#proxy)

## 2013-11-26 (v 4.12)
- POST API call `project-invitations` merged into POST `project-users`
- POST API call `project-users` with `createUser` param will create user if missing in GD
- removed POST API call `project-invitations`
- new DELETE API call `project-users` for removing user from project and cancel his invitations

## 2013-22-20 (v 4.11)
- improved logging of GoodData tasks (accessible in `log` field of job status API call)

## 2013-11-19 (v 4.10)
- added API call `execute-reports`
- removed sanitization from upload jobs

## 2013-11-05 (v 4.9)
- decision if create/update dataset/dimension made on real situation in each GD project (lastExportDate is ignored but so far stays because of backwards compatibility with KBC UI)

## 2013-10-29 (v 4.8)
- added GET API call `proxy` with parameter `query` which can be used to query GoodData directly under writer's credentials

## 2013-10-29 (v 4.7)
- queue backend migrated to Amazon SQS
- possibility to use parameter `queue=secondary` to create a job in second queue which is served separately from primary queue used by default

## 2013-10-14 (v 4.6)
- filtered data load to cloned projects (see http://documentation.keboola.com/writers/gooddata#Project_cloning)

## 2013-09-23 (v 4.5)
- data load and date dimensions migrated from CL tool to Rest API

## 2013-07-29 (v 4.4)
- ExecuteReports job migrated from CL tool to Rest API

## 2013-07-25 (v 4.3)
- private links to S3

## 2013-07-04 (v 4.2)
- [UI] bugifxes
- [UI] rewritten to Angular
- tables export from SAPI using SAPI v2 with gzip
- addition of admin to project will send GD invitations to all its writers
- handling of GD bug with 401 error by exponential backoff

## 2013-05-28 (v 4.1)
- [API] added support for mandatory user filters, see <http://documentation.keboola.com/writers/gooddata/user-filters>

## 2013-05-20 (v 4.0)
- [API] added support for cloning GD projects and creating users, see <http://docs.keboolagooddatawriter4.apiary.io>
- [API] parameter `table` for `upload-table` changed to `tableId`
- [UI] creating and updating of dataset and loading data is hidden under summary `update-table` job
- [UI] `execute-reports` job is hidden from jobs queue
- [UI] added beta version of data model with visualization of references and date dimensions
- [UI] improvements and bug fixes in tables list and jobs queue

## 2013-03-13 (v 3.5)
- [UI] jobs queue speed improvements
- [UI] jobs queue rebuilt using Javascript
- [UI] dates and times are converted to user time zone
- upload of single table does not execute reports
- incremental load from SAPI using number of days in parameter `incrementalLoad`


## 2013-01-09 (v 3.4)
- creation of GoodData projects moved from Connection to writer - new api calls `/create-project` and `/invite-user`, see <http://docs.keboolagooddatawriter.apiary.io/#post-%2Fcreate-project>
- creation of each project creates new dedicated GoodData user whose credentials will be used for performing jobs (are stored in writer's configuration bucket)
- [UI] performance tweaks for loading configuration from SAPI
- [UI] forced uniqueness of names of datasets and date dimensions


## 2012-11-28 (v 3.3)
- [UI] added possibility to define GoodData access token in projects creation
- [UI] removal of writer (config is deleted instantly, GD project is scheduled for removal after one month)
- [UI] jobs queue UI integrated directly to Connection
- [UI] preview of uploaded csv in jobs queue (first 500 rows)
- [UI] preview of first ten values from csv in columns configuration
- updates for SAPI client v 2.5 compatibility
- added mandatory parameter `writerId` for `/job` and `/batch` observation calls
- added call `/jobs` allowing observation of jobs queue for given `writerId`
- removed deprecated API calls for direct communication with GoodData
- removed support for dedicated static auth token, every call needs Storage API token now
- added reports execution to the end of each batch load

## 2012-11-15 (v 3.2)
- added optional parameter `incrementalLoad` for `/upload-table` and `/upload-project` calls to force incremental or full load
- added optional parameter `sanitize` for `/load-data`, `/upload-table` and `/upload-project` calls to force cleaning or skipping null values
- added optional attributes `incrementalLoad` and `sanitize` for configuration tables
- solved dealing with recursion in tables references
- improved dealing with GoodData maintenance
- API calls for job or batch status (`/upload-table` and `/upload-project` calls return batchId and jobsIds, see <http://docs.keboolagooddatawriter.apiary.io/>)
- **[bug]** delete table with configuration when source table is deleted (this is solved when request for xml is called)
- **[bug]** delete configuration for columns removed from source table

## 2012-11-12 (v 3.1)
- added support for multiple writers

## 2012-10-31 (v 3.0)
- configuration in Storage API
- batch upload of whole project from Connection

## 2012-09-21 (v 2.1)
- parallelization of jobs processing

## 2012-08-01 (v 2.0)
- added UI for jobs observation
- added escaping of NULL values

## 2012-07-01 (v 1.0)
- first version