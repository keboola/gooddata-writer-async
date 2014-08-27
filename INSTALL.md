# GoodData Writer development installation #

1. create workspace from repository: `git clone git@bitbucket.org:keboola/gooddata-writer.git`
2. create file `parameters.yml` in workspace root by copy from `parameters.yml.dist`
    - note section `gooddata_writer` which contains specific settings for the Writer:
    
    ```
    #!yaml
    gooddata_writer:
      user_agent: 'Keboola GoodData Writer' # User Agent used for Storage API requests
      ruby_path: # Optional path to ruby executable if not in standard path
      scripts_path: # Full path to folderGoodData
      tmp_path: # Path to folder used for temporary files
      shared_sapi:
        url: 'https://connection.keboola.com' # URL to SAPI with Shared Config project
        token: # SAPI token of project with Shared Config
      gd:
        access_token: # Default access token for new GoodData projects
        domain: # GoodData domain used for newly created users
        sso_provider: keboola.com # SSO provider of newly created users
        key_passphrase: # Passphrase of private key used for SSO link generation
        user_email: '%%s-%%s@clients.keboola.com' # Template for newly created users
        project_name: 'KBC - %%s - %%s' # Template for newly created projects
        invitations_email: gooddata-robot@keboola.com # Email used for invitations to writers created from existing GD project
        invitations_password: # Password to email used for the invitations
      db: # Credentials to Writer's database
        host: localhost
        name: gooddata_writer
        user: gooddata_writer
        password:
      aws:
        access_key: # Amazon access key for accessing S3 and SQS
        secret_key: # Amazon secret key for accessing S3 and SQS
        region: us-east-1
        s3_bucket: kbc-gooddata-writer # Name of bucket in S3
        queue_url: # url to SQS queue
    ```
          
3. create SQS queue, S3 bucket and access keys with access to both of them designated for the Writer and fill part `gooddata_writer.aws` in `parameters.yml`
4. create a database and fill it's credentials to part `gooddata_writer.db` in `parameters.yml`
    1. create db structure from file `db.sql`
    2. add GoodData domain and it's admin to table `domains`, password is encrypted using Syrup's Encryptor
        - you can use method `Writer\SharedConfig::saveDomain() which will handle the encryption for you
        - note that you have to set `components.gooddata-writer.encryption_key` first
5. create Shared Config project in KBC (you have to create your own for each instance of Writer)
    1. fill it's SAPI token in `gooddata_writer.shared_sapi.token` section of `parameters.yml`
    2. create table `jobs` in `in.c-wr-gooddata` bucket:
        - columns: `id,runId,projectId,writerId,token,tokenId,tokenDesc,tokenOwnerName,createdTime,startTime,endTime,command,dataset,parameters,result,gdWriteStartTime,status,logs,debug,definition,projectIdWriterId,batchId,queueId`
        - primary key: `id`
        - indices: `projectIdWriterId, batchId, queueId`
6. run `composer install -n` in the workspace
7. setup Apache host as usual in Syrup