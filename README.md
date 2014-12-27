# GoodData Writer #

### Development installation ###

1. create workspace from repository: `git clone git@bitbucket.org:keboola/gooddata-writer.git`
2. create file `parameters.yml` in workspace root by copy from `parameters.yml.dist`
    - note section `gooddata_writer` which contains specific settings for the Writer:
```
#!yaml

gooddata_writer:
  user_agent: 'Keboola GoodData Writer' # User Agent used for Storage API requests
  ruby_path: # Optional path to ruby executable if not in standard path
  scripts_path: # Full path to folder GoodData
  gd:
    access_token: # Default access token for new GoodData projects
    domain: # GoodData domain used for newly created users
    sso_provider: keboola.com # SSO provider of newly created users
    key_passphrase: # Passphrase of private key used for SSO link generation
    user_email: '%%s-%%s@clients.keboola.com' # Template for newly created users
    project_name: 'KBC - %%s - %%s' # Template for newly created projects
    invitations_email: gooddata-robot@keboola.com # Email used for invitations to writers created from existing GD project
    invitations_password: # Password to email used for the invitations
  aws:
    access_key: # Amazon access key for accessing S3 and SQS
    secret_key: # Amazon secret key for accessing S3 and SQS
    region: us-east-1
    s3_bucket: kbc-gooddata-writer # Name of bucket in S3
    queue_url: # url to SQS queue

database_driver: pdo_mysql
database_host:
database_port: 3306
database_name:
database_user:
database_password:

encryption_key:

components:
  gooddata-writer:
    bundle: Keboola\GoodDataWriter\KeboolaGoodDataWriterBundle
```
3. create SQS queue, S3 bucket and access keys with access to both of them designated for the Writer and fill part `gooddata_writer.aws` in `parameters.yml`
4. create a database and fill it's credentials to part `database_*` in `parameters.yml`
    1. create db structure from file `db.sql`
    2. add GoodData domain and it's admin to table `domains`, password is encrypted using Syrup's Encryptor
        - you can use method `Writer\SharedStorage::saveDomain()` which will handle the encryption for you
        - note that you have to set `encryption_key` in `parameters.yml` first
5. run `composer install -n` in the workspace
6. setup Apache host as usual in Syrup