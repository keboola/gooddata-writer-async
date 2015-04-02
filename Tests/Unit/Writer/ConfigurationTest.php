<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Tests\Unit\Writer;

use Keboola\Csv\CsvFile;
use Keboola\GoodDataWriter\StorageApi\CachedClient;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Client;
use Keboola\Syrup\Encryption\Encryptor;
use Keboola\StorageApi\Table as StorageApiTable;
use Keboola\Syrup\Exception\UserException;

class ConfigurationTest extends \PHPUnit_Framework_TestCase
{

    /**
     * @var Configuration $configuration
     */
    private $configuration;
    /**
     * @var Client
     */
    private $storageApiClient;
    private $dataBucketId;

    public function setUp()
    {
        $db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => GW_DB_HOST,
            'dbname' => GW_DB_NAME,
            'user' => GW_DB_USER,
            'password' => GW_DB_PASSWORD,
        ]);
        $sharedStorage = new SharedStorage($db, new Encryptor(GW_ENCRYPTION_KEY));

        $this->storageApiClient = new Client(['token' => GW_STORAGE_API_TOKEN]);
        $this->configuration = new Configuration(new CachedClient($this->storageApiClient), $sharedStorage);

        // Cleanup
        foreach ($this->storageApiClient->listBuckets() as $bucket) {
            if ($bucket['id'] != 'sys.logs') {
                foreach ($this->storageApiClient->listTables($bucket['id']) as $table) {
                    $this->storageApiClient->dropTable($table['id']);
                }
                $this->storageApiClient->dropBucket($bucket['id']);
            }
        }

        $this->prepareData();
    }

    private function prepareData()
    {
        // Create test config
        $dataBucketName = uniqid();
        $this->dataBucketId = $this->storageApiClient->createBucket($dataBucketName, 'out', 'Writer Test');

        $table = new StorageApiTable($this->storageApiClient, $this->dataBucketId . '.categories', null, 'id');
        $table->setHeader(['id', 'name']);
        $table->setFromArray([
            ['c1', 'Category 1'],
            ['c2', 'Category 2']
        ]);
        $table->save();

        $table = new StorageApiTable($this->storageApiClient, $this->dataBucketId . '.products', null, 'id');
        $table->setHeader(['id', 'name', 'price', 'date', 'category']);
        $table->setFromArray([
            ['p1', 'Product 1', '45', '2013-01-01 00:01:59', 'c1'],
            ['p2', 'Product 2', '26', '2013-01-03 11:12:05', 'c2'],
            ['p3', 'Product 3', '112', '2012-10-28 23:07:06', 'c1']
        ]);
        $table->save();
    }

    private function prepareDataSetsConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $this->configuration->saveDateDimension('ProductDate', true);
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.categories', 'name', 'Categories');
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.categories', 'export', '1');
        $this->configuration->updateColumnsDefinition($this->dataBucketId . '.categories', [
            ['name' => 'id', 'gdName' => 'Id', 'type' => 'CONNECTION_POINT'],
            ['name' => 'name', 'gdName' => 'Name', 'type' => 'ATTRIBUTE']
        ]);
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'name', 'Products');
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'export', '1');
        $this->configuration->updateColumnsDefinition($this->dataBucketId . '.products', [
            ['name' => 'id', 'gdName' => 'Id', 'type' => 'CONNECTION_POINT'],
            ['name' => 'name', 'gdName' => 'Name', 'type' => 'ATTRIBUTE'],
            ['name' => 'price', 'gdName' => 'Price', 'type' => 'FACT'],
            [
                'name' => 'date',
                'gdName' => '',
                'type' => 'DATE',
                'format' => 'yyyy-MM-dd HH:mm:ss',
                'dateDimension' => 'ProductDate'
            ],
            [
                'name' => 'category',
                'gdName' => '',
                'type' => 'REFERENCE',
                'schemaReference' => $this->dataBucketId . '.categories'
            ]
        ]);
        $this->configuration->clearCache();
    }

    public function testWriterConfiguration()
    {
        $writerId = '' . uniqid();

        // setWriterId()
        $this->configuration->setWriterId($writerId);
        $this->assertEquals($writerId, $this->configuration->writerId);

        // createBucket(), bucketId
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;
        $this->configuration->createBucket($writerId);
        $this->assertEquals($configBucketId, $this->configuration->bucketId);
        $this->assertTrue($this->storageApiClient->bucketExists($configBucketId));
        $bucket = $this->storageApiClient->getBucket('sys.c-wr-gooddata-' . $writerId);
        $writerAttr = false;
        $writerIdAttr = false;
        foreach ($bucket['attributes'] as $attr) {
            if ($attr['name'] == 'writer') {
                if ($attr['value'] == 'gooddata') {
                    $writerAttr = true;
                }
            }
            if ($attr['name'] == 'writerId') {
                if ($attr['value'] == $writerId) {
                    $writerIdAttr = true;
                }
            }
        }
        $this->assertTrue($writerAttr);
        $this->assertTrue($writerIdAttr);

        // updateBucketAttribute()
        $testAttrName = uniqid();
        $this->configuration->updateBucketAttribute($testAttrName, $testAttrName);

        // getWriterToApi()
        $writer = $this->configuration->getWriterToApi();
        $this->assertEquals($writerId, $writer['writerId']);
        $this->assertEquals('error', $writer['status']);
        $this->assertArrayHasKey($testAttrName, $writer);
        $this->assertEquals($testAttrName, $writer[$testAttrName]);

        // getWritersToApi()
        $writers = $this->configuration->getWritersToApi();
        $this->assertCount(1, $writers);
        $writer = current($writers);
        $this->assertEquals($writerId, $writer['writerId']);
        $this->assertEquals('error', $writer['status']);
        $this->assertArrayHasKey($testAttrName, $writer);
        $this->assertEquals($testAttrName, $writer[$testAttrName]);

        // deleteBucket()
        $this->assertTrue($this->storageApiClient->bucketExists($configBucketId));
        $this->configuration->deleteBucket();
        $this->assertFalse($this->storageApiClient->bucketExists($configBucketId));
    }

    public function testSapiTablesConfiguration()
    {
        // getOutputSapiTables()
        $outputTables = $this->configuration->getOutputSapiTables();
        $this->assertTrue(in_array($this->dataBucketId . '.categories', $outputTables));
        $this->assertTrue(in_array($this->dataBucketId . '.products', $outputTables));

        // getSapiTable()
        $table = $this->configuration->getSapiTable($this->dataBucketId . '.categories');
        $this->assertArrayHasKey('id', $table);
        $this->assertArrayHasKey('uri', $table);

        // getTableIdFromAttribute()
        $this->assertEquals(
            $this->dataBucketId . '.categories',
            $this->configuration->getTableIdFromAttribute($this->dataBucketId . '.categories.id')
        );
    }

    public function testUsersConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;

        // saveUser()
        $id = uniqid();
        $this->configuration->saveUser($id . '@keboola.com', $id);
        $this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.users'));
        $rawData = $this->storageApiClient->exportTable($configBucketId . '.users');
        $data = Client::parseCsv($rawData);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('email', $data[0]);
        $this->assertArrayHasKey('uid', $data[0]);
        $this->assertEquals($id . '@keboola.com', $data[0]['email']);
        $this->assertEquals($id, $data[0]['uid']);

        // getUser()
        $user = $this->configuration->getUser($id . '@keboola.com');
        $this->assertArrayHasKey('email', $user);
        $this->assertArrayHasKey('uid', $user);
        $this->assertEquals($id . '@keboola.com', $user['email']);
        $this->assertEquals($id, $user['uid']);

        // getUsers()
        $users = $this->configuration->getUsers();
        $this->assertCount(1, $users);
        $this->assertArrayHasKey('email', $users[0]);
        $this->assertArrayHasKey('uid', $users[0]);
        $this->assertEquals($id . '@keboola.com', $users[0]['email']);
        $this->assertEquals($id, $users[0]['uid']);
        $this->configuration->saveUser(uniqid() . '@keboola.com', uniqid());
        $users = $this->configuration->getUsers();
        $this->assertCount(2, $users);

        // checkUsersTable()
        try {
            $this->configuration->checkTable(Configuration::USERS_TABLE_NAME);
        } catch (UserException $e) {
            $this->fail();
        }
        $this->storageApiClient->deleteTableColumn($configBucketId . '.users', 'email');
        $this->configuration->clearCache();
        try {
            $this->configuration->checkTable(Configuration::USERS_TABLE_NAME);
            $this->fail();
        } catch (UserException $e) {
        }
    }

    public function testProjectsConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;

        $mainPid = uniqid();
        $this->configuration->updateBucketAttribute('gd.pid', $mainPid);

        // saveProject()
        $pid = uniqid();
        $this->configuration->saveProject($pid);
        $this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.projects'));
        $rawData = $this->storageApiClient->exportTable($configBucketId . '.projects');
        $data = Client::parseCsv($rawData);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('pid', $data[0]);
        $this->assertArrayHasKey('active', $data[0]);
        $this->assertEquals($pid, $data[0]['pid']);
        $this->assertEquals(1, $data[0]['active']);

        // getProject()
        $project = $this->configuration->getProject($pid);
        $this->assertArrayHasKey('pid', $project);
        $this->assertArrayHasKey('active', $project);
        $this->assertEquals($pid, $project['pid']);
        $this->assertEquals(1, $project['active']);

        // getProjects()
        $projects = $this->configuration->getProjects();
        $this->assertCount(2, $projects);
        $projectFound = false;
        $mainProjectFound = false;
        foreach ($projects as $project) {
            $this->assertArrayHasKey('pid', $project);
            $this->assertArrayHasKey('active', $project);
            if ($project['pid'] == $pid) {
                $projectFound = true;
            }
            if ($project['pid'] == $mainPid) {
                $mainProjectFound = true;
                $this->assertArrayHasKey('main', $project);
                $this->assertTrue($project['main']);
            }
        }
        $this->assertTrue($mainProjectFound);
        $this->assertTrue($projectFound);
        $this->configuration->saveProject(uniqid());
        $projects = $this->configuration->getProjects();
        $this->assertCount(3, $projects);

        // resetProjectsTable()
        $this->configuration->resetProjectsTable();
        $projects = $this->configuration->getProjects();
        $this->assertCount(1, $projects);

        // checkProjectsTable()
        try {
            $this->configuration->checkTable(Configuration::PROJECTS_TABLE_NAME);
        } catch (UserException $e) {
            $this->fail();
        }
        $this->storageApiClient->deleteTableColumn($configBucketId . '.projects', 'active');
        $this->configuration->clearCache();
        try {
            $this->configuration->checkTable(Configuration::PROJECTS_TABLE_NAME);
            $this->fail();
        } catch (UserException $e) {
        }
    }

    public function testProjectsUsersConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;

        $pid = uniqid();
        $this->configuration->saveProject($pid);
        $uid = uniqid();
        $email = $uid . '@keboola.com';
        $this->configuration->saveUser($email, $uid);

        // saveProjectUser()
        $this->configuration->saveProjectUser($pid, $email, 'admin', false);
        $this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.project_users'));
        $rawData = $this->storageApiClient->exportTable($configBucketId . '.project_users');
        $data = Client::parseCsv($rawData);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('pid', $data[0]);
        $this->assertArrayHasKey('email', $data[0]);
        $this->assertArrayHasKey('role', $data[0]);
        $this->assertArrayHasKey('action', $data[0]);
        $this->assertEquals($pid, $data[0]['pid']);
        $this->assertEquals($email, $data[0]['email']);
        $this->assertEquals('admin', $data[0]['role']);
        $this->assertEquals('add', $data[0]['action']);

        // getProjectUsers()
        $data = $this->configuration->getProjectUsers();
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('pid', $data[0]);
        $this->assertArrayHasKey('email', $data[0]);
        $this->assertArrayHasKey('role', $data[0]);
        $this->assertArrayHasKey('action', $data[0]);
        $this->assertEquals($pid, $data[0]['pid']);
        $this->assertEquals($email, $data[0]['email']);
        $this->assertEquals('admin', $data[0]['role']);
        $this->assertEquals('add', $data[0]['action']);
        $data = $this->configuration->getProjectUsers($pid);
        $this->assertCount(1, $data);
        $data = $this->configuration->getProjectUsers(uniqid());
        $this->assertCount(0, $data);

        // isProjectUser()
        $this->assertTrue($this->configuration->isProjectUser($email, $pid));
        $this->assertFalse($this->configuration->isProjectUser($email, uniqid()));

        // deleteProjectUser()
        $this->configuration->deleteProjectUser($pid, $email);
        $data = $this->configuration->getProjectUsers();
        $this->assertCount(0, $data);

        // checkProjectsUsersTable()
        try {
            $this->configuration->checkTable(Configuration::PROJECT_USERS_TABLE_NAME);
        } catch (UserException $e) {
            $this->fail();
        }
        $this->storageApiClient->deleteTableColumn($configBucketId . '.project_users', 'role');
        $this->configuration->clearCache();
        try {
            $this->configuration->checkTable(Configuration::PROJECT_USERS_TABLE_NAME);
            $this->fail();
        } catch (UserException $e) {
        }
    }

    public function testFiltersConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;
        $filterName = uniqid();

        // saveFilter()
        $this->configuration->saveFilter(
            $filterName,
            $this->dataBucketId . 'users.name',
            '=',
            'User 1',
            'over1',
            'to1'
        );
        $this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.filters'));
        $rawData = $this->storageApiClient->exportTable($configBucketId . '.filters');
        $data = Client::parseCsv($rawData);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayHasKey('attribute', $data[0]);
        $this->assertArrayHasKey('operator', $data[0]);
        $this->assertArrayHasKey('value', $data[0]);
        $this->assertArrayHasKey('over', $data[0]);
        $this->assertArrayHasKey('to', $data[0]);
        $this->assertEquals($filterName, $data[0]['name']);
        $this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
        $this->assertEquals('=', $data[0]['operator']);
        $this->assertEquals('User 1', $data[0]['value']);
        $this->assertEquals('over1', $data[0]['over']);
        $this->assertEquals('to1', $data[0]['to']);

        // getFilters()
        $data = $this->configuration->getFilters();
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayHasKey('attribute', $data[0]);
        $this->assertArrayHasKey('operator', $data[0]);
        $this->assertArrayHasKey('value', $data[0]);
        $this->assertArrayHasKey('over', $data[0]);
        $this->assertArrayHasKey('to', $data[0]);
        $this->assertEquals($filterName, $data[0]['name']);
        $this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
        $this->assertEquals('=', $data[0]['operator']);
        $this->assertEquals('User 1', $data[0]['value']);
        $this->assertEquals('over1', $data[0]['over']);
        $this->assertEquals('to1', $data[0]['to']);
        $data = $this->configuration->getFilters([$filterName]);
        $this->assertCount(1, $data);
        $data = $this->configuration->getFilters([uniqid()]);
        $this->assertCount(0, $data);

        // getFilter()
        $data = $this->configuration->getFilter($filterName);
        $this->assertArrayHasKey('name', $data);
        $this->assertArrayHasKey('attribute', $data);
        $this->assertArrayHasKey('operator', $data);
        $this->assertArrayHasKey('value', $data);
        $this->assertArrayHasKey('over', $data);
        $this->assertArrayHasKey('to', $data);
        $this->assertEquals($filterName, $data['name']);
        $this->assertEquals($this->dataBucketId . 'users.name', $data['attribute']);
        $this->assertEquals('=', $data['operator']);
        $this->assertEquals('User 1', $data['value']);
        $this->assertEquals('over1', $data['over']);
        $this->assertEquals('to1', $data['to']);

        // deleteFilter()
        $this->configuration->deleteFilter($filterName);
        $data = $this->configuration->getFilters();
        $this->assertCount(0, $data);

        // checkFiltersTable()
        try {
            $this->configuration->checkTable(Configuration::FILTERS_TABLE_NAME);
        } catch (UserException $e) {
            $this->fail();
        }
        $this->storageApiClient->deleteTableColumn($configBucketId . '.filters', 'operator');
        $this->configuration->clearCache();
        try {
            $this->configuration->checkTable(Configuration::FILTERS_TABLE_NAME);
            $this->fail();
        } catch (UserException $e) {
        }
    }

    public function testFiltersProjectsConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;

        $pid = uniqid();
        $this->configuration->saveProject($pid);
        $filterName = uniqid();
        $this->configuration->saveFilter(
            $filterName,
            $this->dataBucketId . 'users.name',
            '=',
            'User 1',
            'over1',
            'to1'
        );

        // saveFiltersProjects()
        $this->configuration->saveFiltersProjects('uri/' . $filterName, $filterName, $pid);
        $this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.filters_projects'));
        $rawData = $this->storageApiClient->exportTable($configBucketId . '.filters_projects');
        $data = Client::parseCsv($rawData);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('uri', $data[0]);
        $this->assertArrayHasKey('filter', $data[0]);
        $this->assertArrayHasKey('pid', $data[0]);
        $this->assertEquals('uri/' . $filterName, $data[0]['uri']);
        $this->assertEquals($filterName, $data[0]['filter']);
        $this->assertEquals($pid, $data[0]['pid']);

        // getFiltersProjects()
        $data = $this->configuration->getFiltersProjects();
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('uri', $data[0]);
        $this->assertArrayHasKey('filter', $data[0]);
        $this->assertArrayHasKey('pid', $data[0]);
        $this->assertEquals('uri/' . $filterName, $data[0]['uri']);
        $this->assertEquals($filterName, $data[0]['filter']);
        $this->assertEquals($pid, $data[0]['pid']);

        // getFiltersProjectsByFilter()
        $data = $this->configuration->getFiltersProjectsByFilter($filterName);
        $this->assertCount(1, $data);
        $data = $this->configuration->getFiltersProjectsByFilter(uniqid());
        $this->assertCount(0, $data);

        // getFiltersProjectsByPid()
        $data = $this->configuration->getFiltersProjectsByPid($pid);
        $this->assertCount(1, $data);
        $data = $this->configuration->getFiltersProjectsByPid(uniqid());
        $this->assertCount(0, $data);

        // getFiltersForProject()
        $data = $this->configuration->getFiltersForProject($pid);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayHasKey('attribute', $data[0]);
        $this->assertArrayHasKey('operator', $data[0]);
        $this->assertArrayHasKey('value', $data[0]);
        $this->assertArrayHasKey('over', $data[0]);
        $this->assertArrayHasKey('to', $data[0]);
        $this->assertEquals($filterName, $data[0]['name']);
        $this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
        $this->assertEquals('=', $data[0]['operator']);
        $data = $this->configuration->getFiltersForProject(uniqid());
        $this->assertCount(0, $data);

        // deleteFilterFromProject()
        $this->configuration->deleteFilterFromProject('uri/' . $filterName);
        $data = $this->configuration->getFiltersProjects();
        $this->assertCount(0, $data);
    }

    public function testFiltersUsersConfiguration()
    {
        $writerId = uniqid();
        $this->configuration->setWriterId($writerId);
        $this->configuration->createBucket($writerId);
        $configBucketId = 'sys.c-wr-gooddata-' . $writerId;

        $uid = uniqid();
        $email = $uid . '@keboola.com';
        $this->configuration->saveUser($email, $uid);
        $filterName = uniqid();
        $this->configuration->saveFilter(
            $filterName,
            $this->dataBucketId . 'users.name',
            '=',
            'User 1',
            'over1',
            'to1'
        );

        // saveFiltersUsers()
        $this->configuration->saveFiltersUsers([$filterName], $email);
        $this->assertTrue($this->storageApiClient->tableExists($configBucketId . '.filters_users'));
        $rawData = $this->storageApiClient->exportTable($configBucketId . '.filters_users');
        $data = Client::parseCsv($rawData);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('filter', $data[0]);
        $this->assertArrayHasKey('email', $data[0]);
        $this->assertEquals($filterName, $data[0]['filter']);
        $this->assertEquals($email, $data[0]['email']);

        // getFiltersUsers()
        $data = $this->configuration->getFiltersUsers();
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('filter', $data[0]);
        $this->assertArrayHasKey('email', $data[0]);
        $this->assertEquals($filterName, $data[0]['filter']);
        $this->assertEquals($email, $data[0]['email']);

        // getFiltersUsersByEmail()
        $data = $this->configuration->getFiltersUsersByEmail($email);
        $this->assertCount(1, $data);
        $data = $this->configuration->getFiltersUsersByEmail(uniqid());
        $this->assertCount(0, $data);

        // getFiltersUsersByFilter()
        $data = $this->configuration->getFiltersUsersByFilter($filterName);
        $this->assertCount(1, $data);
        $data = $this->configuration->getFiltersUsersByFilter(uniqid());
        $this->assertCount(0, $data);

        // getFiltersForUser()
        $data = $this->configuration->getFiltersForUser($email);
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayHasKey('attribute', $data[0]);
        $this->assertArrayHasKey('operator', $data[0]);
        $this->assertArrayHasKey('value', $data[0]);
        $this->assertArrayHasKey('over', $data[0]);
        $this->assertArrayHasKey('to', $data[0]);
        $this->assertEquals($filterName, $data[0]['name']);
        $this->assertEquals($this->dataBucketId . 'users.name', $data[0]['attribute']);
        $this->assertEquals('=', $data[0]['operator']);
        $data = $this->configuration->getFiltersForProject(uniqid());
        $this->assertCount(0, $data);
    }

    public function testDataSetsConfiguration()
    {
        $this->prepareDataSetsConfiguration();

        // getDataSet()
        $tableId = $this->dataBucketId . '.products';
        $data = $this->configuration->getDataSet($tableId);
        $this->assertArrayHasKey('id', $data);
        $this->assertArrayHasKey('lastChangeDate', $data);
        $this->assertArrayHasKey('columns', $data);
        $this->assertEquals($tableId, $data['id']);
        $this->assertTrue(is_array($data['columns']));
        $this->assertCount(5, $data['columns']);
        $this->assertArrayHasKey('id', $data['columns']);
        $this->assertArrayHasKey('type', $data['columns']['id']);
        $this->assertArrayHasKey('gdName', $data['columns']['id']);
        $this->assertEquals('CONNECTION_POINT', $data['columns']['id']['type']);
        $this->assertArrayHasKey('name', $data['columns']);
        $this->assertArrayHasKey('type', $data['columns']['name']);
        $this->assertArrayHasKey('gdName', $data['columns']['name']);
        $this->assertEquals('Name', $data['columns']['name']['gdName']);
        $this->assertEquals('ATTRIBUTE', $data['columns']['name']['type']);
        $this->assertArrayHasKey('price', $data['columns']);
        $this->assertArrayHasKey('type', $data['columns']['price']);
        $this->assertArrayHasKey('gdName', $data['columns']['price']);
        $this->assertEquals('FACT', $data['columns']['price']['type']);
        $this->assertArrayHasKey('date', $data['columns']);
        $this->assertArrayHasKey('type', $data['columns']['date']);
        $this->assertArrayHasKey('gdName', $data['columns']['date']);
        $this->assertArrayHasKey('format', $data['columns']['date']);
        $this->assertArrayHasKey('dateDimension', $data['columns']['date']);
        $this->assertEquals('DATE', $data['columns']['date']['type']);
        $this->assertEquals('yyyy-MM-dd HH:mm:ss', $data['columns']['date']['format']);
        $this->assertEquals('ProductDate', $data['columns']['date']['dateDimension']);
        $this->assertArrayHasKey('category', $data['columns']);
        $this->assertArrayHasKey('type', $data['columns']['category']);
        $this->assertArrayHasKey('gdName', $data['columns']['category']);
        $this->assertEquals('REFERENCE', $data['columns']['category']['type']);
        $this->assertEquals($this->dataBucketId . '.categories', $data['columns']['category']['schemaReference']);

        // updateDataSetDefinition()
        $this->assertArrayHasKey('export', $data);
        $this->assertEquals(1, $data['export']);
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'export', 0);
        $data = $this->configuration->getDataSet($tableId);
        $this->assertEquals(0, $data['export']);

        // getDataSetDefinition()
        $data = $this->configuration->getDataSetDefinition($tableId);
        $this->assertArrayHasKey('title', $data);
        $this->assertEquals('Products', $data['title']);
        $this->assertArrayHasKey('columns', $data);
        $this->assertTrue(is_array($data['columns']));
        $this->assertCount(5, $data['columns']);
        foreach ($data['columns'] as $column) {
            $this->assertArrayHasKey('name', $column);
            $this->assertArrayHasKey('title', $column);
            $this->assertArrayHasKey('type', $column);
            if ($column['type'] == 'DATE') {
                $this->assertArrayHasKey('format', $column);
                $this->assertArrayHasKey('includeTime', $column);
                $this->assertArrayHasKey('schemaReference', $column);
            }
            if ($column['type'] == 'REFERENCE') {
                $this->assertArrayHasKey('reference', $column);
                $this->assertArrayHasKey('schemaReference', $column);
                $this->assertArrayHasKey('schemaReferenceId', $column);
            }
        }

        // getDataSetForApi()
        $data = $this->configuration->getDataSetForApi($tableId);
        $this->assertArrayHasKey('id', $data);
        $this->assertArrayHasKey('name', $data);
        $this->assertArrayHasKey('export', $data);
        $this->assertArrayHasKey('isExported', $data);
        $this->assertArrayHasKey('lastChangeDate', $data);
        $this->assertArrayHasKey('incrementalLoad', $data);
        $this->assertArrayHasKey('ignoreFilter', $data);
        $this->assertArrayHasKey('columns', $data);
        $this->assertEquals('Products', $data['name']);
        $this->assertEquals(false, $data['export']);
        $this->assertEquals(false, $data['isExported']);
        $this->assertEquals(false, $data['incrementalLoad']);
        $this->assertEquals(false, $data['ignoreFilter']);
        $this->assertTrue(is_array($data['columns']));
        $this->assertCount(5, $data['columns']);

        // getDataSets()
        $data = $this->configuration->getDataSets();
        $this->assertCount(2, $data);
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('bucket', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayHasKey('export', $data[0]);
        $this->assertArrayHasKey('isExported', $data[0]);
        $this->assertArrayHasKey('lastChangeDate', $data[0]);
        $this->assertArrayHasKey('incrementalLoad', $data[0]);
        $this->assertArrayHasKey('ignoreFilter', $data[0]);
        $this->assertEquals($this->dataBucketId, $data[0]['bucket']);

        // getDataSetsWithConnectionPoint()
        $data = $this->configuration->getDataSetsWithConnectionPoint();
        $this->assertCount(2, $data);
        $this->assertArrayHasKey($this->dataBucketId . '.categories', $data);
        $this->assertArrayHasKey($this->dataBucketId . '.products', $data);

        // getSortedDataSets()
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'export', 1);
        $dataSets = array_keys($this->configuration->getSortedDataSets());
        $this->assertCount(2, $dataSets, "Configuration should return all two configured datasets");
        $this->assertEquals($this->dataBucketId . '.categories', $dataSets[0], 'Categories should be first');
        $this->assertEquals($this->dataBucketId . '.products', $dataSets[1], 'Categories should be second');
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.products', 'export', 0);
        $dataSetsToExport = array_keys($this->configuration->getSortedDataSets());
        $this->assertCount(1, $dataSetsToExport, "Configuration should return one configured dataset");

        // updateDataSetsFromSapi()
        $tableId = $this->dataBucketId . '.' . uniqid();
        $table = new StorageApiTable($this->storageApiClient, $tableId, null, 'id');
        $table->setHeader(['id', 'name']);
        $table->save();
        $this->configuration->updateDataSetFromSapi($tableId);
        $data = $this->configuration->getDataSets();
        $this->assertCount(3, $data);

        // updateDataSetFromSapi()
        $tableId = $this->dataBucketId . '.' . uniqid();
        $table = new StorageApiTable($this->storageApiClient, $tableId, null, 'id');
        $table->setHeader(['id', 'name']);
        $table->save();
        $data = $this->configuration->getDataSets();
        $this->assertCount(3, $data);
        $this->configuration->updateDataSetFromSapi($tableId);
        $data = $this->configuration->getDataSets();
        $this->assertCount(4, $data);

        // updateColumnsDefinition()
        $tableId = $this->dataBucketId . '.products';
        $data = $this->configuration->getDataSet($tableId);
        $this->assertArrayHasKey('price', $data['columns']);
        $this->assertEquals('FACT', $data['columns']['price']['type']);
        $this->configuration->updateColumnsDefinition($tableId, 'price', ['type' => 'ATTRIBUTE']);
        $data = $this->configuration->getDataSet($tableId);
        $this->assertArrayHasKey('price', $data['columns']);
        $this->assertEquals('ATTRIBUTE', $data['columns']['price']['type']);
    }

    public function testDateDimensionsConfiguration()
    {
        $this->prepareDataSetsConfiguration();

        // getDateDimensions()
        $data = $this->configuration->getDateDimensions();
        $this->assertCount(1, $data);
        $this->assertArrayHasKey('ProductDate', $data);
        $this->assertArrayHasKey('name', $data['ProductDate']);
        $this->assertArrayHasKey('includeTime', $data['ProductDate']);
        $this->assertArrayHasKey('template', $data['ProductDate']);
        $this->assertArrayHasKey('isExported', $data['ProductDate']);
        $this->assertTrue($data['ProductDate']['includeTime']);

        // saveDateDimension()
        $dimension = uniqid();
        $this->configuration->saveDateDimension($dimension, false);
        $data = $this->configuration->getDateDimensions();
        $this->assertCount(2, $data);
        $this->assertArrayHasKey($dimension, $data);

        // getDateDimensionsWithUsage()
        $data = $this->configuration->getDateDimensionsWithUsage();
        $this->assertArrayHasKey($dimension, $data);
        $this->assertArrayNotHasKey('usedIn', $data[$dimension]);
        $this->assertArrayHasKey('ProductDate', $data);
        $this->assertArrayHasKey('usedIn', $data['ProductDate']);
        $this->assertEquals([$this->dataBucketId . '.products'], $data['ProductDate']['usedIn']);

        // getDimensionsOfDataSet()
        $data = $this->configuration->getDimensionsOfDataSet($this->dataBucketId . '.categories');
        $this->assertCount(0, $data);
        $data = $this->configuration->getDimensionsOfDataSet($this->dataBucketId . '.products');
        $this->assertCount(1, $data);

        // deleteDateDimension()
        $data = $this->configuration->getDateDimensions();
        $this->assertCount(2, $data);
        $this->configuration->deleteDateDimension($dimension);
        $data = $this->configuration->getDateDimensions();
        $this->assertCount(1, $data);

        // setDateDimensionIsExported()
        $data = $this->configuration->getDateDimensions();
        $this->assertFalse($data['ProductDate']['isExported']);
        $this->configuration->setDateDimensionIsExported('ProductDate', true);
        $data = $this->configuration->getDateDimensions();
        $this->assertTrue($data['ProductDate']['isExported']);
        $this->configuration->setDateDimensionIsExported('ProductDate', false);
        $data = $this->configuration->getDateDimensions();
        $this->assertFalse($data['ProductDate']['isExported']);
    }

    /*public function testConfigurationMigration()
    {
        $writerId = uniqid();
        $this->storageApiClient->createBucket('wr-gooddata-'.$writerId, 'sys', 'test');
        $this->storageApiClient->createTable('sys.c-wr-gooddata-'.$writerId, 'data_sets', new CsvFile(__DIR__.'/data_sets_migration.csv'));
        $this->configuration->setWriterId($writerId);
        $this->configuration->migrateDatasets();
        $data = Client::parseCsv($this->storageApiClient->exportTable('sys.c-wr-gooddata-'.$writerId.'.data_sets'));
        foreach ($data as $i => $table) {
            if ($i == 0) {
                $this->assertArrayHasKey('tableId', $table);
                $this->assertArrayHasKey('identifier', $table);
                $this->assertArrayHasKey('title', $table);
                $this->assertArrayHasKey('export', $table);
                $this->assertArrayHasKey('isExported', $table);
                $this->assertArrayHasKey('lastChangeDate', $table);
                $this->assertArrayHasKey('incrementalLoad', $table);
                $this->assertArrayHasKey('ignoreFilter', $table);
                $this->assertArrayHasKey('definition', $table);
            }
            $this->assertTrue(in_array($table['tableId'], ['out.c-main.categories', 'out.c-main.products', 'out.c-main.users']));
            $columns = json_decode($table['definition'], true);
            switch ($table['tableId']) {
                case 'out.c-main.categories':
                    $this->assertEquals('dataset.categories', $table['identifier']);
                    $this->assertEquals('Categories', $table['title']);
                    $this->assertNotEmpty($table['export']);
                    $this->assertNotEmpty($table['isExported']);
                    $this->assertEquals('2014-08-25T13:15:36+02:00', $table['lastChangeDate']);
                    $this->assertEmpty($table['incrementalLoad']);
                    $this->assertEmpty($table['ignoreFilter']);
                    foreach ($columns as $name => $def) {
                        switch($name) {
                            case 'id':
                                $this->assertArrayHasKey('type', $def);
                                $this->assertArrayHasKey('identifier', $def);
                                $this->assertArrayHasKey('title', $def);
                                $this->assertCount(3, $def);
                                $this->assertEquals('CONNECTION_POINT', $def['type']);
                                $this->assertEquals('attr.categories.id', $def['identifier']);
                                $this->assertEquals('id', $def['title']);
                                break;
                            case 'name':
                                $this->assertEquals('ATTRIBUTE', $def['type']);
                                $this->assertEquals('attr.categories.name', $def['identifier']);
                                $this->assertEquals('name', $def['title']);
                                break;
                            default:
                                $this->fail();
                        }
                    }
                    break;
                case 'out.c-main.products':
                    $this->assertEquals('dataset.products', $table['identifier']);
                    $this->assertEquals('Products', $table['title']);
                    $this->assertNotEmpty($table['export']);
                    $this->assertNotEmpty($table['isExported']);
                    $this->assertEquals('2014-08-25T13:15:34+02:00', $table['lastChangeDate']);
                    $this->assertNotEmpty($table['incrementalLoad']);
                    $this->assertEmpty($table['ignoreFilter']);
                    foreach ($columns as $name => $def) {
                        switch($name) {
                            case 'id':
                                $this->assertEquals('attr.products.id', $def['identifier']);
                                break;
                            case 'name':
                                $this->assertEquals('attr.products.name', $def['identifier']);
                                $this->assertArrayHasKey('dataType', $def);
                                $this->assertEquals('VARCHAR', $def['dataType']);
                                $this->assertArrayHasKey('dataTypeSize', $def);
                                $this->assertEquals(64, $def['dataTypeSize']);
                                break;
                            case 'category':
                                $this->assertArrayHasKey('schemaReference', $def);
                                break;
                            case 'date':
                                $this->assertArrayHasKey('format', $def);
                                $this->assertArrayHasKey('dateDimension', $def);
                                break;
                            case 'price':
                                $this->assertEquals('fact.products.price', $def['identifier']);
                                break;
                            case 'info':
                                $this->assertEquals('label.products.info', $def['identifier']);
                                $this->assertArrayHasKey('reference', $def);
                                $this->assertEquals('url', $def['reference']);
                                break;
                            case 'url':
                                $this->assertEquals('attr.products.url', $def['identifier']);
                                break;
                            default:
                                $this->fail();
                        }
                    }
                    break;
                case 'out.c-main.users':
                    $this->assertEquals('dataset.eshopusers', $table['identifier']);
                    $this->assertEquals('E-shop Users', $table['title']);
                    $this->assertNotEmpty($table['export']);
                    $this->assertNotEmpty($table['isExported']);
                    $this->assertEquals('2014-12-30T19:35:57+01:00', $table['lastChangeDate']);
                    $this->assertEmpty($table['incrementalLoad']);
                    $this->assertNotEmpty($table['ignoreFilter']);
                    foreach ($columns as $name => $def) {
                        switch($name) {
                            case 'id':
                                $this->assertEquals('attr.eshopusers.id', $def['identifier']);
                                break;
                            case 'name':
                                $this->assertEquals('attr.eshopusers.name', $def['identifier']);
                                break;
                            case 'date':
                                $this->assertEmpty($def);
                                break;
                            case 'address':
                                $this->assertEmpty($def);
                                break;
                            case 'latitude':
                                $this->assertEmpty($def);
                                break;
                            case 'longitude':
                                $this->assertEmpty($def);
                                break;
                            default:
                                $this->fail();
                        }
                    }
                    break;
                default:
                    $this->fail();
            }
        }
    }*/
}
